// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mvcc

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/google/btree"
	"go.uber.org/zap"
)

var (
	ErrRevisionNotFound = errors.New("mvcc: revision not found")
)

// keyIndex stores the revisions of a key in the backend.
// Each keyIndex has at least one key generation.
// Each generation might have several key versions.
// Tombstone on a key appends an tombstone version at the end
// of the current generation and creates a new empty generation.
// Each version of a key has an index pointing to the backend.
//
// For example: put(1.0);put(2.0);tombstone(3.0);put(4.0);tombstone(5.0) on key "foo"
// generate a keyIndex:
// key:     "foo"
// rev: 5
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {1.0, 2.0, 3.0(t)}
//
// Compact a keyIndex removes the versions with smaller or equal to
// rev except the largest one. If the generation becomes empty
// during compaction, it will be removed. if all the generations get
// removed, the keyIndex should be removed.
//
// For example:
// compact(2) on the previous example
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//    {2.0, 3.0(t)}
//
// compact(4)
// generations:
//    {empty}
//    {4.0, 5.0(t)}
//
// compact(5):
// generations:
//    {empty} -> key SHOULD be removed.
//
// compact(6):
// generations:
//    {empty} -> key SHOULD be removed.
type keyIndex struct {
	// 客户端提供的原始 Key 值
	key         []byte
	// 记录该 Key 值最后一次修改对应的 revision 信息.
	modified    revision // the main rev of the last modification
	// 当第一次创建客户端给定的 Key 值时, 对应的第 0 代版本信息（即 generations[0] 项）也会被创建,
	// 所以每个 Key 值至少对应一个 generation 实例（如果没有, 则表示当前 Key 值应该被删除）, 每代中
	// 包含多个 revision 信息. 当客户端后续不断修改该 Key 时, generations[0] 中会不断追加 revision 信息.
	// 当向 generation 实例追加一个 Tombstone 时, 表示删除当前 Key 值, 此时就会结束当前的 generation,
	// 后续不再向该 generation 实例中追加 revision 信息, 同时会创建新的 generation 实例. 因此可认为 generation
	// 对应了当前 Key 一次从创建到删除的生命周期.
	generations []generation
}

// put puts a revision to the keyIndex.
//
// put 向 keyIndex 中追加新的 revision 信息.
func (ki *keyIndex) put(lg *zap.Logger, main int64, sub int64) {
	// 根据传入的 main revision 和 sub revision 创建 revision 实例
	rev := revision{main: main, sub: sub}

	// 检测该 revision 实例的合法性
	if !rev.GreaterThan(ki.modified) {
		if lg != nil {
			lg.Panic(
				"'put' with an unexpected smaller revision",
				zap.Int64("given-revision-main", rev.main),
				zap.Int64("given-revision-sub", rev.sub),
				zap.Int64("modified-revision-main", ki.modified.main),
				zap.Int64("modified-revision-sub", ki.modified.sub),
			)
		} else {
			plog.Panicf("store.keyindex: put with unexpected smaller revision [%v / %v]", rev, ki.modified)
		}
	}
	// 若为首次创建客户端给定的 Key 值时, 则创建 generations[0] 实例
	if len(ki.generations) == 0 {
		ki.generations = append(ki.generations, generation{})
	}
	g := &ki.generations[len(ki.generations)-1]
	// 新建 Key, 则初始化对应 generation 实例的 created 字段
	if len(g.revs) == 0 { // create a new key
		keysGauge.Inc()
		g.created = rev
	}
	// 向 generation.revs 中追加 revision 信息
	g.revs = append(g.revs, rev)
	// 递增 generation.ver
	g.ver++
	// 更新 keyIndex.modified 字段, 记录该 Key 值最后一次修改对应的 revision 信息.
	ki.modified = rev
}

// restore 恢复当前的 keyIndex 中的信息
func (ki *keyIndex) restore(lg *zap.Logger, created, modified revision, ver int64) {
	// 当前 generations 切片必须为空
	if len(ki.generations) != 0 {
		if lg != nil {
			lg.Panic(
				"'restore' got an unexpected non-empty generations",
				zap.Int("generations-size", len(ki.generations)),
			)
		} else {
			plog.Panicf("store.keyindex: cannot restore non-empty keyIndex")
		}
	}

	ki.modified = modified
	// 创建 generation 实例, 并添加到 generations 中
	g := generation{created: created, ver: ver, revs: []revision{modified}}
	ki.generations = append(ki.generations, g)
	keysGauge.Inc()
}

// tombstone puts a revision, pointing to a tombstone, to the keyIndex.
// It also creates a new empty generation in the keyIndex.
// It returns ErrRevisionNotFound when tombstone on an empty generation.
//
// tombstone 会在当前 generation 中追加一个 revision 实例, 然后新建一个 generation 实例.
func (ki *keyIndex) tombstone(lg *zap.Logger, main int64, sub int64) error {
	// 检测当前的 keyIndex.generations 字段是否为空, 以及当前使用的 generation 实例是否为空
	if ki.isEmpty() {
		if lg != nil {
			lg.Panic(
				"'tombstone' got an unexpected empty keyIndex",
				zap.String("key", string(ki.key)),
			)
		} else {
			plog.Panicf("store.keyindex: unexpected tombstone on empty keyIndex %s", string(ki.key))
		}
	}
	if ki.generations[len(ki.generations)-1].isEmpty() {
		return ErrRevisionNotFound
	}
	// 在当前 generation 中追加一个 revision 信息
	ki.put(lg, main, sub)
	// 在 generations 中创建新 generation 实例
	ki.generations = append(ki.generations, generation{})
	keysGauge.Dec()
	return nil
}

// get gets the modified, created revision and version of the key that satisfies the given atRev.
// Rev must be higher than or equal to the given atRev.
//
// get 在当前 keyIndex 实例中查找小于指定 main revision 的最大 revision.
func (ki *keyIndex) get(lg *zap.Logger, atRev int64) (modified, created revision, ver int64, err error) {
	// 检测当前的 keyIndex 中是否记录了 generation 实例
	if ki.isEmpty() {
		if lg != nil {
			lg.Panic(
				"'get' got an unexpected empty keyIndex",
				zap.String("key", string(ki.key)),
			)
		} else {
			plog.Panicf("store.keyindex: unexpected get on empty keyIndex %s", string(ki.key))
		}
	}
	// 根据给定的 main revison（即 atRev）, 查找对应的 generation 实例, 如果没有对应的 generation, 则报错
	g := ki.findGeneration(atRev)
	if g.isEmpty() {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}

	// 在 generation 中查找对应的 revision 实例, 如果查找失败, 则报错
	n := g.walk(func(rev revision) bool { return rev.main > atRev })
	if n != -1 { // 若找到
		// 返回值除了目标 revision 实例, 还有该 Key 此次创建的 revision 实例， 以及到目标 revision 之前的修改次数.
		return g.revs[n], g.created, g.ver - int64(len(g.revs)-n-1), nil
	}

	// 没有找到, 则返回错误
	return revision{}, revision{}, 0, ErrRevisionNotFound
}

// since returns revisions since the given rev. Only the revision with the
// largest sub revision will be returned if multiple revisions have the same
// main revision.
//
// since 返回当前 keyIndex 实例中 main 部分大于指定值的 revision 实例, 如果查询结果中包含多个 main 部分
// 相同的 revision 实例, 则只返回其中 sub 部分最大的实例.
func (ki *keyIndex) since(lg *zap.Logger, rev int64) []revision {
	if ki.isEmpty() {
		if lg != nil {
			lg.Panic(
				"'since' got an unexpected empty keyIndex",
				zap.String("key", string(ki.key)),
			)
		} else {
			plog.Panicf("store.keyindex: unexpected get on empty keyIndex %s", string(ki.key))
		}
	}
	since := revision{rev, 0}
	var gi int
	// find the generations to start checking
	// 倒序遍历所有 generation 实例, 查询从哪个 generation 开始查找（即 gi 对应的 generation 实例）
	for gi = len(ki.generations) - 1; gi > 0; gi-- {
		g := ki.generations[gi]
		if g.isEmpty() {
			continue
		}
		// 比较创建当前 generation 实例的 revision 与 since
		// 在 revision.GreaterThan() 方法中会先比较 main revision 部分, 如果相同,
		// 则比较 sub revision 部分.
		if since.GreaterThan(g.created) {
			break
		}
	}

	var revs []revision // 记录返回结果
	var last int64 // 用于记录当前所遇到的最大的 main revision 值
	// 从 gi 开始遍历 generations 切片
	for ; gi < len(ki.generations); gi++ {
		// 遍历每个 generation 实例中记录的 revision 实例
		for _, r := range ki.generations[gi].revs {
			if since.GreaterThan(r) {
				continue
			}
			// 如果查找到 main revision 部分相同且 sub revision 更大的 revision 实例,
			// 则用其替换之前记录的返回结果, 从而实现 main 部分相同时只返回 sub 部分较大的 revision 实例.
			if r.main == last {
				// replace the revision with a new one that has higher sub value,
				// because the original one should not be seen by external
				revs[len(revs)-1] = r
				continue
			}
			// 将符合条件的 revison 实例记录到 revs 中
			revs = append(revs, r)
			// 更新 last
			last = r.main
		}
	}
	return revs
}

// compact compacts a keyIndex by removing the versions with smaller or equal
// revision than the given atRev except the largest one (If the largest one is
// a tombstone, it will not be kept).
// If a generation becomes empty during compaction, it will be removed.
//
// compact 随着客户端不断修改键值对, keyIndex 中记录的 revision 实例和 generation 实例会不断增加,
// 可通过 compact() 方法对 keyIndex 进行压缩. 在压缩时会将 main 部分小于指定值的全部 revision 实例
// 全部删除. 在压缩过程中, 如果出现了空的 generation 实例, 则会将其删除. 如果 keyIndex 中全部的 generation
// 实例都被清除了, 则该 keyIndex 实例也会被删除.
func (ki *keyIndex) compact(lg *zap.Logger, atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		if lg != nil {
			lg.Panic(
				"'compact' got an unexpected empty keyIndex",
				zap.String("key", string(ki.key)),
			)
		} else {
			plog.Panicf("store.keyindex: unexpected compact on empty keyIndex %s", string(ki.key))
		}
	}

	// 找到开始执行压缩的 generation 以及该 generation 中执行压缩的 revision 下标
	genIdx, revIndex := ki.doCompact(atRev, available)

	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove the previous contents.
		if revIndex != -1 {
			// 清除当前 generation 中 revs 切片中 0~revIndex 的 revison 数据
			g.revs = g.revs[revIndex:]
		}
		// remove any tombstone
		// 如果目标 generation 实例中只有 tombstone, 则将其删除
		if len(g.revs) == 1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[0])
			genIdx++ // 递增 generation 索引, 后面会清理 genIdx 之前的所有 generation 实例
		}
	}

	// remove the previous generations.
	// 清理目标 generation 之前的全部 generation 实例.
	ki.generations = ki.generations[genIdx:]
}

// keep finds the revision to be kept if compact is called at given atRev.
func (ki *keyIndex) keep(atRev int64, available map[revision]struct{}) {
	if ki.isEmpty() {
		return
	}

	genIdx, revIndex := ki.doCompact(atRev, available)
	g := &ki.generations[genIdx]
	if !g.isEmpty() {
		// remove any tombstone
		if revIndex == len(g.revs)-1 && genIdx != len(ki.generations)-1 {
			delete(available, g.revs[revIndex])
		}
	}
}

func (ki *keyIndex) doCompact(atRev int64, available map[revision]struct{}) (genIdx int, revIndex int) {
	// walk until reaching the first revision smaller or equal to "atRev",
	// and add the revision to the available map
	//
	// 后面遍历 generation 时使用的回调函数
	f := func(rev revision) bool {
		if rev.main <= atRev {
			available[rev] = struct{}{}
			return false
		}
		return true
	}

	genIdx, g := 0, &ki.generations[0]
	// find first generation includes atRev or created after atRev
	// 遍历所有的 generation 实例, 目标 generation 实例中的 tombstone 大于指定的 revision
	for genIdx < len(ki.generations)-1 {
		if tomb := g.revs[len(g.revs)-1].main; tomb > atRev {
			break
		}
		genIdx++
		g = &ki.generations[genIdx]
	}

	// 找到目标 generation 中的目标 revision 的下标值
	revIndex = g.walk(f)

	return genIdx, revIndex
}

func (ki *keyIndex) isEmpty() bool {
	return len(ki.generations) == 1 && ki.generations[0].isEmpty()
}

// findGeneration finds out the generation of the keyIndex that the
// given rev belongs to. If the given rev is at the gap of two generations,
// which means that the key does not exist at the given rev, it returns nil.
//
// findGeneration 查找指定 main revision（即 rev）所在的 generation 实例.
//
// 有时 keyIndex.findGeneration() 方法无法查询到指定的 mian revison 所在的 generation 实例,
// 假设当前的 main revision 是 5, 当前 keyIndex.generations 状态如下：
// - [0] -> | 1.0 | 2.0 | 3.0 | Tombstone |
// - [1] -> | 8.0 | 9.0 | Tombstone |
// 因为 main revision 为 4~7 这段时间内, 该 Key 被删除了, 所以无法查找到其所在的 generation 实例,
// 此时 findGeneration() 方法会返回 nil.
func (ki *keyIndex) findGeneration(rev int64) *generation {
	lastg := len(ki.generations) - 1
	cg := lastg // 指向当前 keyIndex 实例中最后一个 generation 实例, 并逐个向前查找

	for cg >= 0 {
		// 过滤调用空的 generation 实例
		if len(ki.generations[cg].revs) == 0 {
			cg--
			continue
		}
		g := ki.generations[cg]
		// 如果不是最后一个 generation 实例, 则先与 tombone revision 进行比较
		if cg != lastg {
			if tomb := g.revs[len(g.revs)-1].main; tomb <= rev {
				return nil
			}
		}
		// 与 generation 中的第一个 revision 比较
		if g.revs[0].main <= rev {
			return &ki.generations[cg]
		}
		cg--
	}
	return nil
}

func (ki *keyIndex) Less(b btree.Item) bool {
	return bytes.Compare(ki.key, b.(*keyIndex).key) == -1
}

func (ki *keyIndex) equal(b *keyIndex) bool {
	if !bytes.Equal(ki.key, b.key) {
		return false
	}
	if ki.modified != b.modified {
		return false
	}
	if len(ki.generations) != len(b.generations) {
		return false
	}
	for i := range ki.generations {
		ag, bg := ki.generations[i], b.generations[i]
		if !ag.equal(bg) {
			return false
		}
	}
	return true
}

func (ki *keyIndex) String() string {
	var s string
	for _, g := range ki.generations {
		s += g.String()
	}
	return s
}

// generation contains multiple revisions of a key.
type generation struct {
	// 记录当前 generation 所包含的修改次数, 即 revs 数组的长度
	ver     int64
	// 记录创建当前 generation 实例创建时对应的 revision 信息
	created revision // when the generation is created (put in first revision).
	// 当客户端不断更新该键值对时, revs 数组会不断追加每次更新对应的 revision 信息
	revs    []revision
}

func (g *generation) isEmpty() bool { return g == nil || len(g.revs) == 0 }

// walk walks through the revisions in the generation in descending order.
// It passes the revision to the given function.
// walk returns until: 1. it finishes walking all pairs 2. the function returns false.
// walk returns the position at where it stopped. If it stopped after
// finishing walking, -1 will be returned.
//
// walk 根据 f 回调函数在该 generation 实例中查找符合条件的 revision 实例
func (g *generation) walk(f func(rev revision) bool) int {
	l := len(g.revs)
	for i := range g.revs {
		ok := f(g.revs[l-i-1]) // 逆序查找 generation.revs 实例
		if !ok {
			// 返回目标 revision 的下标
			return l - i - 1
		}
	}
	return -1
}

func (g *generation) String() string {
	return fmt.Sprintf("g: created[%d] ver[%d], revs %#v\n", g.created, g.ver, g.revs)
}

func (g generation) equal(b generation) bool {
	if g.ver != b.ver {
		return false
	}
	if len(g.revs) != len(b.revs) {
		return false
	}

	for i := range g.revs {
		ar, br := g.revs[i], b.revs[i]
		if ar != br {
			return false
		}
	}
	return true
}
