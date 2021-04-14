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
	"sort"
	"sync"

	"github.com/google/btree"
	"go.uber.org/zap"
)

// index 接口对 Google 开源的 BTree 实现进行了一层封装
type index interface {
	// 查询指定的 key
	Get(key []byte, atRev int64) (rev, created revision, ver int64, err error)
	// 范围查询
	Range(key, end []byte, atRev int64) ([][]byte, []revision)
	Revisions(key, end []byte, atRev int64) []revision
	// 添加元素
	Put(key []byte, rev revision)
	// 添加 Tombstone
	Tombstone(key []byte, rev revision) error
	RangeSince(key, end []byte, rev int64) []revision
	// 压缩 BTree 中的全部 keyIndex
	Compact(rev int64) map[revision]struct{}
	Keep(rev int64) map[revision]struct{}
	Equal(b index) bool

	Insert(ki *keyIndex)
	KeyIndex(ki *keyIndex) *keyIndex
}

// treeIndex 是 v3 版本存储提供的 index 接口实现, 其中内嵌了 sync.RWMutes
type treeIndex struct {
	// 内嵌 sync.RWMutex, 在进行更新操作时, 如 Insert()、Compact() 方法中, 都需要获取该锁.
	sync.RWMutex
	// 关联的 BTree 实例
	tree *btree.BTree
	lg   *zap.Logger
}

// newTreeIndex 创建并初始化 treeIndex 实例
func newTreeIndex(lg *zap.Logger) index {
	return &treeIndex{
		// 将 BTree 的度初始化为 32, 即除了根节点的每个节点至少有 32 个元素, 每个节点最多有 64 个元素.
		tree: btree.New(32), // 创建 BTree 实例
		lg:   lg,
	}
}

// Put 主要完成两项操作, 一是向 BTree 中添加 keyIndex 实例, 而是向 keyIndex 中追加 revision 信息.
func (ti *treeIndex) Put(key []byte, rev revision) {
	// 创建 keyIndex 实例
	keyi := &keyIndex{key: key}

	ti.Lock() // 加锁
	defer ti.Unlock()
	// 通过 BTree.Get() 方法在 BTree 上查找指定的元素
	item := ti.tree.Get(keyi)
	// 在 BTree 中不存在指定的 key
	if item == nil {
		// 向 keyIndex 中追加一个 revision
		keyi.put(ti.lg, rev.main, rev.sub)
		// 通过 BTree.ReplaceOrInsert() 方法向 BTree 中添加 keyIndex 实例
		ti.tree.ReplaceOrInsert(keyi)
		return
	}
	// 如果在 BTree 中查找到该 key 对应的元素, 则向其中追加一个 revision 实例
	okeyi := item.(*keyIndex)
	okeyi.put(ti.lg, rev.main, rev.sub)
}

// Get 从 BTree 中查询小于 atRev（main revision）的最大 revision 信息
func (ti *treeIndex) Get(key []byte, atRev int64) (modified, created revision, ver int64, err error) {
	// 创建 keyIndex 实例
	keyi := &keyIndex{key: key}
	ti.RLock() // 加读锁
	defer ti.RUnlock()
	// 查询指定 key 对应的 keyIndex 实例
	if keyi = ti.keyIndex(keyi); keyi == nil {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}
	// 在查询到的 keyIndex 实例中, 查找小于 atRev（main revision）的最大 revision 信息.
	return keyi.get(ti.lg, atRev)
}

func (ti *treeIndex) KeyIndex(keyi *keyIndex) *keyIndex {
	ti.RLock()
	defer ti.RUnlock()
	return ti.keyIndex(keyi)
}

// keyIndex 调用 BTree.Get() 方法从 BTree 中获取指定的元素
func (ti *treeIndex) keyIndex(keyi *keyIndex) *keyIndex {
	if item := ti.tree.Get(keyi); item != nil {
		return item.(*keyIndex)
	}
	return nil
}

// visit 遍历 key~end 之间的所有元素, 并对每个元素调用 f 回调函数进行处理
func (ti *treeIndex) visit(key, end []byte, f func(ki *keyIndex)) {
	keyi, endi := &keyIndex{key: key}, &keyIndex{key: end}

	ti.RLock()
	defer ti.RUnlock()

	// 正序遍历比 keyi 大的元素
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		// 直到遍历到的元素小于 endi 为止, 即忽略 end 之后的元素
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}
		f(item.(*keyIndex))
		return true
	})
}

// Revisions 获取 key~end 范围内所有 main revision 小于 atRev 的最大 revision
func (ti *treeIndex) Revisions(key, end []byte, atRev int64) (revs []revision) {
	// 若未指定 end 参数
	if end == nil {
		// 则返回 key 对应的小于 atRev 这个 main revision 的最大 revision 信息
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil
		}
		return []revision{rev}
	}
	// 遍历 key~end 之间的 keyIndex, 返回所有小于 atRev 的最大 revision
	ti.visit(key, end, func(ki *keyIndex) {
		if rev, _, _, err := ki.get(ti.lg, atRev); err == nil {
			revs = append(revs, rev)
		}
	})
	return revs
}

// Range 查询 BTree 中 key~end 之间元素中指定 revision 信息
func (ti *treeIndex) Range(key, end []byte, atRev int64) (keys [][]byte, revs []revision) {
	// 如果未指定 end 参数
	if end == nil {
		// 则只查询 key 对应的 revision 信息
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil, nil
		}
		return [][]byte{key}, []revision{rev}
	}
	ti.visit(key, end, func(ki *keyIndex) {
		// 从当前的 keyIndex 实例 ki 中获取指定的 revision 信息, 并追加到 revs 中
		if rev, _, _, err := ki.get(ti.lg, atRev); err == nil {
			revs = append(revs, rev)
			keys = append(keys, ki.key) // 将 key 追加到 keys 中
		}
	})
	return keys, revs
}

// Tombstone 先在 BTree 中查找 key 对应的 keyIndex 实例, 然后调用 keyIndex.tombstone() 方法结束
// 其当前 generation 实例并创建新的 generation 实例.
func (ti *treeIndex) Tombstone(key []byte, rev revision) error {
	// 创建 key 对应的 keyIndex 实例
	keyi := &keyIndex{key: key}

	ti.Lock() // 加锁
	defer ti.Unlock()
	// 获取指定 key 的 keyIndex 实例
	item := ti.tree.Get(keyi)
	if item == nil {
		return ErrRevisionNotFound
	}

	ki := item.(*keyIndex)
	// 在当前的 generation 末尾添加 rev 这个 revision 实例, 然后创建一个新的 generation 实例
	return ki.tombstone(ti.lg, rev.main, rev.sub)
}

// RangeSince returns all revisions from key(including) to end(excluding)
// at or after the given rev. The returned slice is sorted in the order
// of revision.
//
// RangeSince 查询 key~end 的元素, 并从中查询 main revision 部分大于指定值的 revision 信息.
// 注意, 最终返回的 revision 实例不是按照 key 进行排序的, 而是按照 revision.GreaterThan() 排序的（从小到大）.
func (ti *treeIndex) RangeSince(key, end []byte, rev int64) []revision {
	// 创建 key 对应的 keyIndex 实例
	keyi := &keyIndex{key: key}

	ti.RLock() // 加锁
	defer ti.RUnlock()

	// 若未指定 end 参数
	if end == nil {
		// 则只查询 key 对应的 revision 信息
		item := ti.tree.Get(keyi)
		if item == nil {
			return nil
		}
		keyi = item.(*keyIndex)
		// 调用 keyIndex.since() 方法, 获取 rev 之后的 revision 实例.
		return keyi.since(ti.lg, rev)
	}

	// 创建 end 对应的 keyIndex 实例
	endi := &keyIndex{key: end}
	var revs []revision
	// 调用 BTree.AscendGreaterOrEqual() 方法遍历比 key 大的元素
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		// 直到比 end 大为止, 即忽略大于 end 的元素
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}
		curKeyi := item.(*keyIndex)
		// 调用 keyIndex.since() 方法获取当前 keyIndex 实例中 rev 之后的 revision 实例
		revs = append(revs, curKeyi.since(ti.lg, rev)...)
		return true
	})
	// 对 revs 中记录的 revision 实例进行排序, 并返回
	sort.Sort(revisions(revs))

	return revs
}

// Compact 将 BTree 中 main revision 部分小于 rev 部分的 revision 全部删除
func (ti *treeIndex) Compact(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	if ti.lg != nil {
		ti.lg.Info("compact tree index", zap.Int64("revision", rev))
	} else {
		plog.Printf("store.index: compact %d", rev)
	}
	ti.Lock() // 加锁
	// 调用 BTree.Clone() 克隆 ti.tree 的一个副本 BTree 实例
	clone := ti.tree.Clone()
	ti.Unlock()

	// 调用 BTree.Ascend() 方法遍历其中的全部 keyIndex 实例
	clone.Ascend(func(item btree.Item) bool {
		keyi := item.(*keyIndex)
		//Lock is needed here to prevent modification to the keyIndex while
		//compaction is going on or revision added to empty before deletion
		ti.Lock() // 加锁
		// 移除 main revision 小于 rev 的所有 revision 以及 generation
		keyi.compact(ti.lg, rev, available)
		if keyi.isEmpty() {
			// 删除 BTree 中已经为空的 keyIndex 实例
			item := ti.tree.Delete(keyi)
			if item == nil {
				if ti.lg != nil {
					ti.lg.Panic("failed to delete during compaction")
				} else {
					plog.Panic("store.index: unexpected delete failure during compaction")
				}
			}
		}
		ti.Unlock()
		return true
	})
	return available
}

// Keep finds all revisions to be kept for a Compaction at the given rev.
func (ti *treeIndex) Keep(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	ti.RLock()
	defer ti.RUnlock()
	ti.tree.Ascend(func(i btree.Item) bool {
		keyi := i.(*keyIndex)
		keyi.keep(rev, available)
		return true
	})
	return available
}

func (ti *treeIndex) Equal(bi index) bool {
	b := bi.(*treeIndex)

	if ti.tree.Len() != b.tree.Len() {
		return false
	}

	equal := true

	ti.tree.Ascend(func(item btree.Item) bool {
		aki := item.(*keyIndex)
		bki := b.tree.Get(item).(*keyIndex)
		if !aki.equal(bki) {
			equal = false
			return false
		}
		return true
	})

	return equal
}

// Insert 直接调用 BTree.ReplaceOrInsert() 方法插入 keyIndex 实例.
func (ti *treeIndex) Insert(ki *keyIndex) {
	// 加全局锁
	ti.Lock()
	defer ti.Unlock()
	ti.tree.ReplaceOrInsert(ki)
}
