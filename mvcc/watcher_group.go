// Copyright 2016 The etcd Authors
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
	"fmt"
	"math"

	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/adt"
)

var (
	// watchBatchMaxRevs is the maximum distinct revisions that
	// may be sent to an unsynced watcher at a time. Declared as
	// var instead of const for testing purposes.
	watchBatchMaxRevs = 1000
)

// eventBatch 为提高效率, etcd 的服务端一般会批量处理 watcher 事件, 在结构体 eventBatch 中
// 可以封装多个 Event 事件.
type eventBatch struct {
	// evs is a batch of revision-ordered events
	//
	// 记录的 Event 实例时按照 revision 排序的
	evs []mvccpb.Event
	// revs is the minimum unique revisions observed for this batch
	//
	// 记录当前的 eventBatch 中记录的 Event 实例来自多少个不同的 main revision,
	// 特别注意, 是不相同的 main revision 值的个数.
	revs int
	// moreRev is first revision with more events following this batch
	//
	// 由于当前 eventBatch 中记录的 Event 个数达到上限之后, 后续 Event 实例
	// 无法加入该 eventBatch 中, 该字段记录了无法加入该 eventBatch 实例的第
	// 一个 Event 实例对应的 main revision 值.
	moreRev int64
}

// add 将 Event 实例添加到 eventBatch 中.
func (eb *eventBatch) add(ev mvccpb.Event) {
	// 检测 revs 是否达到上限, 若达到上限, 直接返回
	if eb.revs > watchBatchMaxRevs {
		// maxed out batch size
		return
	}

	// 第一次添加 Event 实例
	if len(eb.evs) == 0 {
		// base case
		eb.revs = 1
		// 将 Event 实例添加到 evs 字段中保存
		eb.evs = append(eb.evs, ev)
		return
	}

	// revision accounting
	// 获取最后一个 Event 实例对应的 main revision 值
	ebRev := eb.evs[len(eb.evs)-1].Kv.ModRevision
	// 新增 Event 实例对应的 main revision 值
	evRev := ev.Kv.ModRevision
	// 比较两个 main revision 值
	if evRev > ebRev {
		// 递增 revs 字段
		eb.revs++
		if eb.revs > watchBatchMaxRevs {
			// 当前 eventBatch 实例中记录的 Event 实例已达上限, 则更新 moreRev 字段,
			// 记录最后一个无法加入的 Event 实例的 main revision 值.
			eb.moreRev = evRev
			return
		}
	}

	// 如果能继续添加 Event 实例, 则将 Event 实例添加到 evs 中.
	eb.evs = append(eb.evs, ev)
}

type watcherBatch map[*watcher]*eventBatch

func (wb watcherBatch) add(w *watcher, ev mvccpb.Event) {
	eb := wb[w]
	if eb == nil {
		eb = &eventBatch{}
		wb[w] = eb
	}
	// 将传入事件 ev 添加到 eventBatch 中
	eb.add(ev)
}

// newWatcherBatch maps watchers to their matched events. It enables quick
// events look up by watcher.
func newWatcherBatch(wg *watcherGroup, evs []mvccpb.Event) watcherBatch {
	// 若没有同步完成的 watcher 实例, 则直接返回 nil
	if len(wg.watchers) == 0 {
		return nil
	}

	wb := make(watcherBatch)
	// 遍历所有事件
	for _, ev := range evs {
		// 根据 key 检索出正在监听该 key 的 watcher 实例
		for w := range wg.watcherSetByKey(string(ev.Kv.Key)) {
			// 事件中的版本号必须要大于等于 watcher 监听的最小版本号,
			// 才允许将事件发送到此 watcher 的事件 channel 中
			if ev.Kv.ModRevision >= w.minRev {
				// don't double notify
				wb.add(w, ev)
			}
		}
	}
	return wb
}

type watcherSet map[*watcher]struct{}

func (w watcherSet) add(wa *watcher) {
	if _, ok := w[wa]; ok {
		panic("add watcher twice!")
	}
	w[wa] = struct{}{}
}

func (w watcherSet) union(ws watcherSet) {
	for wa := range ws {
		w.add(wa)
	}
}

func (w watcherSet) delete(wa *watcher) {
	if _, ok := w[wa]; !ok {
		panic("removing missing watcher!")
	}
	delete(w, wa)
}

type watcherSetByKey map[string]watcherSet

func (w watcherSetByKey) add(wa *watcher) {
	set := w[string(wa.key)]
	if set == nil {
		set = make(watcherSet)
		w[string(wa.key)] = set
	}
	set.add(wa)
}

func (w watcherSetByKey) delete(wa *watcher) bool {
	k := string(wa.key)
	if v, ok := w[k]; ok {
		if _, ok := v[wa]; ok {
			delete(v, wa)
			if len(v) == 0 {
				// remove the set; nothing left
				delete(w, k)
			}
			return true
		}
	}
	return false
}

// watcherGroup is a collection of watchers organized by their ranges
//
// watcherGroup 负责管理多个 watcher, 能够根据 key 快速找到监听该 key 的一个或多个 watcher.
type watcherGroup struct {
	// keyWatchers has the watchers that watch on a single key
	//
	// 记录监听单个 Key 的 watcher 实例, watcherSetByKey 实际上就是 map[string]watcherSet 类型, 该 map 中的 key
	// 就是监听的原始 Key 值. watcherSetByKey 提供了 add() 和 delete() 两个方法, 分别用来添加和删除指定的 watcher
	// 实例.
	keyWatchers watcherSetByKey
	// ranges has the watchers that watch a range; it is sorted by interval
	//
	// 记录进行范围监听的 watcher 实例, IntervalTree（线段树）是二叉树的一种变形, 线段树将一个区间划分成一些单元区间,
	// 每一个区间对应线段树的一个叶节点. 假设在线段树中一个非叶子节点 [a, b], 那么它的左儿子节点表示的区间范围是 [a, (a+b)/2],
	// 右儿子节点表示的区间范围为 ([a+b]/2+1, b), 其所有叶子节点连接起来就表示整个线段的长度.
	ranges adt.IntervalTree
	// watchers is the set of all watchers
	//
	// 记录了当前 watcherGroup 实例中全部的 watcher 实例, 这里的 watcherSet 实际上就是 map[*watcher]struct{} 类型.
	// 在 watcherSet 中定义了 add()、delete() 和 union() 三个方法, 分别用来添加、删除 watcher 实例和合并其他 watcherSet
	// 实例.
	watchers watcherSet
}

func newWatcherGroup() watcherGroup {
	return watcherGroup{
		keyWatchers: make(watcherSetByKey),
		ranges:      adt.NewIntervalTree(),
		watchers:    make(watcherSet),
	}
}

// add puts a watcher in the group.
//
// add 添加一个 watcher 实例到 watcherGroup 中.
func (wg *watcherGroup) add(wa *watcher) {
	// 首先将 watcher 实例添加到 watchers 字段中保存
	wg.watchers.add(wa)
	// watcher.end 为空表示只监听单个 Key, 则将其添加到 keyWatchers 中保存
	if wa.end == nil {
		wg.keyWatchers.add(wa)
		return
	}

	// 如果待添加的是范围 watcher, 则执行下面的添加逻辑

	// interval already registered?
	// 根据待添加 watcher 的 key 和 end 字段创建 IntervalTree 中的一个节点
	ivl := adt.NewStringAffineInterval(string(wa.key), string(wa.end))
	if iv := wg.ranges.Find(ivl); iv != nil {
		// 如果在 IntervalTree 中查找到了对应节点, 则将该 watcher 添加到对应的 watcherSet 中
		iv.Val.(watcherSet).add(wa)
		return
	}

	// not registered, put in interval tree
	// 当前 IntervalTree 中没有对应节点, 则创建对应的 watcherSet, 并添加到 IntervalTree 中
	ws := make(watcherSet)
	ws.add(wa)
	// 将该节点插入到线段树中
	wg.ranges.Insert(ivl, ws)
}

// contains is whether the given key has a watcher in the group.
func (wg *watcherGroup) contains(key string) bool {
	_, ok := wg.keyWatchers[key]
	return ok || wg.ranges.Intersects(adt.NewStringAffinePoint(key))
}

// size gives the number of unique watchers in the group.
func (wg *watcherGroup) size() int { return len(wg.watchers) }

// delete removes a watcher from the group.
func (wg *watcherGroup) delete(wa *watcher) bool {
	if _, ok := wg.watchers[wa]; !ok {
		return false
	}
	wg.watchers.delete(wa)
	if wa.end == nil {
		wg.keyWatchers.delete(wa)
		return true
	}

	ivl := adt.NewStringAffineInterval(string(wa.key), string(wa.end))
	iv := wg.ranges.Find(ivl)
	if iv == nil {
		return false
	}

	ws := iv.Val.(watcherSet)
	delete(ws, wa)
	if len(ws) == 0 {
		// remove interval missing watchers
		if ok := wg.ranges.Delete(ivl); !ok {
			panic("could not remove watcher from interval tree")
		}
	}

	return true
}

// choose selects watchers from the watcher group to update
//
// choose 该方法根据 unsynced watcherGroup 中记录的 watcher 个数对其进行分批返回. 另外, 它还会获取该批 watcher
// 实例中查找最小的 minRev 字段.
func (wg *watcherGroup) choose(maxWatchers int, curRev, compactRev int64) (*watcherGroup, int64) {
	// 当前 unsynced watcherGroup 中记录的 watcher 个数未达到指定上限（默认值为 512）, 则直接调用
	// unsynced watcherGroup 中的 chooseAll() 方法获取所有未完成同步的 watcher 中最小的 minRev.
	if len(wg.watchers) < maxWatchers {
		return wg, wg.chooseAll(curRev, compactRev)
	}
	// 如果当前 unsynced watcherGroup 中的 watcher 个数超过指定上限, 则需要分批处理
	ret := newWatcherGroup() // 创建新的 watcherGroup 实例
	// 从 unsynced watcherGroup 中遍历得到指定量的 watcher, 并添加到新建的 watcherGroup 实例中
	for w := range wg.watchers {
		if maxWatchers <= 0 {
			break
		}
		maxWatchers--
		ret.add(w)
	}
	// 依然通过 chooseAll() 方法从该批 watcher 实例中查找最小的 minRev 值
	return &ret, ret.chooseAll(curRev, compactRev)
}

// chooseAll 遍历 watcherGroup 中记录的全部 watcher 实例, 记录其中最小的 minRev 字段值并返回.
func (wg *watcherGroup) chooseAll(curRev, compactRev int64) int64 {
	minRev := int64(math.MaxInt64) // 用于记录该 watcherGroup 中最小的 minRev 值
	// 遍历 watcherGroup 中所有的 watcher 实例
	for w := range wg.watchers {
		if w.minRev > curRev {
			// after network partition, possibly choosing future revision watcher from restore operation
			// with watch key "proxy-namespace__lostleader" and revision "math.MaxInt64 - 2"
			// do not panic when such watcher had been moved from "synced" watcher during restore operation
			if !w.restore {
				panic(fmt.Errorf("watcher minimum revision %d should not exceed current revision %d", w.minRev, curRev))
			}

			// mark 'restore' done, since it's chosen
			w.restore = false
		}
		// 该 watcher 因为压缩操作而被删除
		if w.minRev < compactRev {
			select {
			// 创建 watcherResponse（注意, 其 CompactRevision 字段是有值的）, 并写入 watcher.ch 通道
			case w.ch <- WatchResponse{WatchID: w.id, CompactRevision: compactRev}:
				w.compacted = true // 将 watcher.compacted 设置为 true, 表示因压缩操作而被删除
				wg.delete(w)       // 将该 watcher 实例从 watcherGroup 中删除
			default:
				// retry next time
				// 如果 watcher.ch 通道阻塞, 则下次调用 chooseAll() 方法时再尝试重新删除该 watcher 实例
			}
			continue
		}
		// 更新 minRev, 记录最小的 watcher.minRev 字段
		if minRev > w.minRev {
			minRev = w.minRev
		}
	}
	return minRev
}

// watcherSetByKey gets the set of watchers that receive events on the given key.
//
// watcherSetByKey 根据 key 检索出正在监听该 key 的 watcher 实例
func (wg *watcherGroup) watcherSetByKey(key string) watcherSet {
	wkeys := wg.keyWatchers[key]
	wranges := wg.ranges.Stab(adt.NewStringAffinePoint(key))

	// zero-copy cases
	switch {
	case len(wranges) == 0:
		// no need to merge ranges or copy; reuse single-key set
		return wkeys
	case len(wranges) == 0 && len(wkeys) == 0:
		return nil
	case len(wranges) == 1 && len(wkeys) == 0:
		return wranges[0].Val.(watcherSet)
	}

	// copy case
	ret := make(watcherSet)
	ret.union(wg.keyWatchers[key])
	for _, item := range wranges {
		ret.union(item.Val.(watcherSet))
	}
	return ret
}
