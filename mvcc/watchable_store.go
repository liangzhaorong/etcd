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
	"go.etcd.io/etcd/auth"
	"sync"
	"time"

	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/traceutil"
	"go.uber.org/zap"
)

// non-const so modifiable by tests
var (
	// chanBufLen is the length of the buffered chan
	// for sending out watched events.
	// See https://github.com/etcd-io/etcd/issues/11906 for more detail.
	chanBufLen = 128

	// maxWatchersPerSync is the number of watchers to sync in a single batch
	maxWatchersPerSync = 512
)

type watchable interface {
	watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc)
	progress(w *watcher)
	rev() int64
}

// watchableStore 结构体完成了注册 watcher 实例、管理 watcher 实例, 以及发送触发 watcher 之后的响应等核心功能.
// 该结构体实现了 Watchable 接口.
type watchableStore struct {
	// 内嵌 store 结构体
	*store

	// mu protects watcher groups and batches. It should never be locked
	// before locking store.mu to avoid deadlock.
	//
	// 在修改 synced watcherGroup、unsynced watcherGroup 等字段时, 需要获取该锁进行同步.
	mu sync.RWMutex

	// victims are watcher batches that were blocked on the watch channel
	//
	// 如果 watcher 实例关联的 ch 通道被阻塞了, 则对应的 watcherBatch 实例会暂时记录到该字段中. 在 watchableStore 实例中
	// 还会启动一个后台 goroutine 来处理该字段中保存的 watcherBatch 实例. watcherBatch 实际上就是 map[*watcher]*eventBatch
	// 类型, watcherBatch 只提供了一个 add() 方法, 用来添加被触发的 watcher 实例与其 eventBatch 之间的映射关系.
	victims []watcherBatch
	// 当有新的 watcherBatch 实例添加到 victims 字段中时, 会向该通道发送一个空结构体作为信号.
	victimc chan struct{}

	// contains all unsynced watchers that needs to sync with events that have happened
	unsynced watcherGroup

	// contains all synced watchers that are in sync with the progress of the store.
	// The key of the map is the key that the watcher watches on.
	//
	// synced watcherGroup 中全部的 watcher 实例都已经同步完毕, 并等待新的更新操作; unsynced watcherGroup 中的 watcher 实例
	// 都落后于当前最新更新操作, 并且有一个单独的后台 goroutine 帮助其进行追赶.
	//
	// 当 etcd 服务端收到客户端的 watch 请求时, 如果请求携带了 revision 参数, 则比较该请求的 revision 信息和 store.currentRev
	// 信息: 如果请求的 revision 信息较大, 则放入 synced watcherGroup 中, 否则放入 unsynced watcherGroup.
	//
	// watchableStore 实例会启动一个后台的 goroutine 持续同步 unsynced watcherGroup, 然后将完成同步的 watcher 实例迁移到
	// synced watcherGroup 中存储.
	synced watcherGroup

	stopc chan struct{}
	// 在 watchableStore 实例中会启动两个后台 goroutine, 在 watchableStore.Close() 方法中会通过该 sync.WaitGroup 实例实现
	// 等待两个后台 goroutine 执行完成的功能.
	wg    sync.WaitGroup
}

// cancelFunc updates unsynced and synced maps when running
// cancel operations.
type cancelFunc func()

func New(lg *zap.Logger, b backend.Backend, le lease.Lessor, as auth.AuthStore, ig ConsistentIndexGetter, cfg StoreConfig) ConsistentWatchableKV {
	return newWatchableStore(lg, b, le, as, ig, cfg)
}

func newWatchableStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, as auth.AuthStore, ig ConsistentIndexGetter, cfg StoreConfig) *watchableStore {
	s := &watchableStore{
		store:    NewStore(lg, b, le, ig, cfg), // 创建 store 实例
		victimc:  make(chan struct{}, 1),
		unsynced: newWatcherGroup(), // 初始化 unsynced watcherGroup
		synced:   newWatcherGroup(), // 初始化 synced watcherGroup
		stopc:    make(chan struct{}),
	}
	// 创建 readView 和 writeView 实例
	s.store.ReadView = &readView{s}
	s.store.WriteView = &writeView{s}
	if s.le != nil {
		// use this store as the deleter so revokes trigger watch events
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write(traceutil.TODO()) })
	}
	if as != nil {
		// TODO: encapsulating consistentindex into a separate package
		as.SetConsistentIndexSyncer(s.store.saveIndex)
	}
	// 初始化 sync.WaitGroup 实例
	s.wg.Add(2)
	go s.syncWatchersLoop() // 启动处理 unsynced watcherGroup 的后台 goroutine
	go s.syncVictimsLoop()  // 启动处理 victims 的后台 goroutine
	return s
}

func (s *watchableStore) Close() error {
	close(s.stopc)
	s.wg.Wait()
	return s.store.Close()
}

// NewWatchStream 创建并返回 watchStream 实例
func (s *watchableStore) NewWatchStream() WatchStream {
	watchStreamGauge.Inc()
	return &watchStream{
		watchable: s,
		ch:        make(chan WatchResponse, chanBufLen),
		cancels:   make(map[WatchID]cancelFunc),
		watchers:  make(map[WatchID]*watcher),
	}
}

func (s *watchableStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc) {
	// 创建 watcher 实例
	wa := &watcher{
		key:    key,
		end:    end,
		minRev: startRev,
		id:     id,
		ch:     ch,
		fcs:    fcs,
	}

	s.mu.Lock()     // 加 mu 锁
	s.revMu.RLock() // 因为要读取 currentRev 字段, 所以还需要加 revMu 锁
	// 比较 startRev 与 store.currentRev, 决定待添加的 watcher 实例是否已经同步完成
	synced := startRev > s.store.currentRev || startRev == 0
	if synced {
		// 设置待添加 watcher 的 minRev 字段值
		wa.minRev = s.store.currentRev + 1
		if startRev > wa.minRev {
			wa.minRev = startRev
		}
	}
	// 如果待添加 watcher 已同步完成, 则将其添加到 synced watcherGroup 中, 否则添加到 unsynced watcherGroup 中.
	if synced {
		s.synced.add(wa)
	} else {
		slowWatcherGauge.Inc()
		s.unsynced.add(wa)
	}
	s.revMu.RUnlock() // 解锁
	s.mu.Unlock()

	watcherGauge.Inc()

	// 返回该 watcher 实例, 以及取消该 watcher 实例的回调函数
	return wa, func() { s.cancelWatcher(wa) }
}

// cancelWatcher removes references of the watcher from the watchableStore
//
// cancelWatcher 取消指定的 watcher 实例
func (s *watchableStore) cancelWatcher(wa *watcher) {
	for {
		s.mu.Lock() // 加 mu 锁
		// 尝试从 unsynced watcherGroup 中删除该 watcher 实例
		if s.unsynced.delete(wa) {
			slowWatcherGauge.Dec()
			break

		// 尝试从 synced watcherGroup 中删除该 watcher 实例
		} else if s.synced.delete(wa) {
			break

		// 如果待删除的 watcher 实例已经因为压缩操作而删除, 则直接返回
		} else if wa.compacted {
			break
		} else if wa.ch == nil {
			// already canceled (e.g., cancel/close race)
			break
		}

		if !wa.victim {
			panic("watcher not victim but not in watch groups")
		}

		// 如果在 (un)synced watcherGroup 中都没有, 则 watcher 可能已经被触发了, 则在 victims 中查找
		var victimBatch watcherBatch
		for _, wb := range s.victims {
			// 查找待删除 watcher 实例对应的 watcherBatch 实例
			if wb[wa] != nil {
				victimBatch = wb
				break
			}
		}
		if victimBatch != nil {
			slowWatcherGauge.Dec()
			// 删除 victims 字段中对应的 watcher 实例
			delete(victimBatch, wa)
			break
		}

		// victim being processed so not accessible; retry
		s.mu.Unlock()
		// 如果未找到, 可能是待删除的 watcher 实例刚刚从 synced watcherGroup 中删除且未添加到 victims 中,
		// 所以稍后进行重试.
		time.Sleep(time.Millisecond)
	}

	watcherGauge.Dec()
	wa.ch = nil
	s.mu.Unlock()
}

func (s *watchableStore) Restore(b backend.Backend) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.store.Restore(b)
	if err != nil {
		return err
	}

	for wa := range s.synced.watchers {
		wa.restore = true
		s.unsynced.add(wa)
	}
	s.synced = newWatcherGroup()
	return nil
}

// syncWatchersLoop syncs the watcher in the unsynced map every 100ms.
//
// watchableStore 每隔 100ms 对 unsynced watcherGroup 进行一次批量的同步.
func (s *watchableStore) syncWatchersLoop() {
	defer s.wg.Done() // 该后台 goroutine 结束时调用

	for {
		s.mu.RLock() // 访问 unsynced 字段是需要加 mu 读锁
		st := time.Now()
		lastUnsyncedWatchers := s.unsynced.size() // 获取当前的 unsynced watcherGroup 的大小
		s.mu.RUnlock()

		unsyncedWatchers := 0
		// 若存在需要进行同步的 watcher 实例
		if lastUnsyncedWatchers > 0 {
			// 则调用 syncWatchers() 方法对 unsynced watcherGroup 中的 watcher 进行批量同步
			unsyncedWatchers = s.syncWatchers()
		}
		// 计算同步周期时长
		syncDuration := time.Since(st)

		waitDuration := 100 * time.Millisecond
		// more work pending?
		if unsyncedWatchers != 0 && lastUnsyncedWatchers > unsyncedWatchers {
			// be fair to other store operations by yielding time taken
			waitDuration = syncDuration
		}

		// 调整该后台 goroutine 执行的时间间隔
		select {
		case <-time.After(waitDuration): // 等待 waitDuration 时长后, 再开始下次同步
		case <-s.stopc:
			return
		}
	}
}

// syncVictimsLoop tries to write precomputed watcher responses to
// watchers that had a blocked watcher channel
//
// syncVictimsLoop 在一个单独的 goroutine 中运行, 该方法会定期处理 watchableStore.victims 中缓存的 watcherBatch 实例.
func (s *watchableStore) syncVictimsLoop() {
	defer s.wg.Done()

	for {
		// 循环处理 victims 中缓存的 watcherBatch 实例
		for s.moveVictims() != 0 {
			// try to update all victim watchers
		}
		s.mu.RLock()
		isEmpty := len(s.victims) == 0
		s.mu.RUnlock()

		// 根据 victims 的长度决定下次处理 victims 的时间间隔
		var tickc <-chan time.Time
		// 如果 victims 中仍有数据, 则 10ms 后再次处理
		if !isEmpty {
			tickc = time.After(10 * time.Millisecond)
		}

		select {
		case <-tickc:

		// 当向 victims 添加 watcherBatch 实例时, 会同时向该 victimc 通道发送一个信号, 这里接收到信号后就立即处理 victims
		case <-s.victimc:
		case <-s.stopc:
			return
		}
	}
}

// moveVictims tries to update watches with already pending event data
func (s *watchableStore) moveVictims() (moved int) {
	s.mu.Lock()
	victims := s.victims
	s.victims = nil // 清空 victims 字段
	s.mu.Unlock()

	var newVictim watcherBatch
	// 遍历 victims 中记录的 watcherBatch 实例
	for _, wb := range victims {
		// try to send responses again
		// 从 watcherBatch 实例中获取 eventBatch 实例
		for w, eb := range wb {
			// watcher has observed the store up to, but not including, w.minRev
			rev := w.minRev - 1
			// 将 eventBatch 封装成 watchResponse, 并调用 watcher.send() 方法尝试将其发送出去
			if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
				pendingEventsGauge.Add(float64(len(eb.evs)))

			// 如果 watcher.ch 通道依然阻塞, 则将对应的 eventBatch 实例重新返回 newVictim 变量中保存
			} else {
				if newVictim == nil {
					newVictim = make(watcherBatch)
				}
				newVictim[w] = eb
				continue
			}
			// 记录重试成功的 eventBatch 个数
			moved++
		}

		// assign completed victim watchers to unsync/sync
		s.mu.Lock()
		s.store.revMu.RLock()
		curRev := s.store.currentRev
		// 遍历 watcherBatch
		for w, eb := range wb {
			// 如果 eventBatch 实例被记录到 newVictim 中, 则表示 watcher.ch 通道依然阻塞,
			// watchResponse 发送失败.
			if newVictim != nil && newVictim[w] != nil {
				// couldn't send watch response; stays victim
				continue
			}
			// 如果 eventBatch 实例未被记录到 newVictim 中, 则表示 watchResponse 发送成功
			w.victim = false
			// 检测当前 watcher 后续是否有未同步的 Event 事件
			if eb.moreRev != 0 {
				w.minRev = eb.moreRev
			}
			// 当前 watcher 未完成同步, 移动到 unsynced watcherGroup 中
			if w.minRev <= curRev {
				s.unsynced.add(w)

			// 当前 watcher 已经完成同步, 移动到 synced watcherGroup 中
			} else {
				slowWatcherGauge.Dec()
				s.synced.add(w)
			}
		}
		s.store.revMu.RUnlock()
		s.mu.Unlock()
	}

	// 使用 newVictim 更新 watchableStore.victims 字段
	if len(newVictim) > 0 {
		s.mu.Lock()
		s.victims = append(s.victims, newVictim)
		s.mu.Unlock()
	}

	// 返回重试成功的 eventBatch 个数
	return moved
}

// syncWatchers syncs unsynced watchers by:
//	1. choose a set of watchers from the unsynced watcher group
//	2. iterate over the set to get the minimum revision and remove compacted watchers
//	3. use minimum revision to get all key-value pairs and send those events to watchers
//	4. remove synced watchers in set from unsynced group and move to synced group
//
// syncWatchers 批量同步 unsynced watcherGroup 中的 watcher 实例. 该方法的大致步骤如下:
// 1. 从 unsynced watcherGroup 中选择一批 watcher 实例, 作为此次需要进行同步的 watcher 实例.
// 2. 从该批 watcher 实例中查找最小的 mainRev 字段值.
// 3. 在 BoltDB 中进行范围查询, 查询 minRev~currentRev（当前 revision 值）的所有键值对.
// 4. 过滤掉该批 watcher 实例中因 minRev~currentRev 之间发生压缩操作而被清除的 watcher 实例.
// 5. 遍历步骤 3 中查询到的键值对, 将其中的更新操作转换为 Event 事件, 然后封装成 WatchResponse, 并写入对应的
//    watcher.ch 通道中. 如果 watcher.ch 通道被填满, 则将 Event 事件记录到 watchableStore.victims 中, 并由
//    另一个后台 goroutine 处理该字段中积累的 Event 事件.
// 6. 将已经完成同步的 watcher 实例记录到 synced watcherGroup 中, 同时将其从 unsycned watcherGroup 中删除.
func (s *watchableStore) syncWatchers() int {
	s.mu.Lock() // 加 mu 互斥锁
	defer s.mu.Unlock()

	// 若 unsynced watcherGroup 为空, 则直接返回
	if s.unsynced.size() == 0 {
		return 0
	}

	s.store.revMu.RLock() // 加 revMu 读锁
	defer s.store.revMu.RUnlock()

	// in order to find key-value pairs from unsynced watchers, we need to
	// find min revision index, and these revisions can be used to
	// query the backend store of key-value pairs
	curRev := s.store.currentRev            // 当前的 revision 信息（main revision 部分的值）
	compactionRev := s.store.compactMainRev // 最近一次压缩后最小的 revision 信息（main revision 部分的值）

	// 从 unsynced watcherGroup 中选择一批 watcher 实例, 作为此次需要进行同步的 watcher 实例, 并将这些 watcher 实例
	// 封装成 watcherGroup 实例返回, 返回 minRev 是这批待同步的 watcher 实例中 minRev 字段最小值.
	wg, minRev := s.unsynced.choose(maxWatchersPerSync, curRev, compactionRev)
	minBytes, maxBytes := newRevBytes(), newRevBytes()
	// 将 minRev 和 currentRev 写入 minBytes 和 maxBytes 中
	revToBytes(revision{main: minRev}, minBytes)
	revToBytes(revision{main: curRev + 1}, maxBytes)

	// UnsafeRange returns keys and values. And in boltdb, keys are revisions.
	// values are actual key-value pairs in backend.
	tx := s.store.b.ReadTx() // 获取只读事务
	tx.RLock() // 只读事务加读锁
	// 在 BoltDB 的 "key" Bucket 中进行范围查询, 查询 minRev~currentRev（当前 revision 值）的所有键值对.
	revs, vs := tx.UnsafeRange(keyBucketName, minBytes, maxBytes, 0)
	var evs []mvccpb.Event
	// 将查询到的键值对转换成对应的 Event 事件
	if s.store != nil && s.store.lg != nil {
		evs = kvsToEvents(s.store.lg, wg, revs, vs)
	} else {
		// TODO: remove this in v3.5
		evs = kvsToEvents(nil, wg, revs, vs)
	}
	tx.RUnlock() // 读取完毕, 释放只读事务读锁

	var victims watcherBatch
	// 将上述 watcher 集合及 Event 事件封装成 watcherBatch
	wb := newWatcherBatch(wg, evs)
	// 遍历 watcherGroup 中所有的 watcher 实例
	for w := range wg.watchers {
		// 更新 watcher.minRev 字段, 这样就不会被同一个修改操作触发两次
		w.minRev = curRev + 1

		// 从 watcherBatch 中获取该 watcher 对应的 eventBatch 实例
		eb, ok := wb[w]
		if !ok {
			// bring un-notified watcher to synced
			// 该 watcher 实例所监听的键值对没有更新操作（在 minRev~currentRev 之间）,
			// 所以没有被触发, 故同步完毕, 这里会将其转移到 synced watcherGroup 中.
			s.synced.add(w)
			s.unsynced.delete(w)
			continue
		}

		if eb.moreRev != 0 {
			// 在 minRev~currentRev 之间触发当前 watcher 的更新操作过多, 无法全部放到一个 eventBatch 中,
			// 这里只将该 watcher.minRev 设置成 moreRev, 则下次处理 unsynced watcherGroup 时, 会将其后的
			// 更新操作查询出来继续处理.
			w.minRev = eb.moreRev
		}

		// 将前面创建的 Event 事件集合封装成 WatchResponse 实例, 然后写入 watcher.ch 通道中
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: curRev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else { // 如果 watcher.ch 通道阻塞, 则将 watcher.victim 字段设置为 true
			if victims == nil {
				victims = make(watcherBatch)
			}
			w.victim = true
		}

		// 如果 watcher.ch 通道阻塞, 则将触发它的 Event 事件记录到 victims 中
		if w.victim {
			victims[w] = eb
		} else {
			// 如果后续还有其他未处理的 Event 事件, 则当前 watcher 依然未同步完成
			if eb.moreRev != 0 {
				// stay unsynced; more to read
				continue
			}
			// 当前 watcher 已经同步完成, 加入 synced watcherGroup 中
			s.synced.add(w)
		}
		// 将当前 watcher 从 unsynced watcherGroup 中删除
		s.unsynced.delete(w)
	}
	// 将 victims 变量中记录的 watcherBatch 添加到 watchableStore.victims 中, 等待处理
	s.addVictim(victims)

	vsz := 0
	for _, v := range s.victims {
		vsz += len(v)
	}
	slowWatcherGauge.Set(float64(s.unsynced.size() + vsz))

	// 返回此次处理之后 unsynced watcherGroup 的长度
	return s.unsynced.size()
}

// kvsToEvents gets all events for the watchers from all key-value pairs
//
// kvsToEvents 将从 BoltDB 中查询到的键值对信息转换成相应的 Event 实例
func kvsToEvents(lg *zap.Logger, wg *watcherGroup, revs, vals [][]byte) (evs []mvccpb.Event) {
	// 键值对数据
	for i, v := range vals {
		var kv mvccpb.KeyValue
		// 反序列化得到 KeyValue 实例
		if err := kv.Unmarshal(v); err != nil {
			if lg != nil {
				lg.Panic("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
			} else {
				plog.Panicf("cannot unmarshal event: %v", err)
			}
		}

		// 在此处理的所有 watcher 实例中, 是否有监听了该 Key 的实例,
		// 需要注意的是, unsynced watcherGroup 中可能同时记录了监听
		// 单个 Key 的 watcher 和监听范围的 watcher, 所以在 watcherGroup.contain()
		// 方法中会同时查找这两种.
		if !wg.contains(string(kv.Key)) {
			continue
		}

		// 默认是更新操作
		ty := mvccpb.PUT
		// 如果是 tombstone, 则表示删除操作
		if isTombstone(revs[i]) {
			ty = mvccpb.DELETE
			// patch in mod revision so watchers won't skip
			kv.ModRevision = bytesToRev(revs[i]).main
		}
		// 将该键值对转换成对应的 Event 实例, 并记录下来
		evs = append(evs, mvccpb.Event{Kv: &kv, Type: ty})
	}
	return evs
}

// notify notifies the fact that given event at the given rev just happened to
// watchers that watch on the key of the event.
func (s *watchableStore) notify(rev int64, evs []mvccpb.Event) {
	var victim watcherBatch
	// 将传入的 Event 实例转换成 watcherBatch（即 map[*watcher]*eventBatch 类型）, 之后进行遍历, 逐个 watcher 进行处理
	for w, eb := range newWatcherBatch(&s.synced, evs) {
		if eb.revs != 1 {
			if s.store != nil && s.store.lg != nil {
				s.store.lg.Panic(
					"unexpected multiple revisions in watch notification",
					zap.Int("number-of-revisions", eb.revs),
				)
			} else {
				plog.Panicf("unexpected multiple revisions in notification")
			}
		}
		// 将当前 watcher 对应的 Event 实例封装成 watchResponse 实例, 并尝试写入 watcher.ch 通道中
		if w.send(WatchResponse{WatchID: w.id, Events: eb.evs, Revision: rev}) {
			pendingEventsGauge.Add(float64(len(eb.evs)))
		} else { // 如果 watcher.ch 通道阻塞, 则将这些 Event 实例记录到 watchableStore.victims 字段中
			// move slow watcher to victims
			w.minRev = rev + 1
			if victim == nil {
				victim = make(watcherBatch)
			}
			w.victim = true
			victim[w] = eb
			s.synced.delete(w) // 将该 watcher 从 synced watcherGroup 中删除
			slowWatcherGauge.Inc()
		}
	}
	s.addVictim(victim)
}

func (s *watchableStore) addVictim(victim watcherBatch) {
	if victim == nil {
		return
	}
	// 将 watcherBatch 实例追加到 watchableStore.victims 字段中
	s.victims = append(s.victims, victim)
	select {
	// 向 victimc 通道中发送一个信号
	case s.victimc <- struct{}{}:
	default:
	}
}

func (s *watchableStore) rev() int64 { return s.store.Rev() }

// progress 查询指定 watcher 的处理进度（即当前 watcher 正在处理哪个 revision 中的更新操作）
func (s *watchableStore) progress(w *watcher) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 只有 watcher 在 synced watcherGroup 中时, 才会响应该方法
	if _, ok := s.synced.watchers[w]; ok {
		// 注意这里发送的 watchResponse 实例不包含任何 Event 事件, 只包含当前的 revision 值.
		// 如果当前 watcher.ch 通道已经阻塞, 其中阻塞的 watchResponse 实例中也包含了 revision 值,
		// 也可以说明当前 watcher 的处理进度, 则没有必要继续添加空的 watchResponse 实例了.
		w.send(WatchResponse{WatchID: w.id, Revision: s.rev()})
		// If the ch is full, this watcher is receiving events.
		// We do not need to send progress at all.
	}
}

// watcher 结构体是整个 Watcher 机制的基础.
type watcher struct {
	// the watcher key
	//
	// 该 watcher 实例监听的原始 Key 值
	key []byte
	// end indicates the end of the range to watch.
	// If end is set, the watcher is on a range.
	//
	// 该 watcher 实例监听的结束位置（也是一个原始 Key 值）. 如果该字段有值, 则当前 watcher 实例是一个范围 watcher.
	// 如果该字段未设置值, 则当前 watcher 只监听上面的 key 字段对应的键值对.
	end []byte

	// victim is set when ch is blocked and undergoing victim processing
	//
	// 当 ch 通道阻塞时, 会将该字段设置成 true
	victim bool

	// compacted is set when the watcher is removed because of compaction
	//
	// 如果该字段被设置为 true, 则表示当前 watcher 已经因为发生了压缩操作而被删除.
	compacted bool

	// restore is true when the watcher is being restored from leader snapshot
	// which means that this watcher has just been moved from "synced" to "unsynced"
	// watcher group, possibly with a future revision when it was first added
	// to the synced watcher
	// "unsynced" watcher revision must always be <= current revision,
	// except when the watcher were to be moved from "synced" watcher group
	restore bool

	// minRev is the minimum revision update the watcher will accept
	//
	// 能够触发当前 watcher 实例的最小 revision 值. 发生在该 revision 之前的更新操作是
	// 无法触发该 watcher 实例的.
	minRev int64
	// 当前 watcher 实例的唯一标识
	id     WatchID

	// 过滤器. 触发当前 watcher 实例的事件（Event 结构体就是对 "事件" 的抽象）需要经过
	// 这些过滤器的过滤才能封装进响应（ WatchResponse 结构体就是对 "响应" 的抽象）中.
	fcs []FilterFunc
	// a chan to send out the watch response.
	// The chan might be shared with other watchers.
	//
	// 当前 watcher 实例被触发后, 会向该通道中写入 WatchResponse. 该通道可能是由多个 watcher 实例共享的.
	ch chan<- WatchResponse
}

// send 方法会使用 watcher.fcs 中记录的过滤器对 Event 进行过滤, 只有通过过滤的 Event 实例才能被放出去.
func (w *watcher) send(wr WatchResponse) bool {
	// 发送 WatchResponse 中封装的 Event 事件
	progressEvent := len(wr.Events) == 0

	// watcher.fcs 字段中记录了过滤 Event 实例的过滤器
	if len(w.fcs) != 0 {
		ne := make([]mvccpb.Event, 0, len(wr.Events))
		// 遍历 WatchResponse 中封装的 Event 实例, 并逐个进行过滤
		for i := range wr.Events {
			// 默认 false 表示过滤成功
			filtered := false
			for _, filter := range w.fcs {
				if filter(wr.Events[i]) {
					filtered = true
					break
				}
			}
			// 通过所有过滤器检测的 Event 实例才能被记录到 WatchResponse 中
			if !filtered {
				ne = append(ne, wr.Events[i])
			}
		}
		wr.Events = ne
	}

	// if all events are filtered out, we should send nothing.
	// 如果没有通过过滤的 Event 实例, 则直接返回 true, 此次发送结束
	if !progressEvent && len(wr.Events) == 0 {
		return true
	}
	select {
	// 将 watchResponse 实例写入 watcher.ch 通道中, 如果 watcher.ch 通道阻塞, 则通过返回 false 进行表示
	case w.ch <- wr:
		return true
	default:
		return false
	}
}
