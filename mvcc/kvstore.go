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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/schedule"
	"go.etcd.io/etcd/pkg/traceutil"

	"github.com/coreos/pkg/capnslog"
	"go.uber.org/zap"
)

var (
	keyBucketName  = []byte("key")
	metaBucketName = []byte("meta")

	consistentIndexKeyName  = []byte("consistent_index")
	scheduledCompactKeyName = []byte("scheduledCompactRev")
	finishedCompactKeyName  = []byte("finishedCompactRev")

	ErrCompacted = errors.New("mvcc: required revision has been compacted")
	ErrFutureRev = errors.New("mvcc: required revision is a future revision")
	ErrCanceled  = errors.New("mvcc: watcher is canceled")
	ErrClosed    = errors.New("mvcc: closed")

	plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "mvcc")
)

const (
	// markedRevBytesLen is the byte length of marked revision.
	// The first `revBytesLen` bytes represents a normal revision. The last
	// one byte is the mark.
	markedRevBytesLen      = revBytesLen + 1
	markBytePosition       = markedRevBytesLen - 1
	markTombstone     byte = 't'
)

var restoreChunkKeys = 10000 // non-const for testing
var defaultCompactBatchLimit = 1000

// ConsistentIndexGetter is an interface that wraps the Get method.
// Consistent index is the offset of an entry in a consistent replicated log.
type ConsistentIndexGetter interface {
	// ConsistentIndex returns the consistent index of current executing entry.
	ConsistentIndex() uint64
}

type StoreConfig struct {
	CompactionBatchLimit int
}

// store 实现了 KV 接口.
type store struct {
	ReadView
	WriteView

	// consistentIndex caches the "consistent_index" key's value. Accessed
	// through atomics so must be 64-bit aligned.
	consistentIndex uint64

	cfg StoreConfig

	// mu read locks for txns and write locks for non-txn store changes.
	//
	// 在开启只读/读写事务时, 需要获取该锁进行同步, 即在 Read() 方法和 Write() 方法中获取该锁, 在 End() 方法中释放.
	// 在进行压缩等非事务性的操作时, 需要加写锁进行同步.
	mu sync.RWMutex

	ig ConsistentIndexGetter

	// 当前 store 实例关联的后端存储
	b       backend.Backend
	// 当前 store 实例关联的内存索引 treeIndex
	kvindex index

	// 租约
	le lease.Lessor

	// revMuLock protects currentRev and compactMainRev.
	// Locked at end of write txn and released after write txn unlock lock.
	// Locked before locking read txn and released after locking.
	//
	// 对 currentRev 字段的访问需要加该锁
	revMu sync.RWMutex
	// currentRev is the revision of the last completed transaction.
	//
	// 记录当前的 revision 信息（main revision 部分的值）
	currentRev int64
	// compactMainRev is the main revision of the last compaction.
	//
	// 记录最近一次压缩后最小的 revision 信息（main revision 部分的值）
	compactMainRev int64

	// bytesBuf8 is a byte slice of length 8
	// to avoid a repetitive allocation in saveIndex.
	//
	// 索引缓冲区, 主要用于记录 ConsistentIndex
	bytesBuf8 []byte

	// FIFO 调度器
	fifoSched schedule.Scheduler

	stopc chan struct{}

	lg *zap.Logger
}

// NewStore returns a new store. It is useful to create a store inside
// mvcc pkg. It should only be used for testing externally.
func NewStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, ig ConsistentIndexGetter, cfg StoreConfig) *store {
	if cfg.CompactionBatchLimit == 0 {
		cfg.CompactionBatchLimit = defaultCompactBatchLimit
	}
	s := &store{
		cfg:     cfg,
		b:       b, // 初始化 backend 字段
		ig:      ig,
		kvindex: newTreeIndex(lg), // 初始化 kvindex 字段

		le: le,

		currentRev:     1,  // currentRev 字段初始化为 1
		compactMainRev: -1, // compactMainRev 字段初始化为 -1

		bytesBuf8: make([]byte, 8),
		fifoSched: schedule.NewFIFOScheduler(),

		stopc: make(chan struct{}),

		lg: lg,
	}
	// 创建 readView 和 writeView 实例
	s.ReadView = &readView{s}
	s.WriteView = &writeView{s}
	if s.le != nil {
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write(traceutil.TODO()) })
	}

	// 获取 backend 的读写事务, 创建名为 "key" 和 "meta" 的两个 Bucket, 然后提交事务
	tx := s.b.BatchTx()
	tx.Lock() // 获取读写事务的锁
	tx.UnsafeCreateBucket(keyBucketName)  // 创建名为 "key" 的 Bucket
	tx.UnsafeCreateBucket(metaBucketName) // 创建名为 "meta" 的 Bucket
	tx.Unlock()
	s.b.ForceCommit() // 提交批量读写事务

	s.mu.Lock() // 获取当前 store 实例的锁
	defer s.mu.Unlock()
	// 从 backend 中恢复当前 store 的所有状态, 其中包括内存中的 BTree 索引等
	if err := s.restore(); err != nil {
		// TODO: return the error instead of panic here?
		panic("failed to recover store from backend")
	}

	return s
}

func (s *store) compactBarrier(ctx context.Context, ch chan struct{}) {
	if ctx == nil || ctx.Err() != nil {
		select {
		case <-s.stopc:
		default:
			// fix deadlock in mvcc,for more information, please refer to pr 11817.
			// s.stopc is only updated in restore operation, which is called by apply
			// snapshot call, compaction and apply snapshot requests are serialized by
			// raft, and do not happen at the same time.
			s.mu.Lock()
			f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
			s.fifoSched.Schedule(f)
			s.mu.Unlock()
		}
		return
	}
	close(ch)
}

func (s *store) Hash() (hash uint32, revision int64, err error) {
	start := time.Now()

	s.b.ForceCommit()
	h, err := s.b.Hash(DefaultIgnores)

	hashSec.Observe(time.Since(start).Seconds())
	return h, s.currentRev, err
}

func (s *store) HashByRev(rev int64) (hash uint32, currentRev int64, compactRev int64, err error) {
	start := time.Now()

	s.mu.RLock()
	s.revMu.RLock()
	compactRev, currentRev = s.compactMainRev, s.currentRev
	s.revMu.RUnlock()

	if rev > 0 && rev <= compactRev {
		s.mu.RUnlock()
		return 0, 0, compactRev, ErrCompacted
	} else if rev > 0 && rev > currentRev {
		s.mu.RUnlock()
		return 0, currentRev, 0, ErrFutureRev
	}

	if rev == 0 {
		rev = currentRev
	}
	keep := s.kvindex.Keep(rev)

	tx := s.b.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	s.mu.RUnlock()

	upper := revision{main: rev + 1}
	lower := revision{main: compactRev + 1}
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	h.Write(keyBucketName)
	err = tx.UnsafeForEach(keyBucketName, func(k, v []byte) error {
		kr := bytesToRev(k)
		if !upper.GreaterThan(kr) {
			return nil
		}
		// skip revisions that are scheduled for deletion
		// due to compacting; don't skip if there isn't one.
		if lower.GreaterThan(kr) && len(keep) > 0 {
			if _, ok := keep[kr]; !ok {
				return nil
			}
		}
		h.Write(k)
		h.Write(v)
		return nil
	})
	hash = h.Sum32()

	hashRevSec.Observe(time.Since(start).Seconds())
	return hash, currentRev, compactRev, err
}

func (s *store) updateCompactRev(rev int64) (<-chan struct{}, error) {
	s.revMu.Lock()
	if rev <= s.compactMainRev {
		ch := make(chan struct{})
		f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
		s.fifoSched.Schedule(f)
		s.revMu.Unlock()
		return ch, ErrCompacted
	}
	if rev > s.currentRev {
		s.revMu.Unlock()
		return nil, ErrFutureRev
	}

	s.compactMainRev = rev

	rbytes := newRevBytes()
	revToBytes(revision{main: rev}, rbytes)

	tx := s.b.BatchTx()
	tx.Lock()
	tx.UnsafePut(metaBucketName, scheduledCompactKeyName, rbytes)
	tx.Unlock()
	// ensure that desired compaction is persisted
	s.b.ForceCommit()

	s.revMu.Unlock()

	return nil, nil
}

// compact 实现键值对的压缩. 该方法首先将传入的参数转换成 revision 实例, 然后在 "meta" Bucket 中
// 记录此次压缩的键值对信息（Key 为 scheduledCompactRev, Value 为 revision）, 之后调用 treeIndex.Compact()
// 方法完成对内存索引的压缩, 最后通过 FIFO Scheduler 异步完成对 BoltDB 的压缩操作.
func (s *store) compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	ch := make(chan struct{})
	// 定义执行压缩操作的 Job 函数
	var j = func(ctx context.Context) {
		if ctx.Err() != nil {
			s.compactBarrier(ctx, ch)
			return
		}
		// 记录当前时间, 主要是为了后面记录监控
		start := time.Now()
		// 将 BTree 中 main revision 部分小于 rev 部分的 revision 全部删除,
		// 返回值是此次压缩过程中涉及的 revision 实例.
		keep := s.kvindex.Compact(rev)
		indexCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))
		// 这里是对 BoltDB 进行压缩的地方
		if !s.scheduleCompaction(rev, keep) {
			s.compactBarrier(nil, ch)
			return
		}
		// 当正常压缩后, 关闭 ch 通道, 通知其他监听的 goroutine
		close(ch)
	}

	// store.fifoSched 是一个 FIFO Scheduler, 在 Scheduler 接口中定义了一个 Schedule(j Job) 方法,
	// 该方法会将 Job 放入调度器进行调度, 其中的 Job 实际上就是一个 func(context.Context) 函数,
	// 这里将对 BoltDB 的压缩操作（即上面定义的 j 函数）放入 FIFO 调度器中, 异步执行.
	s.fifoSched.Schedule(j)
	trace.Step("schedule compaction")
	return ch, nil
}

func (s *store) compactLockfree(rev int64) (<-chan struct{}, error) {
	ch, err := s.updateCompactRev(rev)
	if nil != err {
		return ch, err
	}

	return s.compact(traceutil.TODO(), rev)
}

func (s *store) Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	s.mu.Lock()

	ch, err := s.updateCompactRev(rev)
	trace.Step("check and update compact revision")
	if err != nil {
		s.mu.Unlock()
		return ch, err
	}
	s.mu.Unlock()

	return s.compact(trace, rev)
}

// DefaultIgnores is a map of keys to ignore in hash checking.
var DefaultIgnores map[backend.IgnoreKey]struct{}

func init() {
	DefaultIgnores = map[backend.IgnoreKey]struct{}{
		// consistent index might be changed due to v2 internal sync, which
		// is not controllable by the user.
		{Bucket: string(metaBucketName), Key: string(consistentIndexKeyName)}: {},
	}
}

func (s *store) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.b.BatchTx()
	tx.Lock()
	s.saveIndex(tx)
	tx.Unlock()
	s.b.ForceCommit()
}

// Restore Follower 节点在收到快照数据时, 会使用快照数据恢复当前节点的状态. 在这个恢复过程中就会调用
// store.Restore() 方法完成内存索引和 store 中其他状态的恢复.
func (s *store) Restore(b backend.Backend) error {
	s.mu.Lock() // 获取 mu 上的写锁
	defer s.mu.Unlock()

	close(s.stopc)
	s.fifoSched.Stop() // 关闭当前的 FIFO Scheduler

	atomic.StoreUint64(&s.consistentIndex, 0)
	s.b = b // 更新 store.b 字段, 指向新的 backend 实例
	s.kvindex = newTreeIndex(s.lg) // 更新 store.kvindex 字段, 指向新的内存索引
	s.currentRev = 1 // 重置 currentRev 字段和 compactMainRev 字段
	s.compactMainRev = -1
	s.fifoSched = schedule.NewFIFOScheduler() // 更新 fifoSched 字段, 指向新的 FIFO Scheduler
	s.stopc = make(chan struct{})

	// 调用 restore() 开始恢复内存索引
	return s.restore()
}

// restore 恢复内存索引的大致逻辑是: 首先批量读取 BoltDB 中所有的键值对数据, 然后将每个键值对封装成 revKeyValue 实例,
// 并写入一个通道中, 最后由另一个单独的 goroutine 读取该通道, 并完成内存索引的恢复.
func (s *store) restore() error {
	s.setupMetricsReporter()

	// 创建 min 和 max, 后续在 BoltDB 中进行范围查询时的起始 key 和结束 key 就是 min 和 max
	min, max := newRevBytes(), newRevBytes()
	// min 为 "1_0"
	revToBytes(revision{main: 1}, min)
	// max 为 "MaxInt64_MaxInt64"
	revToBytes(revision{main: math.MaxInt64, sub: math.MaxInt64}, max)

	keyToLease := make(map[string]lease.LeaseID)

	// restore index
	tx := s.b.BatchTx() // 获取读写事务, 并加锁
	tx.Lock()

	// 调用 UnsafeRange() 方法, 在 "meta" Bucket 中查询上次的压缩完成时的相关记录（Key 为 "finishedCompactRev"）
	_, finishedCompactBytes := tx.UnsafeRange(metaBucketName, finishedCompactKeyName, nil, 0)
	// 根据查询结果, 恢复 store.compactMainRev 字段
	if len(finishedCompactBytes) != 0 {
		s.compactMainRev = bytesToRev(finishedCompactBytes[0]).main

		if s.lg != nil {
			s.lg.Info(
				"restored last compact revision",
				zap.String("meta-bucket-name", string(metaBucketName)),
				zap.String("meta-bucket-name-key", string(finishedCompactKeyName)),
				zap.Int64("restored-compact-revision", s.compactMainRev),
			)
		} else {
			plog.Printf("restore compact to %d", s.compactMainRev)
		}
	}
	// 调用 UnsafeRange() 方法, 在 "meta" Bucket 中查询上次的压缩启动时的相关记录（Key 为 "scheduledCompactRev"）
	_, scheduledCompactBytes := tx.UnsafeRange(metaBucketName, scheduledCompactKeyName, nil, 0)
	scheduledCompact := int64(0)
	// 根据查询结果, 更新 scheduledCompact 变量
	if len(scheduledCompactBytes) != 0 {
		scheduledCompact = bytesToRev(scheduledCompactBytes[0]).main
	}

	// index keys concurrently as they're loaded in from tx
	keysGauge.Set(0)
	// 在 restoreIntoIndex() 方法中会启动一个单独的 goroutine, 用于接收从 backend 中读取的键值对数据,
	// 并恢复到新建的内存索引中（store.kvindex）. 注意, 该方法的返回值 rkvc 是一个通道, 也是当前 goroutine
	// 与上述 goroutine 通信的桥梁.
	rkvc, revc := restoreIntoIndex(s.lg, s.kvindex)
	for {
		// 调用 UnsafeRange() 方法查询 BoltDB 中的 "key" Bucket, 返回键值对数量的上限是（restoreChunkKeys）, 默认 10000
		keys, vals := tx.UnsafeRange(keyBucketName, min, max, int64(restoreChunkKeys))
		if len(keys) == 0 { // 查询结果为空, 则直接结束当前 for 循环
			break
		}
		// rkvc blocks if the total pending keys exceeds the restore
		// chunk size to keep keys from consuming too much memory.
		//
		// 将查询到的键值对数据写入 rkvc 这个通道中, 并由 restoreIntoIndex() 方法中创建的 goroutine 进行处理.
		restoreChunk(s.lg, rkvc, keys, vals, keyToLease)
		if len(keys) < restoreChunkKeys {
			// partial set implies final set
			break // 范围查询得到的结果数小于 restoreChunkKeys, 即表示最后一次查询
		}
		// next set begins after where this one ended
		// 更新 min 作为下一次范围查询的起始 key
		newMin := bytesToRev(keys[len(keys)-1][:revBytesLen])
		newMin.sub++
		revToBytes(newMin, min)
	}
	close(rkvc) // 关闭 rkvc 通道
	s.currentRev = <-revc // 从 revc 通道中获取恢复之后的 currentRev

	// keys in the range [compacted revision -N, compaction] might all be deleted due to compaction.
	// the correct revision should be set to compaction revision in the case, not the largest revision
	// we have seen.
	//
	// 矫正 currentRev 和 scheduledCompact 字段
	if s.currentRev < s.compactMainRev {
		s.currentRev = s.compactMainRev
	}
	if scheduledCompact <= s.compactMainRev {
		scheduledCompact = 0
	}

	for key, lid := range keyToLease {
		if s.le == nil {
			panic("no lessor to attach lease")
		}
		// 绑定键值对与指定的 Lease 实例
		err := s.le.Attach(lid, []lease.LeaseItem{{Key: key}})
		if err != nil {
			if s.lg != nil {
				s.lg.Warn(
					"failed to attach a lease",
					zap.String("lease-id", fmt.Sprintf("%016x", lid)),
					zap.Error(err),
				)
			} else {
				plog.Errorf("unexpected Attach error: %v", err)
			}
		}
	}

	tx.Unlock() // 读写事务结束

	// 如果在开始恢复之前, 存在未执行完的压缩操作, 则重启该压缩操作
	if scheduledCompact != 0 {
		s.compactLockfree(scheduledCompact)

		if s.lg != nil {
			s.lg.Info(
				"resume scheduled compaction",
				zap.String("meta-bucket-name", string(metaBucketName)),
				zap.String("meta-bucket-name-key", string(scheduledCompactKeyName)),
				zap.Int64("scheduled-compact-revision", scheduledCompact),
			)
		} else {
			plog.Printf("resume scheduled compaction at %d", scheduledCompact)
		}
	}

	return nil
}

type revKeyValue struct {
	// BoltDB 中的 Key 值, 可以转换得到 revision 实例
	key  []byte
	// BoltDB 中保存的 Value 值
	kv   mvccpb.KeyValue
	// 原始的 Key 值
	kstr string
}

func restoreIntoIndex(lg *zap.Logger, idx index) (chan<- revKeyValue, <-chan int64) {
	// 创建两个通道, 其中 rkvc 通道就是用来传递 revKeyValue 实例的通道
	rkvc, revc := make(chan revKeyValue, restoreChunkKeys), make(chan int64, 1)
	// 启动一个单独 goroutine
	go func() {
		// 记录当前遇到的最大的 main revision 值
		currentRev := int64(1)
		// 在该 goroutine 结束时（即 rkvc 通道被关闭时）, 将当前已知的最大 main revision 值写入
		// revc 通道中, 待其他 goroutine 读取.
		defer func() { revc <- currentRev }()
		// restore the tree index from streaming the unordered index.
		//
		// 虽然 BTree 的查找效率很高, 但是随着 BTree 层次的加深, 效率也随之下降, 这里使用 kiCache 这个 map
		// 做了一层缓存, 更加高效地查找对应的 keyIndex 实例.
		// 前面从 BoltDB 中读取到的键值对数据并不是按照原始 Key 值进行排序的, 如果直接向 BTree 中写入,
		// 则可能会引起节点的分裂等变换操作, 效率比较低. 所以这里使用 kiCache 这个 map 做了一层缓存.
		kiCache := make(map[string]*keyIndex, restoreChunkKeys)
		// 从 rkvc 通道中读取 revKeyValue 实例
		for rkv := range rkvc {
			// 先查询 kiCache 缓存
			ki, ok := kiCache[rkv.kstr]
			// purge kiCache if many keys but still missing in the cache
			//
			// 如果 kiCache 中缓存了大量的 Key, 但是依然没有命中, 则清理缓存
			if !ok && len(kiCache) >= restoreChunkKeys {
				i := 10
				for k := range kiCache {
					delete(kiCache, k)
					if i--; i == 0 {
						break
					}
				}
			}
			// cache miss, fetch from tree index if there
			//
			// 如果缓存未命中, 则从内存索引中查找对应的 keyIndex 实例, 并添加到缓存中.
			if !ok {
				ki = &keyIndex{key: rkv.kv.Key}
				if idxKey := idx.KeyIndex(ki); idxKey != nil {
					kiCache[rkv.kstr], ki = idxKey, idxKey
					ok = true
				}
			}
			// 将 revKeyValue.key 转换成 revision 实例
			rev := bytesToRev(rkv.key)
			// 更新 currentRev 值
			currentRev = rev.main
			if ok {
				// 若当前 revKeyValue 实例对应一个 tombstone 键值对
				if isTombstone(rkv.key) {
					// 在对应的 keyIndex 实例中插入 tombstone
					ki.tombstone(lg, rev.main, rev.sub)
					continue
				}
				// 在对应的 keyIndex 实例中添加正常的 revision 信息
				ki.put(lg, rev.main, rev.sub)
			} else if !isTombstone(rkv.key) {
				// 如果从内存索引中依然未查询到对应的 keyIndex 实例, 则需要填充 keyIndex 实例中其他字段,
				// 并添加到内存索引中.
				ki.restore(lg, revision{rkv.kv.CreateRevision, 0}, rev, rkv.kv.Version)
				idx.Insert(ki)
				kiCache[rkv.kstr] = ki // 同时会将该 keyIndex 实例添加到 kiCache 缓存中
			}
		}
	}()
	return rkvc, revc
}

func restoreChunk(lg *zap.Logger, kvc chan<- revKeyValue, keys, vals [][]byte, keyToLease map[string]lease.LeaseID) {
	// 遍历读取到的 revision 信息
	for i, key := range keys {
		// 创建 revKeyValue 实例
		rkv := revKeyValue{key: key}
		// 反序列化得到 KeyValue
		if err := rkv.kv.Unmarshal(vals[i]); err != nil {
			if lg != nil {
				lg.Fatal("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
			} else {
				plog.Fatalf("cannot unmarshal event: %v", err)
			}
		}
		// 记录对应的原始 Key 值
		rkv.kstr = string(rkv.kv.Key)
		if isTombstone(key) {
			// 删除 tombstone 标识的键值对
			delete(keyToLease, rkv.kstr)

		// 如果键值对有绑定的 Lease 实例, 则记录到 keyToLease 中
		} else if lid := lease.LeaseID(rkv.kv.Lease); lid != lease.NoLease {
			keyToLease[rkv.kstr] = lid
		} else {
			// 如果该键值对没有绑定的 Lease, 则从 keyToLease 中删除
			delete(keyToLease, rkv.kstr)
		}
		// 将上述 revKeyValue 实例写入 rkvc 通道中, 等待处理
		kvc <- rkv
	}
}

func (s *store) Close() error {
	close(s.stopc)
	s.fifoSched.Stop()
	return nil
}

func (s *store) saveIndex(tx backend.BatchTx) {
	if s.ig == nil {
		return
	}
	bs := s.bytesBuf8
	ci := s.ig.ConsistentIndex()
	// 将 ConsistentIndex 值写入 bytesBuf8 缓冲区中
	binary.BigEndian.PutUint64(bs, ci)
	// put the index into the underlying backend
	// tx has been locked in TxnBegin, so there is no need to lock it again
	//
	// 将 ConsistentIndex 值记录到 "meta" Bucket 中
	tx.UnsafePut(metaBucketName, consistentIndexKeyName, bs)
	atomic.StoreUint64(&s.consistentIndex, ci)
}

func (s *store) ConsistentIndex() uint64 {
	if ci := atomic.LoadUint64(&s.consistentIndex); ci > 0 {
		return ci
	}
	tx := s.b.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	_, vs := tx.UnsafeRange(metaBucketName, consistentIndexKeyName, nil, 0)
	if len(vs) == 0 {
		return 0
	}
	v := binary.BigEndian.Uint64(vs[0])
	atomic.StoreUint64(&s.consistentIndex, v)
	return v
}

func (s *store) setupMetricsReporter() {
	b := s.b
	reportDbTotalSizeInBytesMu.Lock()
	reportDbTotalSizeInBytes = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesMu.Unlock()
	reportDbTotalSizeInBytesDebugMu.Lock()
	reportDbTotalSizeInBytesDebug = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesDebugMu.Unlock()
	reportDbTotalSizeInUseInBytesMu.Lock()
	reportDbTotalSizeInUseInBytes = func() float64 { return float64(b.SizeInUse()) }
	reportDbTotalSizeInUseInBytesMu.Unlock()
	reportDbOpenReadTxNMu.Lock()
	reportDbOpenReadTxN = func() float64 { return float64(b.OpenReadTxN()) }
	reportDbOpenReadTxNMu.Unlock()
	reportCurrentRevMu.Lock()
	reportCurrentRev = func() float64 {
		s.revMu.RLock()
		defer s.revMu.RUnlock()
		return float64(s.currentRev)
	}
	reportCurrentRevMu.Unlock()
	reportCompactRevMu.Lock()
	reportCompactRev = func() float64 {
		s.revMu.RLock()
		defer s.revMu.RUnlock()
		return float64(s.compactMainRev)
	}
	reportCompactRevMu.Unlock()
}

// appendMarkTombstone appends tombstone mark to normal revision bytes.
func appendMarkTombstone(lg *zap.Logger, b []byte) []byte {
	if len(b) != revBytesLen {
		if lg != nil {
			lg.Panic(
				"cannot append tombstone mark to non-normal revision bytes",
				zap.Int("expected-revision-bytes-size", revBytesLen),
				zap.Int("given-revision-bytes-size", len(b)),
			)
		} else {
			plog.Panicf("cannot append mark to non normal revision bytes")
		}
	}
	return append(b, markTombstone)
}

// isTombstone checks whether the revision bytes is a tombstone.
func isTombstone(b []byte) bool {
	return len(b) == markedRevBytesLen && b[markBytePosition] == markTombstone
}
