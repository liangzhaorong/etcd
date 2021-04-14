// Copyright 2017 The etcd Authors
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
	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/traceutil"
	"go.uber.org/zap"
)

type storeTxnRead struct {
	// 该 storeTxnRead 实例关联的 store 实例
	s  *store
	// 该 storeTxnRead 实例关联的 ReadTx 实例
	tx backend.ReadTx

	// 分别对应于关联的 store 实例的 compactMainRev 和 currentRev 字段
	firstRev int64 // compactMainRev: 记录最近一次压缩后最小的 revision 信息（main revision 部分的值）
	rev      int64 // currentRev: 记录当前的 revision 信息（main revision 部分的值）

	trace *traceutil.Trace
}

func (s *store) Read(trace *traceutil.Trace) TxnRead {
	s.mu.RLock() // 加读锁
	s.revMu.RLock()
	// backend holds b.readTx.RLock() only when creating the concurrentReadTx. After
	// ConcurrentReadTx is created, it will not block write transaction.
	//
	// 创建一个不阻塞的只读事务
	tx := s.b.ConcurrentReadTx()
	// 读取 store 中的 compactMainRev 字段和 currentRev 字段, 加读锁同步
	tx.RLock() // RLock is no-op. concurrentReadTx does not need to be locked after it is created.
	firstRev, rev := s.compactMainRev, s.currentRev
	s.revMu.RUnlock() // 读取完 compactMainRev 字段和 currentRev 字段后, 释放读锁
	// metricsTxnWrite 结构体同时实现了 TxnRead 和 TxnWrite 接口, 并在原有的功能上记录监控信息
	return newMetricsTxnRead(&storeTxnRead{s, tx, firstRev, rev, trace})
}

func (tr *storeTxnRead) FirstRev() int64 { return tr.firstRev }
func (tr *storeTxnRead) Rev() int64      { return tr.rev }

func (tr *storeTxnRead) Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	return tr.rangeKeys(key, end, tr.Rev(), ro)
}

// End 结束事务, 这里会释放在 store.Read() 方法中获取的 store.mu 上的读锁和底层只读事务上的锁.
func (tr *storeTxnRead) End() {
	tr.tx.RUnlock() // RUnlock signals the end of concurrentReadTx.
	tr.s.mu.RUnlock()
}

// storeTxnWrite 实现了 TxnWrite 接口, 其中内嵌了 storeTxnRead
type storeTxnWrite struct {
	storeTxnRead
	// 当前 storeTxnWrite 实例关联的读写事务, 实际指向 backend.batchTx 字段
	tx backend.BatchTx
	// beginRev is the revision where the txn begins; it will write to the next revision.
	//
	// 事务创建时, 记录创建当前 storeTxnWrite 实例时 store.currentRev（main revison） 字段的值
	// 之后每次事务更改操作都会底层该值
	beginRev int64
	// 在当前读写事务中发生改动的键值对信息 (注意, 每次更新操作的 次版本号 是该切片的长度)
	changes  []mvccpb.KeyValue
}

// Write 创建一个读写事务
func (s *store) Write(trace *traceutil.Trace) TxnWrite {
	s.mu.RLock() // 加读锁
	// 获取读写事务
	tx := s.b.BatchTx()
	tx.Lock() // 获取读写事务的锁
	// 创建 storeTxnWrite 实例
	tw := &storeTxnWrite{
		storeTxnRead: storeTxnRead{s, tx, 0, 0, trace},
		tx:           tx,
		beginRev:     s.currentRev,
		changes:      make([]mvccpb.KeyValue, 0, 4),
	}
	// 将 storeTxnWrite 实例封装成 metricsTxnWrite 实例并返回
	return newMetricsTxnWrite(tw)
}

func (tw *storeTxnWrite) Rev() int64 { return tw.beginRev }

func (tw *storeTxnWrite) Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	rev := tw.beginRev
	if len(tw.changes) > 0 {
		// 如果当前读写事务中有更新键值对的操作, 则该方法也需要能查询到这些更新的键值对,
		// 所以传入的 rev 参数是 beginRev+1
		rev++
	}
	return tw.rangeKeys(key, end, rev, ro)
}

func (tw *storeTxnWrite) DeleteRange(key, end []byte) (int64, int64) {
	if n := tw.deleteRange(key, end); n != 0 || len(tw.changes) > 0 {
		// 根据当前读写事务是否有更新操作, 决定返回的 main revision 值
		return n, tw.beginRev + 1
	}
	return 0, tw.beginRev
}

// Put 向存储中添加一个键值对数据
func (tw *storeTxnWrite) Put(key, value []byte, lease lease.LeaseID) int64 {
	tw.put(key, value, lease)
	return tw.beginRev + 1
}

func (tw *storeTxnWrite) End() {
	// only update index if the txn modifies the mvcc state.
	// 检测当前读写事务中是否有修改操作
	if len(tw.changes) != 0 {
		// 将 ConsistentIndex（实际就是当前处理的最后一条 Entry 记录的索引值）添加记录到 BoltDB
		// 中名为 "meta" 的 Bucket 中.
		tw.s.saveIndex(tw.tx)
		// hold revMu lock to prevent new read txns from opening until writeback.
		tw.s.revMu.Lock() // 修改 store 中的 currentRev 字段, 加读锁同步
		tw.s.currentRev++ // 递增 currentRev
	}
	// 在 batchTxBuffered 即 batchTx 中的 Unlock() 方法实现中会将当前读写事务提交, 同时开启新的读写事务
	tw.tx.Unlock()
	if len(tw.changes) != 0 {
		tw.s.revMu.Unlock() // 修改 currentRev 完成, 释放读锁
	}
	tw.s.mu.RUnlock()
}

// rangeKeys 首先扫描内存索引（即 treeIndex）, 得到对应的 revision, 然后通过 revision 查询 BoltDB 得到真正的键值对数据,
// 最后将键值对数据反序列化成 KeyValue 并封装成 RangeResult 实例返回.
func (tr *storeTxnRead) rangeKeys(key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
	rev := ro.Rev
	// 检测 RangeOptions 中指定的 main revision 信息是否合法, 如果不合法, 则根据 curRev（对 storeTxnRead 来说,
	// 就是 rev 字段）值对 rev 进行校正
	if rev > curRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: curRev}, ErrFutureRev
	}
	// 若小于 0, 则使用 curRev
	if rev <= 0 {
		rev = curRev
	}
	// 小于 compactMainRev 的 revision 都是已经被压缩了（即被删除了）
	if rev < tr.s.compactMainRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: 0}, ErrCompacted
	}

	// 获取 key~end 范围内所有 main revision 小于 rev 的最大 revision
	revpairs := tr.s.kvindex.Revisions(key, end, rev)
	tr.trace.Step("range keys from in-memory index tree")
	if len(revpairs) == 0 {
		return &RangeResult{KVs: nil, Count: 0, Rev: curRev}, nil
	}
	// 若为 true, 则表示仅返回键值对个数
	if ro.Count {
		return &RangeResult{KVs: nil, Count: len(revpairs), Rev: curRev}, nil
	}

	limit := int(ro.Limit)
	// 矫正 limit 的值
	if limit <= 0 || limit > len(revpairs) {
		limit = len(revpairs)
	}

	kvs := make([]mvccpb.KeyValue, limit)
	revBytes := newRevBytes()
	for i, revpair := range revpairs[:len(kvs)] {
		// 将 revision 结构体对象转换为 []byte
		revToBytes(revpair, revBytes)
		// 在指定的 Bucket 中进行范围查找. 这里仅查询 revBytes（这个 revision）对应的键值对数据
		_, vs := tr.tx.UnsafeRange(keyBucketName, revBytes, nil, 0)
		// 返回的 value 必须为 1 个
		if len(vs) != 1 {
			if tr.s.lg != nil {
				tr.s.lg.Fatal(
					"range failed to find revision pair",
					zap.Int64("revision-main", revpair.main),
					zap.Int64("revision-sub", revpair.sub),
				)
			} else {
				plog.Fatalf("range cannot find rev (%d,%d)", revpair.main, revpair.sub)
			}
		}
		// 将查询得到的键值对数据进行反序列化, 得到 mvccpb.KeyValue 实例, 并追加到 kvs 中
		if err := kvs[i].Unmarshal(vs[0]); err != nil {
			if tr.s.lg != nil {
				tr.s.lg.Fatal(
					"failed to unmarshal mvccpb.KeyValue",
					zap.Error(err),
				)
			} else {
				plog.Fatalf("cannot unmarshal event: %v", err)
			}
		}
	}
	tr.trace.Step("range keys from bolt db")
	// 将 kvs 封装成 RangeResult 实例并返回
	return &RangeResult{KVs: kvs, Count: len(revpairs), Rev: curRev}, nil
}

func (tw *storeTxnWrite) put(key, value []byte, leaseID lease.LeaseID) {
	// 当前事务产生的修改对应的 main revision 部分都是该值
	rev := tw.beginRev + 1
	c := rev
	oldLease := lease.NoLease

	// if the key exists before, use its previous created and
	// get its previous leaseID
	//
	// 调用 treeIndex.Get() 方法在内存索引中查找对应的键值对信息, 在后面创建 KeyValue 实例时会用到这些值
	_, created, ver, err := tw.s.kvindex.Get(key, rev)
	// 若为首次添加, 则会返回 not found 错误
	if err == nil {
		c = created.main
		// 先将 Key 值封装成 LeaseItem 实例, 然后通过 lessor.GetLease() 方法查找对应的 Lease 实例
		oldLease = tw.s.le.GetLease(lease.LeaseItem{Key: string(key)})
	}
	tw.trace.Step("get key's previous created_revision and leaseID")
	ibytes := newRevBytes()
	// 创建此次操作对应的 revision 实例, 其中 main revision 部分为 beginRev + 1
	idxRev := revision{main: rev, sub: int64(len(tw.changes))}
	// 将 main revision 和 sub revision 两部分写入 ibytes 中, 后续会作为写入 BoltDB 的 key
	revToBytes(idxRev, ibytes)

	// 递增在该 key 上执行过的修改操作次数
	ver = ver + 1
	// 创建 KeyValue 实例
	kv := mvccpb.KeyValue{
		Key:            key,   // 原始 key 值
		Value:          value, // 原始 value 值
		// 如果内存索引中已经存在该键值对, 则创建版本不变, 否则为新建 key, 则将 CreateRevision 设置为 beginRev+1
		CreateRevision: c,
		ModRevision:    rev,   // beginRev + 1
		Version:        ver,   // 递增 version
		Lease:          int64(leaseID),
	}

	// 将创建的 KeyValue 实例序列化
	d, err := kv.Marshal()
	if err != nil {
		if tw.storeTxnRead.s.lg != nil {
			tw.storeTxnRead.s.lg.Fatal(
				"failed to marshal mvccpb.KeyValue",
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot marshal event: %v", err)
		}
	}

	tw.trace.Step("marshal mvccpb.KeyValue")
	// 将 Key（main revision 和 sub revision 组成）和 Value（上述 KeyValue 实例序列化的结果）写入
	// BoltDB 中名为 "key" 的 Bucket.
	tw.tx.UnsafeSeqPut(keyBucketName, ibytes, d)
	// 将原始 key 与 revision 实例的对应关系写入内存索引中
	tw.s.kvindex.Put(key, idxRev)
	// 将上述 KeyValue 实例写入到 changes 中
	tw.changes = append(tw.changes, kv)
	tw.trace.Step("store kv pair into bolt db")

	if oldLease != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to detach lease")
		}
		// 将键值对与之前的 Lease 实例解绑
		err = tw.s.le.Detach(oldLease, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			if tw.storeTxnRead.s.lg != nil {
				tw.storeTxnRead.s.lg.Fatal(
					"failed to detach old lease from a key",
					zap.Error(err),
				)
			} else {
				plog.Errorf("unexpected error from lease detach: %v", err)
			}
		}
	}
	// 将键值对与新指定的 Lease 实例绑定
	if leaseID != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to attach lease")
		}
		// 调用 lessor.Attach() 方法进行绑定
		err = tw.s.le.Attach(leaseID, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			panic("unexpected error from lease Attach")
		}
	}
	tw.trace.Step("attach lease to kv pair")
}

// deleteRange 首先会从内存索引中查询待删除的 Key 和对应的 revision 实例,
// 然后会调用 storeTxnWrite.delete() 方法逐个删除.
func (tw *storeTxnWrite) deleteRange(key, end []byte) int64 {
	rrev := tw.beginRev
	// 根据当前事务是否有更新操作, 决定后续使用的 main revision 值
	if len(tw.changes) > 0 {
		rrev++
	}
	// 在内存索引中查询待删除的 key
	keys, _ := tw.s.kvindex.Range(key, end, rrev)
	if len(keys) == 0 {
		return 0
	}
	// 遍历待删除的 key, 并调用 delete() 方法完成删除
	for _, key := range keys {
		tw.delete(key)
	}
	// 返回删除的 key 数量
	return int64(len(keys))
}

// delete 会向 BoltDB 和内存索引中添加 tombstone 键值对
func (tw *storeTxnWrite) delete(key []byte) {
	// 将 revision 转换成 BoltDB 中的 key
	ibytes := newRevBytes()
	idxRev := revision{main: tw.beginRev + 1, sub: int64(len(tw.changes))}
	revToBytes(idxRev, ibytes)

	// 追加一个 't' 标识 Tombstone
	if tw.storeTxnRead.s != nil && tw.storeTxnRead.s.lg != nil {
		ibytes = appendMarkTombstone(tw.storeTxnRead.s.lg, ibytes)
	} else {
		// TODO: remove this in v3.5
		ibytes = appendMarkTombstone(nil, ibytes)
	}

	// 创建 KeyValue 实例, 其中只包含 key, 并进行序列化
	kv := mvccpb.KeyValue{Key: key}

	d, err := kv.Marshal()
	if err != nil {
		if tw.storeTxnRead.s.lg != nil {
			tw.storeTxnRead.s.lg.Fatal(
				"failed to marshal mvccpb.KeyValue",
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot marshal event: %v", err)
		}
	}

	// 将上面生成的 key（ibytes）和 value（KeyValue 序列化后的数据 d）写入名为 "key" 的 Bucket 中
	tw.tx.UnsafeSeqPut(keyBucketName, ibytes, d)
	// 在内存索引中添加 Tombstone
	err = tw.s.kvindex.Tombstone(key, idxRev)
	if err != nil {
		if tw.storeTxnRead.s.lg != nil {
			tw.storeTxnRead.s.lg.Fatal(
				"failed to tombstone an existing key",
				zap.String("key", string(key)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot tombstone an existing key (%s): %v", string(key), err)
		}
	}
	// 向 changes 中追加上述 KeyValue 操作
	tw.changes = append(tw.changes, kv)

	// 将 Key 值封装成 LeaseItem 实例
	item := lease.LeaseItem{Key: string(key)}
	// 然后根据 LeaseItem 实例查找对应 Lease 实例的 id
	leaseID := tw.s.le.GetLease(item)

	if leaseID != lease.NoLease {
		// 将键值对与旧的 Lease 实例解绑
		err = tw.s.le.Detach(leaseID, []lease.LeaseItem{item})
		if err != nil {
			if tw.storeTxnRead.s.lg != nil {
				tw.storeTxnRead.s.lg.Fatal(
					"failed to detach old lease from a key",
					zap.Error(err),
				)
			} else {
				plog.Errorf("cannot detach %v", err)
			}
		}
	}
}

func (tw *storeTxnWrite) Changes() []mvccpb.KeyValue { return tw.changes }
