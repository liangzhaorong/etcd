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

package backend

import (
	"bytes"
	"math"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// safeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
// overwrites on a bucket should only fetch with limit=1, but safeRangeBucket
// is known to never overwrite any key so range is safe.
var safeRangeBucket = []byte("key")

// ReadTx 接口 etcd 对只读事务的抽象.
type ReadTx interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()

	// 在指定的 Bucket 中进行范围查找.
	UnsafeRange(bucketName []byte, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)
	// 遍历指定 Bucket 中的全部键值对.
	UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error
}

// readTx 结构体实现了只读事务 ReadTx 接口
type readTx struct {
	// mu protects accesses to the txReadBuffer
	// 在读写 buf 中的缓存区数据时, 需要获取该锁进行同步
	mu  sync.RWMutex
	// 该 buffer 主要用来缓存 Bucket 与其中键值对集合的映射关系.
	buf txReadBuffer

	// TODO: group and encapsulate {txMu, tx, buckets, txWg}, as they share the same lifecycle.
	// txMu protects accesses to buckets and tx on Range requests.
	//
	// 在进行查询之前, 需要获取该锁进行同步
	txMu    sync.RWMutex
	// 该 readTx 实例底层封装的 bolt.Tx 实例, 即 BoltDB 层面的只读事务.
	tx      *bolt.Tx
	// 缓存 Bucket 名称与 Bucket 实例的映射
	buckets map[string]*bolt.Bucket
	// txWg protects tx from being rolled back at the end of a batch interval until all reads using this tx are done.
	txWg *sync.WaitGroup
}

func (rt *readTx) Lock()    { rt.mu.Lock() }
func (rt *readTx) Unlock()  { rt.mu.Unlock() }
func (rt *readTx) RLock()   { rt.mu.RLock() }
func (rt *readTx) RUnlock() { rt.mu.RUnlock() }

// UnsafeRange 在指定的 Bucket 中进行范围查找. 在该方法的实现中, 只能对 safeRangeBucket（即名称为 "key" 的 Bucket,
// 该 Bucket 中的 key 即为 revision, value 为键值对）进行真正的范围查找, 对其他 Bucket 的查询只能返回单个键值对.
// 参数:
// - key: 即为 revision
func (rt *readTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	// 对非法的 limit 进行重新设置
	if endKey == nil {
		// forbid duplicates for single keys
		// 没有指定 endKey 则只返回一个
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	// 只有查询 safeRangeBucket（即名称为 "key" 的 Bucket）时, 才是真正的范围查询, 否则只能返回一个键值对.
	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {
		panic("do not use unsafeRange on non-keys bucket")
	}
	// 首先从当前只读事务的缓存中查询键值对
	keys, vals := rt.buf.Range(bucketName, key, endKey, limit)
	// 检测缓存返回的键值对数量是否达到 limit 的限制, 如果达到 limit 指定的上限, 则直接返回缓存的查询结果.
	if int64(len(keys)) == limit {
		return keys, vals
	}

	// find/cache bucket
	bn := string(bucketName)
	rt.txMu.RLock() // 获取 txMu 锁
	// 先尝试从缓存中获取该指定 Bucket
	bucket, ok := rt.buckets[bn]
	rt.txMu.RUnlock() // 解锁
	// 若缓存中不存在该指定 Bucket
	if !ok {
		rt.txMu.Lock()
		// 则从 readTx 底层封装的 Tx 只读事务中获取指定 Bucket
		bucket = rt.tx.Bucket(bucketName)
		// 将 Bucket 实例缓存到 readTx.buckets 这个 map 中
		rt.buckets[bn] = bucket
		rt.txMu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
		return keys, vals
	}
	rt.txMu.Lock()
	c := bucket.Cursor()
	rt.txMu.Unlock()

	// 通过 unsafeRange 函数从 BoltDB 中查询
	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))
	// 将查询缓存的结果与查询 BoltDB 的结果合并, 然后返回
	return append(k2, keys...), append(v2, vals...)
}

// UnsafeForEach 遍历指定 Bucket 的缓存和 Bucket 中的全部键值对, 并通过 visitor 回调函数处理这些遍历到的键值对.
func (rt *readTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	// 获取 bucketBuffer 中的所有 key, 保存到 dups 中
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	// 调用 bucketBuffer.ForEach() 方法, 获取缓存中指定 bucketBuffer 中所有的 key.
	if err := rt.buf.ForEach(bucketName, getDups); err != nil {
		return err
	}
	rt.txMu.Lock() // 调用 readTx.tx 字段前, 需要获取 txMu 锁
	// 遍历 boltDB 中指定 bucket 的所有键值对, 对于不存在 readTx.buf 缓存中的 key 调用传入的 visitor 回调函数.
	err := unsafeForEach(rt.tx, bucketName, visitNoDup)
	rt.txMu.Unlock()
	if err != nil {
		return err
	}
	// 再次遍历缓存中指定 bucketBuffer 中的所有键值对, 对每个键调用传入的 visitor 回调函数
	return rt.buf.ForEach(bucketName, visitor)
}

func (rt *readTx) reset() {
	// 清空 readTx 中的缓存
	rt.buf.reset()
	rt.buckets = make(map[string]*bolt.Bucket)
	rt.tx = nil
	rt.txWg = new(sync.WaitGroup)
}

// TODO: create a base type for readTx and concurrentReadTx to avoid duplicated function implementation?
//
// concurrentReadTx 实现了并发读特性.
// 并发读特性的核心原理是创建读事务对象时，它会全量拷贝当前写事务未提交的 buffer 数据，
// 并发的读写事务不再阻塞在一个 buffer 资源锁上，实现了全并发读。
type concurrentReadTx struct {
	buf     txReadBuffer
	txMu    *sync.RWMutex
	tx      *bolt.Tx
	buckets map[string]*bolt.Bucket
	txWg    *sync.WaitGroup
}

func (rt *concurrentReadTx) Lock()   {}
func (rt *concurrentReadTx) Unlock() {}

// RLock is no-op. concurrentReadTx does not need to be locked after it is created.
func (rt *concurrentReadTx) RLock() {}

// RUnlock signals the end of concurrentReadTx.
func (rt *concurrentReadTx) RUnlock() { rt.txWg.Done() }

func (rt *concurrentReadTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	if err := rt.buf.ForEach(bucketName, getDups); err != nil {
		return err
	}
	rt.txMu.Lock()
	err := unsafeForEach(rt.tx, bucketName, visitNoDup)
	rt.txMu.Unlock()
	if err != nil {
		return err
	}
	return rt.buf.ForEach(bucketName, visitor)
}

func (rt *concurrentReadTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {
		panic("do not use unsafeRange on non-keys bucket")
	}
	keys, vals := rt.buf.Range(bucketName, key, endKey, limit)
	if int64(len(keys)) == limit {
		return keys, vals
	}

	// find/cache bucket
	bn := string(bucketName)
	rt.txMu.RLock()
	bucket, ok := rt.buckets[bn]
	rt.txMu.RUnlock()
	if !ok {
		rt.txMu.Lock()
		bucket = rt.tx.Bucket(bucketName)
		rt.buckets[bn] = bucket
		rt.txMu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
		return keys, vals
	}
	rt.txMu.Lock()
	c := bucket.Cursor()
	rt.txMu.Unlock()

	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))
	return append(k2, keys...), append(v2, vals...)
}
