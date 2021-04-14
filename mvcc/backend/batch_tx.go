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

package backend

import (
	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// BatchTx 接口是 etcd 对批量读写事务的抽象.
type BatchTx interface {
	// 内嵌了 ReadTx 接口
	ReadTx
	// 创建 Bucket
	UnsafeCreateBucket(name []byte)
	// 向指定 Bucket 中添加键值对
	UnsafePut(bucketName []byte, key []byte, value []byte)
	// 向指定 Bucket 中添加键值对, 与 UnsafePut() 方法的区别是, 其中会将对应 Bucket 实例的
	// 填充比例设置为 90%, 这样可以在顺序写入时, 提高 Bucket 的利用率.
	UnsafeSeqPut(bucketName []byte, key []byte, value []byte)
	// 在指定 Bucket 中删除键值对
	UnsafeDelete(bucketName []byte, key []byte)
	// Commit commits a previous tx and begins a new writable one.
	//
	// 提交当前的读写事务, 之后立即打开一个新的读写事务.
	Commit()
	// CommitAndStop commits the previous tx and does not create a new one.
	//
	// 提交当前的读写事务, 之后并不会再打开新的读写事务.
	CommitAndStop()
}

// batchTx 结构体是批量读写事务 BatchTx 接口的实现之一.
type batchTx struct {
	// 内嵌了 sync.Mutex
	sync.Mutex
	// 该 batchTx 实例底层封装的 bolt.Tx 实例, 即 BoltDB 层面的读写事务.
	tx      *bolt.Tx
	// 该 batchTx 实例关联的 backend 实例
	backend *backend

	// 当前事务中执行的修改操作个数, 在当前读写事务提交时, 该值会被重置为 0.
	pending int
}

func (t *batchTx) Lock() {
	t.Mutex.Lock()
}

// Unlock 重写了内嵌的 sync.Mutex 的 Unlock() 方法
func (t *batchTx) Unlock() {
	// 检测当前事务的修改操作数量是否达到上限
	if t.pending >= t.backend.batchLimit {
		// 提交当前读写事务, 并开启新的读写事务
		t.commit(false)
	}
	// 释放锁
	t.Mutex.Unlock()
}

// BatchTx interface embeds ReadTx interface. But RLock() and RUnlock() do not
// have appropriate semantics in BatchTx interface. Therefore should not be called.
// TODO: might want to decouple ReadTx and BatchTx

func (t *batchTx) RLock() {
	panic("unexpected RLock")
}

func (t *batchTx) RUnlock() {
	panic("unexpected RUnlock")
}

// UnsafeCreateBucket 调用底层的 BoltDB 提供的 API 创建 Bucket 实例.
func (t *batchTx) UnsafeCreateBucket(name []byte) {
	// 创建一个 Bucket 实例
	_, err := t.tx.CreateBucket(name)
	if err != nil && err != bolt.ErrBucketExists {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to create a bucket",
				zap.String("bucket-name", string(name)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot create bucket %s (%v)", name, err)
		}
	}
	// 递增 pending
	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
func (t *batchTx) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, false)
}

// UnsafeSeqPut must be called holding the lock on the tx.
func (t *batchTx) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, true)
}

func (t *batchTx) unsafePut(bucketName []byte, key []byte, value []byte, seq bool) {
	// 通过 BoltDB 提供的 API 获取指定的 Bucket 实例
	bucket := t.tx.Bucket(bucketName)
	// 若 Bucket 不存在, 则报错并终止程序
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}
	// 如果是顺序写入, 则将填充率设置成 90%
	if seq {
		// it is useful to increase fill percent when the workloads are mostly append-only.
		// this can delay the page split and reduce space usage.
		bucket.FillPercent = 0.9
	}
	// 调用 BoltDB 提供的 API 写入键值对
	if err := bucket.Put(key, value); err != nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to write to a bucket",
				zap.String("bucket-name", string(bucketName)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot put key into bucket (%v)", err)
		}
	}
	// 递增 pending
	t.pending++
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}
	return unsafeRange(bucket.Cursor(), key, endKey, limit)
}

func unsafeRange(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
		// 如果没有指定 endKey, 则直接一个 key 对应的键值对
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

	// 定位到 key 并从 key 的位置开始查询
	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {
		vs = append(vs, cv)     // 记录符合条件的 value 值
		keys = append(keys, ck) // 记录符合条件的 key 值
		// 获取的数据达到 limit 的上限, 则停止查询
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTx) UnsafeDelete(bucketName []byte, key []byte) {
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}
	err := bucket.Delete(key)
	if err != nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to delete a key",
				zap.String("bucket-name", string(bucketName)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot delete key from bucket (%v)", err)
		}
	}
	t.pending++
}

// UnsafeForEach must be called holding the lock on the tx.
func (t *batchTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	return unsafeForEach(t.tx, bucketName, visitor)
}

// unsafeForEach 遍历 boltDB 中指定 Bucket 的所有键值对
func unsafeForEach(tx *bolt.Tx, bucket []byte, visitor func(k, v []byte) error) error {
	if b := tx.Bucket(bucket); b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTx) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTx) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

// safePending 获取当前读写事务中执行的修改操作个数
func (t *batchTx) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

// commit 提交读写事务, 并根据 stop 决定是否开启新的读写事务.
func (t *batchTx) commit(stop bool) {
	// commit the last tx
	if t.tx != nil {
		// 当前读写事务未进行任何修改操作, 则无须开启新事务
		if t.pending == 0 && !stop {
			return
		}

		start := time.Now()

		// gofail: var beforeCommit struct{}
		err := t.tx.Commit() // 通过 BoltDB 提供的 API 提交当前读写事务
		// gofail: var afterCommit struct{}

		rebalanceSec.Observe(t.tx.Stats().RebalanceTime.Seconds())
		spillSec.Observe(t.tx.Stats().SpillTime.Seconds())
		writeSec.Observe(t.tx.Stats().WriteTime.Seconds())
		commitSec.Observe(time.Since(start).Seconds())
		atomic.AddInt64(&t.backend.commits, 1)

		// 重置 pending
		t.pending = 0
		if err != nil {
			if t.backend.lg != nil {
				t.backend.lg.Fatal("failed to commit tx", zap.Error(err))
			} else {
				plog.Fatalf("cannot commit tx (%s)", err)
			}
		}
	}
	// 根据 stop 参数决定是否开启读写事务
	if !stop {
		// 开启读写事务
		t.tx = t.backend.begin(true)
	}
}

// batchTxBuffered 结构体内嵌了 batchTx 结构体, 在 batchTx 的基础上扩展了缓存功能.
type batchTxBuffered struct {
	// 内嵌 batchTx, 注意, 在 batchTx 结构体中内嵌了 sync.Mutex, 并且在 batchTx 和 batchTxBuffered
	// 中都重写其 Unlock() 方法.
	batchTx
	buf txWriteBuffer // 读写事务的缓存
}

// newBatchTxBuffered 创建 batchTxBuffered 实例, 该实例是在 batchTx 的基础上扩展了缓存功能.
func newBatchTxBuffered(backend *backend) *batchTxBuffered {
	tx := &batchTxBuffered{
		// 创建内嵌的 batchTx 实例
		batchTx: batchTx{backend: backend},
		// 创建 txWriteBuffer 缓冲区
		buf: txWriteBuffer{
			txBuffer: txBuffer{make(map[string]*bucketBuffer)},
			seq:      true, // 顺序写入
		},
	}
	// 开启事务, 为 t.backend.readTx.tx 开启只读事务; 为 t.batchTx.tx 开启读写事务
	tx.Commit()
	return tx
}

func (t *batchTxBuffered) Unlock() {
	// 检测当前读写事务中是否发生了修改操作
	if t.pending != 0 {
		// 获取只读事务的互斥锁
		t.backend.readTx.Lock() // blocks txReadBuffer for writing.
		// 更新 readTx 的缓存, 即将 batchTxBuffered 中缓存的数据回写到 readTx 的缓存中
		t.buf.writeback(&t.backend.readTx.buf)
		t.backend.readTx.Unlock() // 释放互斥锁
		// 若当前读写事务的修改操作数量达到上限, 则提交当前读写事务, 并开启新的读写事务
		if t.pending >= t.backend.batchLimit {
			t.commit(false)
		}
	}
	// 调用 batchTx.Unlock() 方法完成解锁.
	t.batchTx.Unlock()
}

// Commit 开启事务.
// 为 t.backend.readTx.tx 开启只读事务; 为 t.batchTx.tx 开启读写事务
func (t *batchTxBuffered) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

func (t *batchTxBuffered) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

// commit 提交事务, stop 参数决定是否开始新的读写事务.
func (t *batchTxBuffered) commit(stop bool) {
	// all read txs must be closed to acquire boltdb commit rwlock
	t.backend.readTx.Lock()
	t.unsafeCommit(stop)
	t.backend.readTx.Unlock()
}

func (t *batchTxBuffered) unsafeCommit(stop bool) {
	// 如果当前已经开启了只读事务, 则将该事务回滚（BoltDB 中的只读事务只能回滚, 无法提交）
	if t.backend.readTx.tx != nil {
		// wait all store read transactions using the current boltdb tx to finish,
		// then close the boltdb tx
		//
		// 启动一个单独的 goroutine 来回滚只读事务
		go func(tx *bolt.Tx, wg *sync.WaitGroup) {
			// 等待当前只读事务中所有操作都执行完毕
			wg.Wait()
			// 回滚只读事务
			if err := tx.Rollback(); err != nil {
				// 回滚失败则打印并终止程序
				if t.backend.lg != nil {
					t.backend.lg.Fatal("failed to rollback tx", zap.Error(err))
				} else {
					plog.Fatalf("cannot rollback tx (%s)", err)
				}
			}
		}(t.backend.readTx.tx, t.backend.readTx.txWg)
		// 重置 readTx
		t.backend.readTx.reset()
	}

	// 如果当前开始了读写事务, 则将该事务提交, 并根据 stop 确定是否创建新的读写事务
	t.batchTx.commit(stop)

	// 根据 stop 参数, 决定是否开启新的只读事务
	if !stop {
		t.backend.readTx.tx = t.backend.begin(false)
	}
}

// UnsafePut 将 key-value 键值对写入到 BoltDB 指定 bucketName 中
func (t *batchTxBuffered) UnsafePut(bucketName []byte, key []byte, value []byte) {
	// 调用读写事务接口方法 batchTx.UnsafePut() 将键值对写入 BoltDB 中
	t.batchTx.UnsafePut(bucketName, key, value)
	// 将键值对写入 txWriteBuffer 缓存中
	t.buf.put(bucketName, key, value)
}

func (t *batchTxBuffered) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.batchTx.UnsafeSeqPut(bucketName, key, value)
	t.buf.putSeq(bucketName, key, value)
}
