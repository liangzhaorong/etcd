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
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/pkg/capnslog"
	humanize "github.com/dustin/go-humanize"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var (
	defaultBatchLimit    = 10000
	defaultBatchInterval = 100 * time.Millisecond

	defragLimit = 10000

	// initialMmapSize is the initial size of the mmapped region. Setting this larger than
	// the potential max db size can prevent writer from blocking reader.
	// This only works for linux.
	initialMmapSize = uint64(10 * 1024 * 1024 * 1024)

	plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "mvcc/backend")

	// minSnapshotWarningTimeout is the minimum threshold to trigger a long running snapshot warning.
	minSnapshotWarningTimeout = 30 * time.Second
)

// Backend 接口的主要功能是将底层存储与上层进行解耦, 其中定义底层存储需要对上层提供的接口.
type Backend interface {
	// ReadTx returns a read transaction. It is replaced by ConcurrentReadTx in the main data path, see #10523.
	//
	// 创建一个只读事务, 这里的 ReadTx 接口是 v3 存储对只读事务的抽象.
	ReadTx() ReadTx
	// 创建一个批量事务, 这里的 BatchTx 接口是对批量读写事务的抽象.
	BatchTx() BatchTx
	// ConcurrentReadTx returns a non-blocking read transaction.
	//
	// 创建一个不阻塞的并发读事务.
	// 并发读特性的核心原理是创建读事务对象时, 它会全量拷贝当前写事务未提交的 buffer 数据,
	// 并发的读写事务不再阻塞在一个 buffer 资源锁上, 实现了全并发读.
	ConcurrentReadTx() ReadTx

	// 创建快照
	Snapshot() Snapshot
	Hash(ignores map[IgnoreKey]struct{}) (uint32, error)
	// Size returns the current size of the backend physically allocated.
	// The backend can hold DB space that is not utilized at the moment,
	// since it can conduct pre-allocation or spare unused space for recycling.
	// Use SizeInUse() instead for the actual DB size.
	//
	// Size 返回 backend 物理分配的当前大小. backend 可以保留当前未使用的数据库空间,
	// 因为它可以进行预分配或保留未使用的空间以进行回收. 使用 SizeInUse() 代替实际的数据库大小.
	Size() int64
	// SizeInUse returns the current size of the backend logically in use.
	// Since the backend can manage free space in a non-byte unit such as
	// number of pages, the returned value can be not exactly accurate in bytes.
	//
	// SizeInUse 返回当前逻辑上正在使用的 backend 的当前大小.
	SizeInUse() int64
	// OpenReadTxN returns the number of currently open read transactions in the backend.
	OpenReadTxN() int64
	// 碎片整理
	Defrag() error
	// 提交批量读写事务
	ForceCommit()
	Close() error
}

type Snapshot interface {
	// Size gets the size of the snapshot.
	Size() int64
	// WriteTo writes the snapshot into the given writer.
	WriteTo(w io.Writer) (n int64, err error)
	// Close closes the snapshot.
	Close() error
}

// backend 是 v3 版本存储提供的 Backend 接口的默认实现.
type backend struct {
	// size and commits are used with atomic operations so they must be
	// 64-bit aligned, otherwise 32-bit tests will crash

	// size is the number of bytes allocated in the backend
	//
	// 当前 backend 实例已分配的总字节数
	size int64
	// sizeInUse is the number of bytes actually used in the backend
	//
	// 当前 backed 实例实际使用的总字节数
	sizeInUse int64
	// commits counts number of commits since start
	//
	// 从启动到目前为止, 已经提交的事务数
	commits int64
	// openReadTxN is the number of currently open read transactions in the backend
	openReadTxN int64

	mu sync.RWMutex
	// 底层的 BoltDB 存储
	db *bolt.DB

	// 两次批量读写事务提交的最大时间差. 默认值是 100ms.
	batchInterval time.Duration
	// 指定一次批量事务中最大的操作数, 当超过该阈值时, 当前的批量事务会自动提交. 默认值是 10000.
	batchLimit    int
	// 批量读写事务, batchTxBuffered 是在 batchTx 的基础上添加了缓存功能, 两者都实现了 BatchTx 接口.
	// 指向 batchTxBuffered 实例
	batchTx       *batchTxBuffered

	// 只读事务, readTx 实现了 ReadTx 接口.
	readTx *readTx

	stopc chan struct{}
	donec chan struct{}

	lg *zap.Logger
}

type BackendConfig struct {
	// Path is the file path to the backend file.
	//
	// BoltDB 数据库文件的路径, {节点名}.etcd/member/snap/db
	Path string
	// BatchInterval is the maximum time before flushing the BatchTx.
	//
	// 提交两次批量事务的最大时间差, 用来初始化 backend 实例中的 batchInterval 字段, 默认值是 100ms.
	BatchInterval time.Duration
	// BatchLimit is the maximum puts before flushing the BatchTx.
	//
	// 指定每个批量读写事务能包含的最多的操作个数, 当超过这个阈值后, 当前批量读写事务会自动提交, 该字段
	// 用来初始化 backend 实例中的 batchLimit 字段, 默认值是 10000.
	BatchLimit int
	// BackendFreelistType is the backend boltdb's freelist type.
	BackendFreelistType bolt.FreelistType
	// MmapSize is the number of bytes to mmap for the backend.
	//
	// BoltDB 使用了 mmap 技术对数据库文件进行映射, 该字段用来设置 mmap 中使用的内存大小, 该字段会在创建
	// BoltDB 实例时使用.
	MmapSize uint64
	// Logger logs backend-side operations.
	Logger *zap.Logger
	// UnsafeNoFsync disables all uses of fsync.
	UnsafeNoFsync bool `json:"unsafe-no-fsync"`
}

func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		BatchInterval: defaultBatchInterval,
		BatchLimit:    defaultBatchLimit,
		MmapSize:      initialMmapSize,
	}
}

// New 根据 BackendConfig 配置创建 Backend 实例
func New(bcfg BackendConfig) Backend {
	return newBackend(bcfg)
}

// NewDefaultBackend 使用默认配置创建一个 Backend 实例
func NewDefaultBackend(path string) Backend {
	bcfg := DefaultBackendConfig()
	bcfg.Path = path
	return newBackend(bcfg)
}

func newBackend(bcfg BackendConfig) *backend {
	// 创建 Options 实例, 该实例存储了用于初始化 BoltDB 时的参数
	bopts := &bolt.Options{}
	if boltOpenOptions != nil {
		*bopts = *boltOpenOptions
	}
	// mmap 使用的内存大小
	bopts.InitialMmapSize = bcfg.mmapSize()
	bopts.FreelistType = bcfg.BackendFreelistType
	bopts.NoSync = bcfg.UnsafeNoFsync
	bopts.NoGrowSync = bcfg.UnsafeNoFsync

	// 创建 bolt.DB 实例
	db, err := bolt.Open(bcfg.Path, 0600, bopts)
	if err != nil {
		if bcfg.Logger != nil {
			bcfg.Logger.Panic("failed to open database", zap.String("path", bcfg.Path), zap.Error(err))
		} else {
			plog.Panicf("cannot open database at %s (%v)", bcfg.Path, err)
		}
	}

	// In future, may want to make buffering optional for low-concurrency systems
	// or dynamically swap between buffered/non-buffered depending on workload.
	//
	// 创建 backend 实例, 并初始化其中各个字段
	b := &backend{
		db: db,

		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,

		// 创建 readTx 实例并初始化 backend.readTx 字段
		readTx: &readTx{
			buf: txReadBuffer{
				txBuffer: txBuffer{make(map[string]*bucketBuffer)},
			},
			buckets: make(map[string]*bolt.Bucket),
			txWg:    new(sync.WaitGroup),
		},

		stopc: make(chan struct{}),
		donec: make(chan struct{}),

		lg: bcfg.Logger,
	}
	// 创建 batchTxBuffered 实例并初始化 backend.batchTx 字段,
	// 注意, 该方法中同时为 b.readTx.tx 开启一个只读事务.
	b.batchTx = newBatchTxBuffered(b)
	// 启动一个单独的 goroutine, 其中会定时提交当前的批量读写事务, 并开启新的批量读写事务.
	go b.run()
	return b
}

// BatchTx returns the current batch tx in coalescer. The tx can be used for read and
// write operations. The write result can be retrieved within the same tx immediately.
// The write result is isolated with other txs until the current one get committed.
//
// BatchTx 返回 backend 中的读写事务 batchTx 实例
func (b *backend) BatchTx() BatchTx {
	return b.batchTx
}

// ReadTx 返回 backend 中的只读事务 readTx 实例
func (b *backend) ReadTx() ReadTx { return b.readTx }

// ConcurrentReadTx creates and returns a new ReadTx, which:
// A) creates and keeps a copy of backend.readTx.txReadBuffer,
// B) references the boltdb read Tx (and its bucket cache) of current batch interval.
//
// ConcurrentReadTx 创建一个并发读事务
func (b *backend) ConcurrentReadTx() ReadTx {
	b.readTx.RLock()
	defer b.readTx.RUnlock()
	// prevent boltdb read Tx from been rolled back until store read Tx is done. Needs to be called when holding readTx.RLock().
	b.readTx.txWg.Add(1)
	// TODO: might want to copy the read buffer lazily - create copy when A) end of a write transaction B) end of a batch interval.
	return &concurrentReadTx{
		buf:     b.readTx.buf.unsafeCopy(),
		tx:      b.readTx.tx,
		txMu:    &b.readTx.txMu,
		buckets: b.readTx.buckets,
		txWg:    b.readTx.txWg,
	}
}

// ForceCommit forces the current batching tx to commit.
//
// ForceCommit 提交当前的读写事务并立即开启新的读写事务
func (b *backend) ForceCommit() {
	b.batchTx.Commit()
}

// Snapshot 用当前的 BoltDB 中的数据创建相应的快照, 其中使用 Tx.WriteTo() 方法备份
// 整个 BoltDB 数据库的数据.
func (b *backend) Snapshot() Snapshot {
	// 提交当前的读写事务, 主要是为了提交缓冲区中的操作
	b.batchTx.Commit()

	// 加读锁
	b.mu.RLock()
	defer b.mu.RUnlock()
	// 开启一个只读事务
	tx, err := b.db.Begin(false)
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to begin tx", zap.Error(err))
		} else {
			plog.Fatalf("cannot begin tx (%s)", err)
		}
	}

	stopc, donec := make(chan struct{}), make(chan struct{})
	// 获取整个 BoltDB 中保存的数据大小
	dbBytes := tx.Size()
	// 启动一个单独的 goroutine, 用来检测快照数据是否已经发送完成
	go func() {
		defer close(donec)
		// sendRateBytes is based on transferring snapshot data over a 1 gigabit/s connection
		// assuming a min tcp throughput of 100MB/s.
		//
		// 这里假设发送快照的最小速度是 100MB/s
		var sendRateBytes int64 = 100 * 1024 * 1014
		// 创建定时器
		warningTimeout := time.Duration(int64((float64(dbBytes) / float64(sendRateBytes)) * float64(time.Second)))
		if warningTimeout < minSnapshotWarningTimeout {
			warningTimeout = minSnapshotWarningTimeout
		}
		start := time.Now()
		ticker := time.NewTicker(warningTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C: // 超时未发送完快照数据, 则会输出警告日志
				if b.lg != nil {
					b.lg.Warn(
						"snapshotting taking too long to transfer",
						zap.Duration("taking", time.Since(start)),
						zap.Int64("bytes", dbBytes),
						zap.String("size", humanize.Bytes(uint64(dbBytes))),
					)
				} else {
					plog.Warningf("snapshotting is taking more than %v seconds to finish transferring %v MB [started at %v]", time.Since(start).Seconds(), float64(dbBytes)/float64(1024*1014), start)
				}

			case <-stopc: // 发送快照数据结束
				snapshotTransferSec.Observe(time.Since(start).Seconds())
				return
			}
		}
	}()

	// 创建快照实例
	return &snapshot{tx, stopc, donec}
}

type IgnoreKey struct {
	Bucket string
	Key    string
}

func (b *backend) Hash(ignores map[IgnoreKey]struct{}) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	b.mu.RLock()
	defer b.mu.RUnlock()
	err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for next, _ := c.First(); next != nil; next, _ = c.Next() {
			b := tx.Bucket(next)
			if b == nil {
				return fmt.Errorf("cannot get hash of bucket %s", string(next))
			}
			h.Write(next)
			b.ForEach(func(k, v []byte) error {
				bk := IgnoreKey{Bucket: string(next), Key: string(k)}
				if _, ok := ignores[bk]; !ok {
					h.Write(k)
					h.Write(v)
				}
				return nil
			})
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

func (b *backend) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *backend) SizeInUse() int64 {
	return atomic.LoadInt64(&b.sizeInUse)
}

// run 会按照 batchInterval 指定的时间间隔, 定时提交批量读写数据, 在提交之后会立即
// 开启一个新的批量读写事务.
func (b *backend) run() {
	defer close(b.donec)
	// 创建定时器
	t := time.NewTimer(b.batchInterval)
	defer t.Stop()
	for {
		// 阻塞等待定时器到期
		select {
		case <-t.C:
		case <-b.stopc:
			// 提交读写事务, 并不再创建新的读写事务
			b.batchTx.CommitAndStop()
			return
		}
		// 获取当前读写事务中执行的修改操作个数
		if b.batchTx.safePending() != 0 {
			// 提交当前的批量读写事务, 并开启一个新的批量读写事务
			b.batchTx.Commit()
		}
		// 重置定时器
		t.Reset(b.batchInterval)
	}
}

func (b *backend) Close() error {
	close(b.stopc)
	<-b.donec
	return b.db.Close()
}

// Commits returns total number of commits since start
func (b *backend) Commits() int64 {
	return atomic.LoadInt64(&b.commits)
}

// Defrag 主要功能就是整理当前 BoltDB 实例中的碎片, 其实就是提高其中 Bucket 的填充率.
// 整理碎片实际上就是创建新的 BoltDB 数据库文件并将旧数据库文件的数据写入新数据库文件中.
// 因为在写入新数据库文件时是顺序写入的, 所以会提高填充比例（FillPercent）, 从而达到整理
// 碎片的目的.
// 注意, 在整理碎片的过程中, 需要持有 readTx、batchTx 和 backend 的锁.
func (b *backend) Defrag() error {
	return b.defrag()
}

func (b *backend) defrag() error {
	now := time.Now()

	// TODO: make this non-blocking?
	// lock batchTx to ensure nobody is using previous tx, and then
	// close previous ongoing tx.
	//
	// 加锁, 这里会获取 batchTx、backend 和 readTx 三把锁
	b.batchTx.Lock()
	defer b.batchTx.Unlock()

	// lock database after lock tx to avoid deadlock.
	b.mu.Lock()
	defer b.mu.Unlock()

	// block concurrent read requests while resetting tx
	b.readTx.Lock()
	defer b.readTx.Unlock()

	// 提交当前的批量读写事务, 注意参数, 此次提交后不会立即打开新的批量读写事务.
	b.batchTx.unsafeCommit(true)

	b.batchTx.tx = nil

	// Create a temporary file to ensure we start with a clean slate.
	// Snapshotter.cleanupSnapdir cleans up any of these that are found during startup.
	dir := filepath.Dir(b.db.Path())
	temp, err := ioutil.TempFile(dir, "db.tmp.*")
	if err != nil {
		return err
	}
	options := bolt.Options{}
	if boltOpenOptions != nil {
		options = *boltOpenOptions
	}
	options.OpenFile = func(path string, i int, mode os.FileMode) (file *os.File, err error) {
		return temp, nil
	}
	// 获取新数据库文件的路径
	tdbp := temp.Name()
	// 创建新的 bolt.DB 实例, 对应的数据库文件是个临时文件
	tmpdb, err := bolt.Open(tdbp, 0600, &options)
	if err != nil {
		return err
	}

	dbp := b.db.Path() // 获取旧数据库文件的路径
	size1, sizeInUse1 := b.Size(), b.SizeInUse()
	if b.lg != nil {
		b.lg.Info(
			"defragmenting",
			zap.String("path", dbp),
			zap.Int64("current-db-size-bytes", size1),
			zap.String("current-db-size", humanize.Bytes(uint64(size1))),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse1),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse1))),
		)
	}
	// gofail: var defragBeforeCopy struct{}
	//
	// 进行碎片整理, 其底层是创建一个新的 BoltDB 数据库文件并将当前数据库中的全部数据写入到其中,
	// 在写入过程中, 会将新 Bucket 的填充比例设置成 90%, 从而达到碎片整理的效果.
	err = defragdb(b.db, tmpdb, defragLimit)
	if err != nil {
		tmpdb.Close()
		if rmErr := os.RemoveAll(tmpdb.Path()); rmErr != nil {
			if b.lg != nil {
				b.lg.Error("failed to remove db.tmp after defragmentation completed", zap.Error(rmErr))
			} else {
				plog.Fatalf("failed to remove db.tmp after defragmentation completed: %v", rmErr)
			}
		}
		return err
	}

	err = b.db.Close() // 关闭旧的 bolt.DB 实例
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to close database", zap.Error(err))
		} else {
			plog.Fatalf("cannot close database (%s)", err)
		}
	}
	err = tmpdb.Close() // 关闭新的 bolt.DB 实例
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to close tmp database", zap.Error(err))
		} else {
			plog.Fatalf("cannot close database (%s)", err)
		}
	}
	// gofail: var defragBeforeRename struct{}
	err = os.Rename(tdbp, dbp) // 重命名新数据库文件, 覆盖旧数据库文件
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to rename tmp database", zap.Error(err))
		} else {
			plog.Fatalf("cannot rename database (%s)", err)
		}
	}

	// 重新创建 bolt.DB 实例, 此时使用的数据库文件是整理之后的新数据库文件
	b.db, err = bolt.Open(dbp, 0600, boltOpenOptions)
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to open database", zap.String("path", dbp), zap.Error(err))
		} else {
			plog.Panicf("cannot open database at %s (%v)", dbp, err)
		}
	}
	// 开启新的批量读写事务
	b.batchTx.tx = b.unsafeBegin(true)

	b.readTx.reset()
	// 开启新的只读事务
	b.readTx.tx = b.unsafeBegin(false)

	size := b.readTx.tx.Size()
	db := b.readTx.tx.DB()
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(db.Stats().FreePageN)*int64(db.Info().PageSize)))

	took := time.Since(now)
	defragSec.Observe(took.Seconds())

	size2, sizeInUse2 := b.Size(), b.SizeInUse()
	if b.lg != nil {
		b.lg.Info(
			"defragmented",
			zap.String("path", dbp),
			zap.Int64("current-db-size-bytes-diff", size2-size1),
			zap.Int64("current-db-size-bytes", size2),
			zap.String("current-db-size", humanize.Bytes(uint64(size2))),
			zap.Int64("current-db-size-in-use-bytes-diff", sizeInUse2-sizeInUse1),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse2),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse2))),
			zap.Duration("took", took),
		)
	}
	return nil
}

// defragdb 主要完成了旧数据库文件向新数据库文件的复制键值对的功能.
func defragdb(odb, tmpdb *bolt.DB, limit int) error {
	// open a tx on tmpdb for writes
	// 在新数据库实例上开启一个读写事务
	tmptx, err := tmpdb.Begin(true)
	if err != nil {
		return err
	}

	// open a tx on old db for read
	// 在旧数据库实例上开启一个只读事务
	tx, err := odb.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback() // 方法结束时关闭该只读事务

	// 获取旧实例上的 Cursor, 用于遍历其中的所有 Bucket
	c := tx.Cursor()

	count := 0
	// 读取旧数据库实例中的所有 Bucket, 并在新数据库实例上创建对应 Bucket
	for next, _ := c.First(); next != nil; next, _ = c.Next() {
		// 获取指定的 Bucket 实例
		b := tx.Bucket(next)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %s", string(next))
		}

		// 在新数据库实例中创建指定的 Bucket（不存在的话）
		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		if berr != nil {
			return berr
		}
		// 为提高利用率, 将填充比例设置成 90%, 因为下面会从读取旧 Bucket 中全部的键值对,
		// 并填充到新 Bucket 中, 这个过程是顺序写入的.
		tmpb.FillPercent = 0.9 // for seq write in for each

		// 遍历旧 Bucket 中的全部键值对
		b.ForEach(func(k, v []byte) error {
			count++
			// 当读取的键值对数量超过阈值, 则提交当前读写事务（新数据库）
			if count > limit {
				err = tmptx.Commit()
				if err != nil {
					return err
				}
				// 重新开启一个读写事务, 继续后面的写入操作
				tmptx, err = tmpdb.Begin(true)
				if err != nil {
					return err
				}
				tmpb = tmptx.Bucket(next)
				// 设置填充比例
				tmpb.FillPercent = 0.9 // for seq write in for each

				// 重新计数
				count = 0
			}
			// 将读取到的键值对写入新数据库文件中
			return tmpb.Put(k, v)
		})
	}

	// 最后提交读写事务（新数据库）
	return tmptx.Commit()
}

// begin 根据 write 参数确定开启只读事务还是读写事务, 并更新 backend 中的相关字段.
func (b *backend) begin(write bool) *bolt.Tx {
	b.mu.RLock() // 对 BoltDB 进行操作时需要加读锁
	// 根据 write 参数决定开启只读事务还是读写事务
	tx := b.unsafeBegin(write)
	b.mu.RUnlock()

	// 更新 backend 中的相关字段
	size := tx.Size()
	db := tx.DB()
	stats := db.Stats()
	atomic.StoreInt64(&b.size, size)
	atomic.StoreInt64(&b.sizeInUse, size-(int64(stats.FreePageN)*int64(db.Info().PageSize)))
	atomic.StoreInt64(&b.openReadTxN, int64(stats.OpenTxN))

	return tx
}

func (b *backend) unsafeBegin(write bool) *bolt.Tx {
	// 调用 BoltDB 的 API 开启事务
	tx, err := b.db.Begin(write)
	if err != nil {
		if b.lg != nil {
			b.lg.Fatal("failed to begin tx", zap.Error(err))
		} else {
			plog.Fatalf("cannot begin tx (%s)", err)
		}
	}
	return tx
}

func (b *backend) OpenReadTxN() int64 {
	return atomic.LoadInt64(&b.openReadTxN)
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(batchInterval time.Duration, batchLimit int) (*backend, string) {
	dir, err := ioutil.TempDir(os.TempDir(), "etcd_backend_test")
	if err != nil {
		panic(err)
	}
	tmpPath := filepath.Join(dir, "database")
	bcfg := DefaultBackendConfig()
	bcfg.Path, bcfg.BatchInterval, bcfg.BatchLimit = tmpPath, batchInterval, batchLimit
	return newBackend(bcfg), tmpPath
}

func NewDefaultTmpBackend() (*backend, string) {
	return NewTmpBackend(defaultBatchInterval, defaultBatchLimit)
}

// snapshot 结构体实现了 backend.Snapshot 接口, 该接口中最重要的接口就是 WriteTo() 方法.
// snapshot 中内嵌了 bolt.Tx, backend.snapshot.WriteTo() 方法就是通过 Tx.WriteTo() 方法实现的.
type snapshot struct {
	*bolt.Tx
	stopc chan struct{}
	donec chan struct{}
}

func (s *snapshot) Close() error {
	close(s.stopc)
	<-s.donec
	return s.Tx.Rollback()
}
