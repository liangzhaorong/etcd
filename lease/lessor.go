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

package lease

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/lease/leasepb"
	"go.etcd.io/etcd/mvcc/backend"
	"go.uber.org/zap"
)

// NoLease is a special LeaseID representing the absence of a lease.
const NoLease = LeaseID(0)

// MaxLeaseTTL is the maximum lease TTL value
const MaxLeaseTTL = 9000000000

var (
	forever = time.Time{}

	leaseBucketName = []byte("lease")

	// maximum number of leases to revoke per second; configurable for tests
	leaseRevokeRate = 1000

	// maximum number of lease checkpoints recorded to the consensus log per second; configurable for tests
	leaseCheckpointRate = 1000

	// the default interval of lease checkpoint
	defaultLeaseCheckpointInterval = 5 * time.Minute

	// maximum number of lease checkpoints to batch into a single consensus log entry
	maxLeaseCheckpointBatchSize = 1000

	// the default interval to check if the expired lease is revoked
	defaultExpiredleaseRetryInterval = 3 * time.Second

	ErrNotPrimary       = errors.New("not a primary lessor")
	ErrLeaseNotFound    = errors.New("lease not found")
	ErrLeaseExists      = errors.New("lease already exists")
	ErrLeaseTTLTooLarge = errors.New("too large lease TTL")
)

// TxnDelete is a TxnWrite that only permits deletes. Defined here
// to avoid circular dependency with mvcc.
type TxnDelete interface {
	DeleteRange(key, end []byte) (n, rev int64)
	End()
}

// RangeDeleter is a TxnDelete constructor.
type RangeDeleter func() TxnDelete

// Checkpointer permits checkpointing of lease remaining TTLs to the consensus log. Defined here to
// avoid circular dependency with mvcc.
type Checkpointer func(ctx context.Context, lc *pb.LeaseCheckpointRequest)

type LeaseID int64

// Lessor owns leases. It can grant, revoke, renew and modify leases for lessee.
type Lessor interface {
	// SetRangeDeleter lets the lessor create TxnDeletes to the store.
	// Lessor deletes the items in the revoked or expired lease by creating
	// new TxnDeletes.
	SetRangeDeleter(rd RangeDeleter)

	SetCheckpointer(cp Checkpointer)

	// Grant grants a lease that expires at least after TTL seconds.
	//
	// 创建 Lease 实例, 该 Lease 会在指定的时间（ttl）之后过期
	Grant(id LeaseID, ttl int64) (*Lease, error)
	// Revoke revokes a lease with given ID. The item attached to the
	// given lease will be removed. If the ID does not exist, an error
	// will be returned.
	//
	// 撤销指定的 Lease, 该 Lease 实例相关的 LeaseItem 也会被删除
	Revoke(id LeaseID) error

	// Checkpoint applies the remainingTTL of a lease. The remainingTTL is used in Promote to set
	// the expiry of leases to less than the full TTL when possible.
	Checkpoint(id LeaseID, remainingTTL int64) error

	// Attach attaches given leaseItem to the lease with given LeaseID.
	// If the lease does not exist, an error will be returned.
	//
	// 将指定 Lease 与指定 LeaseItem 绑定, 在 LeaseItem 中封装了键值对的 Key 值
	Attach(id LeaseID, items []LeaseItem) error

	// GetLease returns LeaseID for given item.
	// If no lease found, NoLease value will be returned.
	//
	// 根据 LeaseItem 查询对应 Lease 实例的 id
	GetLease(item LeaseItem) LeaseID

	// Detach detaches given leaseItem from the lease with given LeaseID.
	// If the lease does not exist, an error will be returned.
	//
	// 取消指定 Lease 与指定 LeaseItem 之间的绑定关系
	Detach(id LeaseID, items []LeaseItem) error

	// Promote promotes the lessor to be the primary lessor. Primary lessor manages
	// the expiration and renew of leases.
	// Newly promoted lessor renew the TTL of all lease to extend + previous TTL.
	//
	// 如果当前节点成为 Leader 节点, 则其使用的 Lessor 实例将通过该方法晋升, 成为主 Lessor.
	// 主 Lessor 将控制整个集群中所有 Lease 实例.
	Promote(extend time.Duration)

	// Demote demotes the lessor from being the primary lessor.
	//
	// 如果当前节点从 Leader 状态转换为其他状态, 则会通过该方法将其使用的 Lessor 实例进行降级.
	Demote()

	// Renew renews a lease with given ID. It returns the renewed TTL. If the ID does not exist,
	// an error will be returned.
	//
	// 续约指定的 Lease 实例
	Renew(id LeaseID) (int64, error)

	// Lookup gives the lease at a given lease id, if any
	//
	// 查询指定 id 对应的 Lease 实例
	Lookup(id LeaseID) *Lease

	// Leases lists all leases.
	//
	// 获取所有的 Lease 实例
	Leases() []*Lease

	// ExpiredLeasesC returns a chan that is used to receive expired leases.
	//
	// 如果出现 Lease 过期, 则会被写入到 ExpiredLeasesC 通道中
	ExpiredLeasesC() <-chan []*Lease

	// Recover recovers the lessor state from the given backend and RangeDeleter.
	//
	// 从底层的 v3 存储中恢复 Lease 实例
	Recover(b backend.Backend, rd RangeDeleter)

	// Stop stops the lessor for managing leases. The behavior of calling Stop multiple
	// times is undefined.
	Stop()
}

// lessor implements Lessor interface.
// TODO: use clockwork for testability.
//
// lessor 实现 Lessor 接口
type lessor struct {
	mu sync.RWMutex

	// demotec is set when the lessor is the primary.
	// demotec will be closed if the lessor is demoted.
	//
	// 用于判断当前 lessor 实例是否为主 lessor（primary lessor）. 如果当前 lessor 是主 Lessor 实例,
	// 则会开启该通道, 当节点切换成非 Leader 状态时, 会关闭该通道.
	demotec chan struct{}

	// 记录 leaseId 到 Lease 实例之间的映射关系
	leaseMap             map[LeaseID]*Lease
	leaseExpiredNotifier *LeaseExpiredNotifier
	leaseCheckpointHeap  LeaseQueue
	// 记录 LeaseItem 到 Lease 实例 id 的映射
	itemMap              map[LeaseItem]LeaseID

	// When a lease expires, the lessor will delete the
	// leased range (or key) by the RangeDeleter.
	//
	// RangeDeleter 接口主要用于从底层的存储中删除过期的 lease 实例
	rd RangeDeleter

	// When a lease's deadline should be persisted to preserve the remaining TTL across leader
	// elections and restarts, the lessor will checkpoint the lease by the Checkpointer.
	cp Checkpointer

	// backend to persist leases. We only persist lease ID and expiry for now.
	// The leased items can be recovered by iterating all the keys in kv.
	//
	// 底层持久化 Lease 的存储
	b backend.Backend

	// minLeaseTTL is the minimum lease TTL that can be granted for a lease. Any
	// requests for shorter TTLs are extended to the minimum TTL.
	//
	// Lease 实例过期时间的最小值
	minLeaseTTL int64

	// 过期的 Lease 实例会被写入该通道中, 并等待其他 goroutine 进行处理
	expiredC chan []*Lease
	// stopC is a channel whose closure indicates that the lessor should be stopped.
	stopC chan struct{}
	// doneC is a channel whose closure indicates that the lessor is stopped.
	doneC chan struct{}

	lg *zap.Logger

	// Wait duration between lease checkpoints.
	checkpointInterval time.Duration
	// the interval to check if the expired lease is revoked
	expiredLeaseRetryInterval time.Duration
}

type LessorConfig struct {
	MinLeaseTTL                int64
	CheckpointInterval         time.Duration
	ExpiredLeasesRetryInterval time.Duration
}

func NewLessor(lg *zap.Logger, b backend.Backend, cfg LessorConfig) Lessor {
	return newLessor(lg, b, cfg)
}

// newLessor 创建 lessor 实例, 并调用 lessor.initAndRecover() 方法完成初始化操作, 同时还会启动一个后台 goroutine
// 查找当前 lessor 中是否存在过期的 lease 实例.
func newLessor(lg *zap.Logger, b backend.Backend, cfg LessorConfig) *lessor {
	checkpointInterval := cfg.CheckpointInterval
	expiredLeaseRetryInterval := cfg.ExpiredLeasesRetryInterval
	if checkpointInterval == 0 {
		checkpointInterval = defaultLeaseCheckpointInterval
	}
	if expiredLeaseRetryInterval == 0 {
		expiredLeaseRetryInterval = defaultExpiredleaseRetryInterval
	}
	l := &lessor{
		leaseMap:                  make(map[LeaseID]*Lease),
		itemMap:                   make(map[LeaseItem]LeaseID),
		leaseExpiredNotifier:      newLeaseExpiredNotifier(),
		leaseCheckpointHeap:       make(LeaseQueue, 0),
		b:                         b,
		minLeaseTTL:               cfg.MinLeaseTTL,
		checkpointInterval:        checkpointInterval,
		expiredLeaseRetryInterval: expiredLeaseRetryInterval,
		// expiredC is a small buffered chan to avoid unnecessary blocking.
		expiredC: make(chan []*Lease, 16),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
		lg:       lg,
	}
	l.initAndRecover()

	go l.runLoop()

	return l
}

// isPrimary indicates if this lessor is the primary lessor. The primary
// lessor manages lease expiration and renew.
//
// in etcd, raft leader is the primary. Thus there might be two primary
// leaders at the same time (raft allows concurrent leader but with different term)
// for at most a leader election timeout.
// The old primary leader cannot affect the correctness since its proposal has a
// smaller term and will not be committed.
//
// TODO: raft follower do not forward lease management proposals. There might be a
// very small window (within second normally which depends on go scheduling) that
// a raft follow is the primary between the raft leader demotion and lessor demotion.
// Usually this should not be a problem. Lease should not be that sensitive to timing.
func (le *lessor) isPrimary() bool {
	return le.demotec != nil
}

func (le *lessor) SetRangeDeleter(rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.rd = rd
}

func (le *lessor) SetCheckpointer(cp Checkpointer) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.cp = cp
}

// Grant 根据指定的 id 和过期时长新建 Lease 实例, 然后将其保存到 leaseMap 中并进行持久化.
func (le *lessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	if id == NoLease {
		return nil, ErrLeaseNotFound
	}

	// 过期时长超过最大过期时间
	if ttl > MaxLeaseTTL {
		return nil, ErrLeaseTTLTooLarge
	}

	// TODO: when lessor is under high load, it should give out lease
	// with longer TTL to reduce renew load.
	// 新建 Lease 实例
	l := &Lease{
		ID:      id,
		ttl:     ttl,
		itemSet: make(map[LeaseItem]struct{}),
		revokec: make(chan struct{}),
	}

	le.mu.Lock()
	defer le.mu.Unlock()

	// 检测指定的 id 在当前 leaseMap 中是否存在对应 Lease 实例
	if _, ok := le.leaseMap[id]; ok {
		return nil, ErrLeaseExists
	}

	// 修正过期时长
	if l.ttl < le.minLeaseTTL {
		l.ttl = le.minLeaseTTL
	}

	// 若为主 Lessor
	if le.isPrimary() {
		// 则更新 Lease 实例的过期时间戳
		l.refresh(0)
	} else {
		// 如果当前节点不是 Leader, 则将 Lease 设置为永不过期
		l.forever()
	}

	// 在 leaseMap 中记录该 Lease 实例
	le.leaseMap[id] = l
	// 持久化 Lease 信息
	l.persistTo(le.b)

	leaseTotalTTLs.Observe(float64(l.ttl))
	leaseGranted.Inc()

	if le.isPrimary() {
		item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}

	return l, nil
}

// Revoke 撤销指定 id 对应的 Lease 实例, 其中会关闭 Lease 实例对应的 revokec 通道,
// 并将 Lease 实例从 lessor.leaseMap 和存储中删除, 需要注意的是, 除了删除 Lease 实例
// 本身, 还需要删除与该 Lease 实例关联的键值对.
func (le *lessor) Revoke(id LeaseID) error {
	le.mu.Lock()

	// 从 leaseMap 中查找指定 id 的 Lease 实例
	l := le.leaseMap[id]
	if l == nil {
		le.mu.Unlock()
		return ErrLeaseNotFound
	}
	defer close(l.revokec) // 在方法结束时, 关闭该 Lease 实例中的 revokec 通道
	// unlock before doing external work
	le.mu.Unlock()

	// 若没有设置底层存储的删除接口, 则直接返回
	if le.rd == nil {
		return nil
	}

	txn := le.rd()

	// sort keys so deletes are in same order among all members,
	// otherwise the backened hashes will be different
	keys := l.Keys() // 获取与该 Lease 实例绑定的 Key 值
	sort.StringSlice(keys).Sort()
	// 删除与该 Lease 绑定的全部键值对
	for _, key := range keys {
		txn.DeleteRange([]byte(key), nil)
	}

	le.mu.Lock()
	defer le.mu.Unlock()
	delete(le.leaseMap, l.ID) // 将该 Lease 实例从 leaseMap 中删除
	// lease deletion needs to be in the same backend transaction with the
	// kv deletion. Or we might end up with not executing the revoke or not
	// deleting the keys if etcdserver fails in between.
	//
	// 将该 Lease 实例的信息从底层的 v3 存储中删除
	le.b.BatchTx().UnsafeDelete(leaseBucketName, int64ToBytes(int64(l.ID)))

	txn.End() // 提交事务

	leaseRevoked.Inc()
	return nil
}

func (le *lessor) Checkpoint(id LeaseID, remainingTTL int64) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if l, ok := le.leaseMap[id]; ok {
		// when checkpointing, we only update the remainingTTL, Promote is responsible for applying this to lease expiry
		l.remainingTTL = remainingTTL
		if le.isPrimary() {
			// schedule the next checkpoint as needed
			le.scheduleCheckpointIfNeeded(l)
		}
	}
	return nil
}

// Renew renews an existing lease. If the given lease does not exist or
// has expired, an error will be returned.
//
// Renew 续租一个已经存在的 Lease 实例, 注意, 只有当前节点是 Leader 时才能完成该续租操作.
func (le *lessor) Renew(id LeaseID) (int64, error) {
	le.mu.RLock()
	// 非 Leader 节点不可以执行续租操作
	if !le.isPrimary() {
		// forward renew request to primary instead of returning error.
		le.mu.RUnlock()
		return -1, ErrNotPrimary
	}

	demotec := le.demotec

	// 在 leaseMap 中查找指定的 Lease 实例
	l := le.leaseMap[id]
	if l == nil {
		le.mu.RUnlock()
		return -1, ErrLeaseNotFound
	}
	// Clear remaining TTL when we renew if it is set
	clearRemainingTTL := le.cp != nil && l.remainingTTL > 0

	le.mu.RUnlock()
	// 若 Lease 实例已过期, 则返回错误
	if l.expired() {
		select {
		// A expired lease might be pending for revoking or going through
		// quorum to be revoked. To be accurate, renew request must wait for the
		// deletion to complete.
		case <-l.revokec:
			return -1, ErrLeaseNotFound
		// The expired lease might fail to be revoked if the primary changes.
		// The caller will retry on ErrNotPrimary.
		case <-demotec:
			return -1, ErrNotPrimary
		case <-le.stopC:
			return -1, ErrNotPrimary
		}
	}

	// Clear remaining TTL when we renew if it is set
	// By applying a RAFT entry only when the remainingTTL is already set, we limit the number
	// of RAFT entries written per lease to a max of 2 per checkpoint interval.
	if clearRemainingTTL {
		le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: []*pb.LeaseCheckpoint{{ID: int64(l.ID), Remaining_TTL: 0}}})
	}

	le.mu.Lock()
	// 更新过期时间
	l.refresh(0)
	item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
	le.leaseExpiredNotifier.RegisterOrUpdate(item)
	le.mu.Unlock()

	leaseRenewed.Inc()
	return l.ttl, nil
}

func (le *lessor) Lookup(id LeaseID) *Lease {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaseMap[id]
}

func (le *lessor) unsafeLeases() []*Lease {
	leases := make([]*Lease, 0, len(le.leaseMap))
	for _, l := range le.leaseMap {
		leases = append(leases, l)
	}
	return leases
}

func (le *lessor) Leases() []*Lease {
	le.mu.RLock()
	ls := le.unsafeLeases()
	le.mu.RUnlock()
	sort.Sort(leasesByExpiry(ls))
	return ls
}

func (le *lessor) Promote(extend time.Duration) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.demotec = make(chan struct{})

	// refresh the expiries of all leases.
	for _, l := range le.leaseMap {
		l.refresh(extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
	}

	if len(le.leaseMap) < leaseRevokeRate {
		// no possibility of lease pile-up
		return
	}

	// adjust expiries in case of overlap
	leases := le.unsafeLeases()
	sort.Sort(leasesByExpiry(leases))

	baseWindow := leases[0].Remaining()
	nextWindow := baseWindow + time.Second
	expires := 0
	// have fewer expires than the total revoke rate so piled up leases
	// don't consume the entire revoke limit
	targetExpiresPerSecond := (3 * leaseRevokeRate) / 4
	for _, l := range leases {
		remaining := l.Remaining()
		if remaining > nextWindow {
			baseWindow = remaining
			nextWindow = baseWindow + time.Second
			expires = 1
			continue
		}
		expires++
		if expires <= targetExpiresPerSecond {
			continue
		}
		rateDelay := float64(time.Second) * (float64(expires) / float64(targetExpiresPerSecond))
		// If leases are extended by n seconds, leases n seconds ahead of the
		// base window should be extended by only one second.
		rateDelay -= float64(remaining - baseWindow)
		delay := time.Duration(rateDelay)
		nextWindow = baseWindow + delay
		l.refresh(delay + extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}
}

type leasesByExpiry []*Lease

func (le leasesByExpiry) Len() int           { return len(le) }
func (le leasesByExpiry) Less(i, j int) bool { return le[i].Remaining() < le[j].Remaining() }
func (le leasesByExpiry) Swap(i, j int)      { le[i], le[j] = le[j], le[i] }

func (le *lessor) Demote() {
	le.mu.Lock()
	defer le.mu.Unlock()

	// set the expiries of all leases to forever
	for _, l := range le.leaseMap {
		l.forever()
	}

	le.clearScheduledLeasesCheckpoints()
	le.clearLeaseExpiredNotifier()

	if le.demotec != nil {
		close(le.demotec)
		le.demotec = nil
	}
}

// Attach attaches items to the lease with given ID. When the lease
// expires, the attached items will be automatically removed.
// If the given lease does not exist, an error will be returned.
//
// Attach 将指定的 Lease 实例和键值对绑定
func (le *lessor) Attach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	// 获取指定 id 对应的 Lease 实例
	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		l.itemSet[it] = struct{}{} // 在 itemSet 这个 map 中记录该 LeaseItem 实例
		le.itemMap[it] = id        // 在 Lease 实例的 itemMap 字段中记录该 LeaseItem 实例, 从而实现绑定
	}
	l.mu.Unlock()
	return nil
}

func (le *lessor) GetLease(item LeaseItem) LeaseID {
	le.mu.RLock()
	id := le.itemMap[item]
	le.mu.RUnlock()
	return id
}

// Detach detaches items from the lease with given ID.
// If the given lease does not exist, an error will be returned.
//
// Detach 将指定的 Lease 实例和键值对解绑
func (le *lessor) Detach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		delete(l.itemSet, it)
		delete(le.itemMap, it)
	}
	l.mu.Unlock()
	return nil
}

func (le *lessor) Recover(b backend.Backend, rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.b = b
	le.rd = rd
	le.leaseMap = make(map[LeaseID]*Lease)
	le.itemMap = make(map[LeaseItem]LeaseID)
	le.initAndRecover()
}

func (le *lessor) ExpiredLeasesC() <-chan []*Lease {
	return le.expiredC
}

func (le *lessor) Stop() {
	close(le.stopC)
	<-le.doneC
}

// runLoop 根据当前 lessor 实例是否为主 lessor, 决定是否检测过期 Lease 实例.
func (le *lessor) runLoop() {
	defer close(le.doneC)

	for {
		// 检测到若为主 Lessor, 则查找过期的 Lease 实例, 并将其传入到 expiredC 通道中
		le.revokeExpiredLeases()
		le.checkpointScheduledLeases()

		select {
		case <-time.After(500 * time.Millisecond): // 500ms 后再次进行检测
		case <-le.stopC:
			return
		}
	}
}

// revokeExpiredLeases finds all leases past their expiry and sends them to epxired channel for
// to be revoked.
func (le *lessor) revokeExpiredLeases() {
	var ls []*Lease

	// rate limit
	// 限制每次查询获取过期 Lease 实例的数量
	revokeLimit := leaseRevokeRate / 2

	le.mu.RLock()
	// 检测当前 lessor 是否为主 Lessor
	if le.isPrimary() {
		// 查找过期的 Lease 实例
		ls = le.findExpiredLeases(revokeLimit)
	}
	le.mu.RUnlock()

	if len(ls) != 0 {
		select {
		case <-le.stopC:
			return
		case le.expiredC <- ls: // 将过期的 Lease 实例集合传入到 expiredC 通道中
		default: // 如果当前 expiredC 通道阻塞, 则放弃处理检测
			// the receiver of expiredC is probably busy handling
			// other stuff
			// let's try this next time after 500ms
		}
	}
}

// checkpointScheduledLeases finds all scheduled lease checkpoints that are due and
// submits them to the checkpointer to persist them to the consensus log.
func (le *lessor) checkpointScheduledLeases() {
	var cps []*pb.LeaseCheckpoint

	// rate limit
	for i := 0; i < leaseCheckpointRate/2; i++ {
		le.mu.Lock()
		if le.isPrimary() {
			cps = le.findDueScheduledCheckpoints(maxLeaseCheckpointBatchSize)
		}
		le.mu.Unlock()

		if len(cps) != 0 {
			le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: cps})
		}
		if len(cps) < maxLeaseCheckpointBatchSize {
			return
		}
	}
}

func (le *lessor) clearScheduledLeasesCheckpoints() {
	le.leaseCheckpointHeap = make(LeaseQueue, 0)
}

func (le *lessor) clearLeaseExpiredNotifier() {
	le.leaseExpiredNotifier = newLeaseExpiredNotifier()
}

// expireExists returns true if expiry items exist.
// It pops only when expiry item exists.
// "next" is true, to indicate that it may exist in next attempt.
func (le *lessor) expireExists() (l *Lease, ok bool, next bool) {
	if le.leaseExpiredNotifier.Len() == 0 {
		return nil, false, false
	}

	item := le.leaseExpiredNotifier.Poll()
	l = le.leaseMap[item.id]
	if l == nil {
		// lease has expired or been revoked
		// no need to revoke (nothing is expiry)
		le.leaseExpiredNotifier.Unregister() // O(log N)
		return nil, false, true
	}
	now := time.Now()
	if now.UnixNano() < item.time /* expiration time */ {
		// Candidate expirations are caught up, reinsert this item
		// and no need to revoke (nothing is expiry)
		return l, false, false
	}

	// recheck if revoke is complete after retry interval
	item.time = now.Add(le.expiredLeaseRetryInterval).UnixNano()
	le.leaseExpiredNotifier.RegisterOrUpdate(item)
	return l, true, false
}

// findExpiredLeases loops leases in the leaseMap until reaching expired limit
// and returns the expired leases that needed to be revoked.
func (le *lessor) findExpiredLeases(limit int) []*Lease {
	leases := make([]*Lease, 0, 16)

	for {
		l, ok, next := le.expireExists()
		if !ok && !next {
			break
		}
		if !ok {
			continue
		}
		if next {
			continue
		}

		// 该 Lease 实例已过期, 则将其添加到 leases 切片中
		if l.expired() {
			leases = append(leases, l)

			// reach expired limit
			// 达到查找的上限, 则停止查找
			if len(leases) == limit {
				break
			}
		}
	}

	return leases
}

func (le *lessor) scheduleCheckpointIfNeeded(lease *Lease) {
	if le.cp == nil {
		return
	}

	if lease.RemainingTTL() > int64(le.checkpointInterval.Seconds()) {
		if le.lg != nil {
			le.lg.Debug("Scheduling lease checkpoint",
				zap.Int64("leaseID", int64(lease.ID)),
				zap.Duration("intervalSeconds", le.checkpointInterval),
			)
		}
		heap.Push(&le.leaseCheckpointHeap, &LeaseWithTime{
			id:   lease.ID,
			time: time.Now().Add(le.checkpointInterval).UnixNano(),
		})
	}
}

func (le *lessor) findDueScheduledCheckpoints(checkpointLimit int) []*pb.LeaseCheckpoint {
	if le.cp == nil {
		return nil
	}

	now := time.Now()
	cps := []*pb.LeaseCheckpoint{}
	for le.leaseCheckpointHeap.Len() > 0 && len(cps) < checkpointLimit {
		lt := le.leaseCheckpointHeap[0]
		if lt.time /* next checkpoint time */ > now.UnixNano() {
			return cps
		}
		heap.Pop(&le.leaseCheckpointHeap)
		var l *Lease
		var ok bool
		if l, ok = le.leaseMap[lt.id]; !ok {
			continue
		}
		if !now.Before(l.expiry) {
			continue
		}
		remainingTTL := int64(math.Ceil(l.expiry.Sub(now).Seconds()))
		if remainingTTL >= l.ttl {
			continue
		}
		if le.lg != nil {
			le.lg.Debug("Checkpointing lease",
				zap.Int64("leaseID", int64(lt.id)),
				zap.Int64("remainingTTL", remainingTTL),
			)
		}
		cps = append(cps, &pb.LeaseCheckpoint{ID: int64(lt.id), Remaining_TTL: remainingTTL})
	}
	return cps
}

func (le *lessor) initAndRecover() {
	// 开启 v3 存储的读写事务
	tx := le.b.BatchTx()
	tx.Lock() // 对该读写事务加锁

	// 在 BoltDB 中创建名为 "lease" 的 Bucket
	tx.UnsafeCreateBucket(leaseBucketName)
	// 查找 "lease" Bucket 中所有的信息
	_, vs := tx.UnsafeRange(leaseBucketName, int64ToBytes(0), int64ToBytes(math.MaxInt64), 0)
	// TODO: copy vs and do decoding outside tx lock if lock contention becomes an issue.
	for i := range vs {
		var lpb leasepb.Lease
		// 反序列化得到 leasepb.Lease 实例, 其中只记录了对应的 id 和过期时间
		err := lpb.Unmarshal(vs[i])
		if err != nil {
			tx.Unlock()
			panic("failed to unmarshal lease proto item")
		}
		ID := LeaseID(lpb.ID)
		// 修正过期时间
		if lpb.TTL < le.minLeaseTTL {
			lpb.TTL = le.minLeaseTTL
		}
		// 创建 Lease 实例并记录到 leaseMap 中
		le.leaseMap[ID] = &Lease{
			ID:  ID,
			ttl: lpb.TTL,
			// itemSet will be filled in when recover key-value pairs
			// set expiry to forever, refresh when promoted
			itemSet: make(map[LeaseItem]struct{}),
			expiry:  forever,
			revokec: make(chan struct{}),
		}
	}
	le.leaseExpiredNotifier.Init()
	heap.Init(&le.leaseCheckpointHeap)
	tx.Unlock()

	// 提交事务
	le.b.ForceCommit()
}

type Lease struct {
	// 该 Lease 实例的唯一 ID
	ID           LeaseID
	// 该 Lease 实例的存活时长
	ttl          int64 // time to live of the lease in seconds
	remainingTTL int64 // remaining time to live in seconds, if zero valued it is considered unset and the full ttl should be used
	// expiryMu protects concurrent accesses to expiry
	expiryMu sync.RWMutex
	// expiry is time when lease should expire. no expiration when expiry.IsZero() is true
	//
	// 该 Lease 实例过期的时间戳
	expiry time.Time

	// mu protects concurrent accesses to itemSet
	mu      sync.RWMutex
	// 该 map 中的 Key 是与当前 Lease 实例绑定的 LeaseItem 实例, Value 始终为空结构体
	itemSet map[LeaseItem]struct{}
	// 该 Lease 实例被撤销时会关闭该通道, 从而实现通知监听该通道的 goroutine 的效果.
	revokec chan struct{}
}

func (l *Lease) expired() bool {
	return l.Remaining() <= 0
}

func (l *Lease) persistTo(b backend.Backend) {
	key := int64ToBytes(int64(l.ID))

	lpb := leasepb.Lease{ID: int64(l.ID), TTL: l.ttl, RemainingTTL: l.remainingTTL}
	val, err := lpb.Marshal()
	if err != nil {
		panic("failed to marshal lease proto item")
	}

	b.BatchTx().Lock()
	b.BatchTx().UnsafePut(leaseBucketName, key, val)
	b.BatchTx().Unlock()
}

// TTL returns the TTL of the Lease.
func (l *Lease) TTL() int64 {
	return l.ttl
}

// RemainingTTL returns the last checkpointed remaining TTL of the lease.
// TODO(jpbetz): do not expose this utility method
func (l *Lease) RemainingTTL() int64 {
	if l.remainingTTL > 0 {
		return l.remainingTTL
	}
	return l.ttl
}

// refresh refreshes the expiry of the lease.
func (l *Lease) refresh(extend time.Duration) {
	newExpiry := time.Now().Add(extend + time.Duration(l.RemainingTTL())*time.Second)
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = newExpiry
}

// forever sets the expiry of lease to be forever.
func (l *Lease) forever() {
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = forever
}

// Keys returns all the keys attached to the lease.
func (l *Lease) Keys() []string {
	l.mu.RLock()
	keys := make([]string, 0, len(l.itemSet))
	for k := range l.itemSet {
		keys = append(keys, k.Key)
	}
	l.mu.RUnlock()
	return keys
}

// Remaining returns the remaining time of the lease.
func (l *Lease) Remaining() time.Duration {
	l.expiryMu.RLock()
	defer l.expiryMu.RUnlock()
	if l.expiry.IsZero() {
		return time.Duration(math.MaxInt64)
	}
	return time.Until(l.expiry)
}

type LeaseItem struct {
	Key string // 键值对的原始 Key 值
}

func int64ToBytes(n int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(n))
	return bytes
}

// FakeLessor is a fake implementation of Lessor interface.
// Used for testing only.
type FakeLessor struct{}

func (fl *FakeLessor) SetRangeDeleter(dr RangeDeleter) {}

func (fl *FakeLessor) SetCheckpointer(cp Checkpointer) {}

func (fl *FakeLessor) Grant(id LeaseID, ttl int64) (*Lease, error) { return nil, nil }

func (fl *FakeLessor) Revoke(id LeaseID) error { return nil }

func (fl *FakeLessor) Checkpoint(id LeaseID, remainingTTL int64) error { return nil }

func (fl *FakeLessor) Attach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) GetLease(item LeaseItem) LeaseID            { return 0 }
func (fl *FakeLessor) Detach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) Promote(extend time.Duration) {}

func (fl *FakeLessor) Demote() {}

func (fl *FakeLessor) Renew(id LeaseID) (int64, error) { return 10, nil }

func (fl *FakeLessor) Lookup(id LeaseID) *Lease { return nil }

func (fl *FakeLessor) Leases() []*Lease { return nil }

func (fl *FakeLessor) ExpiredLeasesC() <-chan []*Lease { return nil }

func (fl *FakeLessor) Recover(b backend.Backend, rd RangeDeleter) {}

func (fl *FakeLessor) Stop() {}

type FakeTxnDelete struct {
	backend.BatchTx
}

func (ftd *FakeTxnDelete) DeleteRange(key, end []byte) (n, rev int64) { return 0, 0 }
func (ftd *FakeTxnDelete) End()                                       { ftd.Unlock() }
