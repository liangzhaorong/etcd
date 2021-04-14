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

package raft

import (
	"context"
	"errors"

	pb "go.etcd.io/etcd/raft/raftpb"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState = pb.HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	// 当前集群的 Leader 节点 ID
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	// 当前节点的角色
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
//
// Ready 实例只是用来传递数据的, 其全部字段都是只读.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	//
	// 内嵌 SoftState, 在 SoftState 中封装了当前集群的 Leader 节点 ID（Lead 字段）及当前节点的角色（RaftState）.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	//
	// 内嵌 HardState, 在 HardState 中封装了当前节点的任期号（Term 字段）、当前节点在该任期投票结果（Vote 字段）及
	// 当前节点的 raftLog 的已提交位置.
	pb.HardState

	// ReadStates can be used for node to serve linearizable read requests locally
	// when its applied index is greater than the index in ReadState.
	// Note that the readState will be returned when raft receives msgReadIndex.
	// The returned is only valid for the request that requested to read.
	//
	// 该字段记录了当前节点中等待处理的只读请求.
	ReadStates []ReadState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	//
	// 该字段中的 Entry 记录是从 unstable 中读取出来的, 上层模块会将其保存到 Storage 中.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	//
	// 待持久化的快照数据, pb.Snapshot 中封装了快照数据及相关元数据.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	//
	// 已提交、待应用的 Entry 记录, 这些 Entry 记录之前已经保存到 Storage 中.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	//
	// 该字段中保存了当前节点中等待发送到集群中其他节点的 Message 消息.
	Messages []pb.Message

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk or if an asynchronous write is permissible.
	MustSync bool
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0 || len(rd.ReadStates) != 0
}

// appliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information is
// contained in the Ready, returns zero.
func (rd Ready) appliedCursor() uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	if index := rd.Snapshot.Metadata.Index; index > 0 {
		return index
	}
	return 0
}

// Node represents a node in a raft cluster.
//
// Node 代表 raft 集群中的一个节点.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	//
	// 该方法用来推进 raft 结构体中的选举计时器和心跳计时器的逻辑时钟的指针.
	Tick()
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	//
	// 当选举计时器超时后, 会调用 Campaign 方法将当前节点切换成 Candidate 状态（或是 PerCandidate 状态）, 底层
	// 就是通过发送 MsgHup 消息实现的.
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log. Note that proposals can be lost without
	// notice, therefore it is user's job to ensure proposal retries.
	//
	// 接收到 Client 发来的写请求时, Node 实例会调用 Propose() 方法进行处理, 底层就是通过发送 MsgProp 消息实现的.
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes a configuration change. Like any proposal, the
	// configuration change may be dropped with or without an error being
	// returned. In particular, configuration changes are dropped unless the
	// leader has certainty that there is no prior unapplied configuration
	// change in its log.
	//
	// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
	// message. The latter allows arbitrary configuration changes via joint
	// consensus, notably including replacing a voter. Passing a ConfChangeV2
	// message is only allowed if all Nodes participating in the cluster run a
	// version of this library aware of the V2 API. See pb.ConfChangeV2 for
	// usage details and semantics.
	//
	// Client 除了会发送读写请求, 还会发送修改集群配置的请求（如, 新增集群中的节点）,
	// 这种请求 Node 实例会调用 ProposeConfChange() 方法进行处理, 底层就是通过发送
	// MsgProp 消息实现的, 只不过其中记录的 Entry 记录是 EntryConfChange 类型.
	ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error

	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	//
	// 当前节点收到其他节点的消息时, 会通过 Step() 方法将消息交给底层封装的 raft 实例进行处理.
	Step(ctx context.Context, msg pb.Message) error

	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by Ready.
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	//
	// Ready() 方法返回的是一个 channel, 通过该 channel 返回的 Ready 实例中封装了底层 raft 实例的相关状态数据,
	// 如, 需要发送到其他节点的消息、交给上层模块的 Entry 记录, 等等. 这是 etcd-raft 模块与上层模块交互的主要方式.
	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	//
	// 当上层模块处理完从上述 channel 中返回的 Ready 实例后, 需要调用 Advance() 通知底层的 etcd-raft 模块返回
	// 新的 Ready 实例.
	Advance()
	// ApplyConfChange applies a config change (previously passed to
	// ProposeConfChange) to the node. This must be called whenever a config
	// change is observed in Ready.CommittedEntries.
	//
	// Returns an opaque non-nil ConfState protobuf which must be recorded in
	// snapshots.
	//
	// 在收到集群配置请求时, 会通过调用 ApplyConfChange() 方法进行处理.
	ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState

	// TransferLeadership attempts to transfer leadership to the given transferee.
	//
	// 该方法用于 Leader 节点的转移.
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	// ReadIndex request a read state. The read state will be set in the ready.
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	//
	// 该方法用于处理只读请求.
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	//
	// 返回当前节点的运行状态.
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.
	//
	// 通过该方法通知底层的 raft 实例, 当前节点无法与指定的节点进行通信.
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot. The id is the raft ID of the follower
	// who is meant to receive the snapshot, and the status is SnapshotFinish or SnapshotFailure.
	// Calling ReportSnapshot with SnapshotFinish is a no-op. But, any failure in applying a
	// snapshot (for e.g., while streaming it from leader to follower), should be reported to the
	// leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
	// log probes until the follower can apply the snapshot and advance its state. If the follower
	// can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
	// updates from the leader. Therefore, it is crucial that the application ensures that any
	// failure in snapshot sending is caught and reported back to the leader; so it can resume raft
	// log probing in the follower.
	//
	// 该方法用于通知底层的 raft 实例上次发送快照的结果.
	ReportSnapshot(id uint64, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.
	//
	// 关闭当前节点.
	Stop()
}

type Peer struct {
	ID      uint64 // 节点 ID
	Context []byte // 该节点对应的 Member 实例序列化后的数据
}

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
//
// StartNode 当集群中的节点初次启动时会通过 StartNode() 函数启动创建对应的 node 实例和底层的 raft 实例. 在 StartNode() 方法中,
// 主要根据传入的配置 c 创建并启动 Node 实例, 以及创建 raft peers 列表.
//
// peers: Peer 结构体封装了节点的 ID, peers 参数记录当前集群中全部节点的 ID.
func StartNode(c *Config, peers []Peer) Node {
	// 若当前集群没有节点, 则终止程序
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	// 创建 RawNode 实例, 在 RawNode 中封装了 raft 实例.
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	// 集群中的节点初始启动会调用该方法引导并初始化节点 raft 的初始状态
	rn.Bootstrap(peers)

	// 初始化 node 实例
	n := newNode(rn)

	// 启动一个 goroutine, 其中会根据底层 raft 的状态及上层模块传递的数据, 协调处理 node 中各个通道的数据.
	go n.run()
	return &n
}

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
//
// RestartNode 当集群中的节点重新启动时, 就不是通过 StartNode() 函数创建 node 实例, 而是调用 RestartNode() 函数,
// 该函数与 StartNode() 最大的区别是 raft 实例的初始信息是从 Storage 中恢复的.
func RestartNode(c *Config) Node {
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	n := newNode(rn)
	go n.run()
	return &n
}

type msgWithResult struct {
	m      pb.Message
	// 消息的处理结果通过该通道返回
	result chan error
}

// node is the canonical implementation of the Node interface
//
// node 结构体是 Node 接口的实现之一.
type node struct {
	// 该通道用于接收 MsgProp 类型的消息.
	propc      chan msgWithResult
	// 除 MsgProp 外的其他类型的消息都是由该通道接收的.
	recvc      chan pb.Message
	// 当节点收到 EntryConfChange 类型的 Entry 记录时, 会转换成 ConfChange, 并写入该通道中等待处理.
	// 在 ConfChange 中封装了其唯一 ID、待处理的节点 ID（NodeID 字段）及处理类型（Type 字段）.
	confc      chan pb.ConfChangeV2
	// 在 ConfState 中封装了当前集群中所有节点的 ID, 该通道用于向上层模块返回 ConfState 实例.
	confstatec chan pb.ConfState
	// 该通道用于向上层模块返回 Ready 实例, 即 node.Ready() 方法的返回值.
	readyc     chan Ready
	// 当上层模块处理完通过上述 readyc 通道获取到的 Ready 实例之后, 会通过 node.Advance() 方法向该通道
	// 写入信号, 从而通知底层 raft 实例.
	advancec   chan struct{}
	// 用来接收逻辑时钟发出的信号, 之后会根据当前节点的角色推进选举计时器和心跳计时器. (仅该 channel 带 128 缓冲, 其他都是无缓冲的)
	tickc      chan struct{}
	// 当检测到 done 通道关闭后, 在其上阻塞的 goroutine 会继续执行, 并进行相应的关闭操作.
	done       chan struct{}
	// 当 node.Stop() 方法被调用时, 会向该通道发送信号, 有另一个 goroutine 会尝试读取该通道中的内容,
	// 当读取到信息后, 会关闭 done 通道.
	stop       chan struct{}
	status     chan chan Status

	rn *RawNode
}

func newNode(rn *RawNode) node {
	return node{
		propc:      make(chan msgWithResult),
		recvc:      make(chan pb.Message),
		confc:      make(chan pb.ConfChangeV2),
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickc:  make(chan struct{}, 128),
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
		status: make(chan chan Status),
		rn:     rn,
	}
}

func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}

// run 根据底层 raft 的状态及上层模块传递的数据, 协调处理 node 中各个通道的数据
func (n *node) run() {
	// 指向 node.propc 通道
	var propc chan msgWithResult
	// 指向 node.readyc 通道
	var readyc chan Ready
	// 指向 node.advancec 通道
	var advancec chan struct{}
	var rd Ready

	r := n.rn.raft

	// 用于记录当前 Leader 节点
	lead := None

	for {
		if advancec != nil {
			// 上层模块还在处理上次从 readyc 通道返回的 Ready 实例, 所以不能继续向 readyc 中写入数据
			readyc = nil
		} else if n.rn.HasReady() { // 检测是否有待处理的 Ready 实例
			// Populate a Ready. Note that this Ready is not guaranteed to
			// actually be handled. We will arm readyc, but there's no guarantee
			// that we will actually send on it. It's possible that we will
			// service another channel instead, loop around, and then populate
			// the Ready again. We could instead force the previous Ready to be
			// handled first, but it's generally good to emit larger Readys plus
			// it simplifies testing (by emitting less frequently and more
			// predictably).
			//
			// 获取待处理的 Ready 实例
			rd = n.rn.readyWithoutAccept()
			// 将 readyc 指向 node.readyc, 从而将 Ready 实例交给上层模块处理
			readyc = n.readyc
		}

		// 检测当前的 Leader 节点是否发生变化
		if lead != r.lead {
			// 如果当前节点无法确定集群中的 Leader 节点, 则清空 propc, 此次循环不再处理 MsgProp 消息
			if r.hasLeader() {
				if lead == None {
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				// 当前集群存在 Leader 节点, 则将 propc 指向 node.propc, 从而可以处理 MsgProp 消息
				propc = n.propc
			} else {
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				// 清空 propc, 此次循环不再处理 MsgProp 消息
				propc = nil
			}
			// 更新 Leader
			lead = r.lead
		}

		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		case pm := <-propc: // 读取 propc 通道, 获取 MsgProp 消息, 并交给 raft.Step() 方法处理
		    // 获取 MsgProp 消息
			m := pm.m
			// 设置 MsgProp 消息的 From 字段为当前节点 ID
			m.From = r.id
			// 将消息交给 raft.Step() 处理
			err := r.Step(m)
			// 若 pm.result 不为 nil, 则表示上层等待 Raft 处理该 MsgProp 消息的结果
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}
		case m := <-n.recvc: // 读取 node.recvc 通道, 获取消息（非 MsgProp 类型）
			// filter out response message from unknown From.
			// 过滤掉未知来源的消息, 如 MsgHeartbeatResp 类型消息将会被过滤掉
			if pr := r.prs.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
				// 将消息交给 raft.Step() 处理
				r.Step(m)
			}
		case cc := <-n.confc: // 读取 node.confc 通道, 读取 ConfChange 实例
			_, okBefore := r.prs.Progress[r.id]
			// 处理该 ConfChange 实例
			cs := r.applyConfChange(cc)
			// If the node was removed, block incoming proposals. Note that we
			// only do this if the node was in the config before. Nodes may be
			// a member of the group without knowing this (when they're catching
			// up on the log and don't have the latest config) and we don't want
			// to block the proposal channel in that case.
			//
			// NB: propc is reset when the leader changes, which, if we learn
			// about it, sort of implies that we got readded, maybe? This isn't
			// very sound and likely has bugs.
			//
			// 若当前节点被从集群中移除了
			if _, okAfter := r.prs.Progress[r.id]; okBefore && !okAfter {
				var found bool
				for _, sl := range [][]uint64{cs.Voters, cs.VotersOutgoing} {
					for _, id := range sl {
						if id == r.id {
							found = true
						}
					}
				}
				// 当前节点被删除了, 则清空 propc 变量, 保证后续不再处理 MsgProp 消息
				if !found {
					propc = nil
				}
			}
			select {
			// 将当前集群中结点的信息封装成 ConfState 实例并写入 confstatec 通道中, 上层模块会读取该通道,
			// 并从中获取当前集群的节点信息.
			case n.confstatec <- cs:
			case <-n.done:
			}
		case <-n.tickc: // 逻辑时钟每推进一次, 就会向 tickc 通道写入一个信号
		    // 这里, 若为 Leader 节点则推进选举计时器, 若为 Follower 节点则推进心跳计时器
			n.rn.Tick()
		case readyc <- rd: // 将前面创建的 Ready 实例写入 node.readyc 通道中, 等待上层模块读取
			// 上层模块接收了 Ready 实例后, 调用该方法更新 RawNode 实例中的相关字段
			n.rn.acceptReady(rd)
			// 将 advancec 指向 node.advancec 通道, 这样在下次 for 循环时, 就无法继续向上层模块返回
			// Ready 实例了（因为 readyc 会被设置为 nil, 无法向 readyc 通道中写入 Ready 实例）
			advancec = n.advancec
		case <-advancec: // 上层模块处理完 Ready 实例后, 会向 advancec 通道写入信号
			n.rn.Advance(rd)
			rd = Ready{}
			// 清空 advancec, 下次 for 循环处理的过程中, 就能向 readyc 通道写入 Ready 实例了
			advancec = nil
		case c := <-n.status:
			c <- getStatus(r) // 创建 Status 实例
		case <-n.stop:
			close(n.done)
			return
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
//
// Tick 递增当前节点的内部逻辑时钟. 选举超时和心跳超时以该时钟滴答为单位.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.rn.raft.logger.Warningf("%x (leader %v) A tick missed to fire. Node blocks too long!", n.rn.raft.id, n.rn.raft.id == n.rn.raft.lead)
	}
}

// Campaign 当选举计时器超时后, 会调用 Campaign 方法将当前节点切换成 Candidate 状态（或是 PerCandidate 状态）, 底层
// 就是通过发送 MsgHup 消息实现的.
func (n *node) Campaign(ctx context.Context) error { return n.step(ctx, pb.Message{Type: pb.MsgHup}) }

// Propose 接收到 Client 发来的写请求时, Node 实例会调用 Propose() 方法进行处理, 底层就是通过发送 MsgProp 消息实现的.
func (n *node) Propose(ctx context.Context, data []byte) error {
	// 将 InternalRaftRequest 实例序列化后的数据 data 封装成 MsgProp 类型的 Message 消息,
	// 然后调用 node.stepWait() 消息进行处理, 该方法会阻塞等待处理结果.
	return n.stepWait(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (n *node) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) {
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

func confChangeToMsg(c pb.ConfChangeI) (pb.Message, error) {
	typ, data, err := pb.MarshalConfChange(c)
	if err != nil {
		return pb.Message{}, err
	}
	return pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: typ, Data: data}}}, nil
}

// ProposeConfChange Client 除了会发送读写请求, 还会发送修改集群配置的请求（如, 新增集群中的节点）,
// 这种请求 Node 实例会调用 ProposeConfChange() 方法进行处理, 底层就是通过发送
// MsgProp 消息实现的, 只不过其中记录的 Entry 记录是 EntryConfChange 类型.
func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error {
	msg, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return n.Step(ctx, msg)
}

// step 该方法主要处理从其他节点收到的网络消息, 而不是本地消息.
func (n *node) step(ctx context.Context, m pb.Message) error {
	// 第三个参数为 false 表示不阻塞等待结果, 将消息发送出去后直接返回
	return n.stepWithWaitOption(ctx, m, false)
}

func (n *node) stepWait(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, true)
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
//
// wait 为 true 表示等待消息的处理结果
func (n *node) stepWithWaitOption(ctx context.Context, m pb.Message, wait bool) error {
	// 非 MsgProp 消息统一写入 node.recvc 通道
	if m.Type != pb.MsgProp {
		select {
		case n.recvc <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}
	ch := n.propc
	// 再次将 MsgProp 消息封装成 msgWithResult 实例
	pm := msgWithResult{m: m}
	// wait 为 true 表示需等待消息的处理结果
	if wait {
		pm.result = make(chan error, 1)
	}
	select {
	case ch <- pm: // 将封装后的 MsgProp 消息写入 propc 通道中
		// 若无需等待处理结果, 则直接返回
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	select {
	// 监听等待消息的处理结果
	case err := <-pm.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	return nil
}

func (n *node) Ready() <-chan Ready { return n.readyc }

// Advance 当上层模块通过 readyc 通道读取 Ready 实例后, 会将其中封装数据进行一系列处理, 如, 上层模块会应用
// Ready.CommittedEntries 中的 Entry 记录、持久化 Ready.Entries 中的记录和快照数据等. 当上层模块处理完此次
// 返回的 Ready 实例后, 会通过 node.Advance() 方法向 node.advancec 通道写入信号, 通知 etcd-raft 模块其此次
// 返回的 Ready 实例已经被处理完成.
func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}: // 向 node.advancec 通道写入信号
	case <-n.done:
	}
}

func (n *node) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	var cs pb.ConfState
	select {
	case n.confc <- cc.AsV2():
	case <-n.done:
	}
	select {
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

// Status 用于读取当前节点的状态
func (n *node) Status() Status {
	// 创建一个用于传递 Status 实例的通道
	c := make(chan Status)
	select {
	case n.status <- c: // 将该通道写入到 node.status 通道中
		return <-c      // 阻塞等待 run() 方法写入 Status 实例
	case <-n.done:
		return Status{}
	}
}

// ReportUnreachable 通过该方法通知底层的 raft 实例, 当前节点无法与指定的节点进行通信.
func (n *node) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-n.done:
	}
}

func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	// 用于设置 MsgSnapStatus 消息的 Reject
	rej := status == SnapshotFailure

	select {
	case n.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

// TransferLeadership 用于 Leader 节点的转移.
func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	// manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
	case n.recvc <- pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead}:
	case <-n.done:
	case <-ctx.Done():
	}
}

// ReadIndex 用于处理线性读请求.
func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	// 将传入的值 rctx（实际为当前线性读请求的 id）封装成 MsgReadIndex 类型的 Message 消息
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}

// newReady 读取底层 raft 实例中的各项数据及相关状态, 并最终封装成 Ready 实例, 该 Ready 实例最终会返回给上层模块.
//
// prevSoftSt 和 prevHardSt 用于记录上次创建 Ready 实例时记录的 raft 实例的相关状态
func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	// 创建 Ready 实例
	rd := Ready{
		// 获取 unstable 中所有的 Entry 记录, 这些 Entry 记录会交给上层模块进行持久化
		Entries:          r.raftLog.unstableEntries(),
		// 获取当前节点 raftLog 中所有已提交待应用的 Entry 记录, 即 raftLog 中 applied~committed 之间的所有记录
		CommittedEntries: r.raftLog.nextEnts(),
		// 获取当前节点中等待发送到集群中其他节点的消息
		Messages:         r.msgs,
	}
	// 检测两次创建 Ready 实例之间, raft 实例状态是否发生变化, 如果无变化, 则将 Ready 实例相关字段设置为 nil,
	// 表示无须上层模块处理
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	// 检测 unstable 中是否记录了新的快照数据, 如果有, 则将其封装到 Ready 实例中, 交给上层模块进行处理
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	// 底层 raft 会将能响应的只读请求信息封装成 ReadState 并记录到 readStates 中, 这里会检测 raft.readStates 字段,
	// 并将其封装到 Ready 实例返回给上层模块.
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	rd.MustSync = MustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}
