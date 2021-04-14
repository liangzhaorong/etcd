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
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/confchange"
	"go.etcd.io/etcd/raft/quorum"
	pb "go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/raft/tracker"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0
const noLimit = math.MaxUint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)

type ReadOnlyOption int

const (
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlySafe 是 etcd 作者推荐的模式, 该模式下的只读请求处理, 不会受节点之间时钟差异和网络分区的影响.
	ReadOnlySafe ReadOnlyOption = iota
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	//
	// ReadOnlyLeaseBased 模式下, Leader 节点并不会发送任何消息来确认自身身份.
	ReadOnlyLeaseBased
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// Config contains the parameters to start a raft.
//
// Config 主要用于配置参数的传递, 在创建 raft 实例时需要的参数会通过 Config 实例传递进去.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	//
	// 当前节点的 ID.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	//
	// 记录了集群中所有节点的 ID.
	peers []uint64

	// learners contains the IDs of all learner nodes (including self if the
	// local node is a learner) in the raft cluster. learners only receives
	// entries from the leader node. It does not vote or promote itself.
	learners []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	//
	// 用于初始化 raft.electionTimeout, 即逻辑时钟连续推进多少次后, 就会触发 Follower 节点的状态切换
	// 及新一轮的 Leader 选举.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	//
	// 用于初始化 raft.heartbeatTimeout, 即逻辑时钟连续推进多少次后, 就触发 Leader 节点发送心跳消息.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	//
	// 当前节点保存 raft 日志记录使用的存储.（通常指向 MemoryStorage）
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	//
	// 当前已经应用的记录位置（已应用的最后一条 Entry 记录的索引值）, 该值在节点重启时需要设置, 否则会重新
	// 应用已经应用过的 Entry 记录.
	// 当上层模块自己来控制正确的已应用位置时使用该配置
	Applied uint64

	// MaxSizePerMsg limits the max byte size of each append message. Smaller
	// value lowers the raft recovery cost(initial probing and message lost
	// during normal operation). On the other side, it might affect the
	// throughput during normal replication. Note: math.MaxUint64 for unlimited,
	// 0 for at most one entry per message.
	//
	// 用于初始化 raft.maxMsgSize 字段, 每条消息的最大字节数, 如果是 math.MaxUint64 则没有上限, 如果是 0 则
	// 表示每条消息最多携带一条 Entry.
	MaxSizePerMsg uint64
	// MaxCommittedSizePerReady limits the size of the committed entries which
	// can be applied.
	//
	// 限制可应用的已提交 Entry 记录的总字节数
	 MaxCommittedSizePerReady uint64
	// MaxUncommittedEntriesSize limits the aggregate byte size of the
	// uncommitted entries that may be appended to a leader's log. Once this
	// limit is exceeded, proposals will begin to return ErrProposalDropped
	// errors. Note: 0 for no limit.
	MaxUncommittedEntriesSize uint64
	// MaxInflightMsgs limits the max number of in-flight append messages during
	// optimistic replication phase. The application transportation layer usually
	// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	// overflowing that sending buffer. TODO (xiangli): feedback to application to
	// limit the proposal rate?
	//
	// 用于初始化 maxInflight, 即已经发送出去且未收到响应的最大消息个数.
	MaxInflightMsgs int

	// CheckQuorum specifies if the leader should check quorum activity. Leader
	// steps down when quorum is not active for an electionTimeout.
	//
	// 是否开启 CheckQuorum 模式, 用于初始化 raft.checkQuorum 字段.
	CheckQuorum bool

	// PreVote enables the Pre-Vote algorithm described in raft thesis section
	// 9.6. This prevents disruption when a node that has been partitioned away
	// rejoins the cluster.
	//
	// 是否开启 PreVote 模式, 用于初始化 raft.preVote 字段.
	PreVote bool

	// ReadOnlyOption specifies how the read only request is processed.
	//
	// ReadOnlySafe guarantees the linearizability of the read only request by
	// communicating with the quorum. It is the default and suggested option.
	//
	// ReadOnlyLeaseBased ensures linearizability of the read only request by
	// relying on the leader lease. It can be affected by clock drift.
	// If the clock drift is unbounded, leader might keep the lease longer than it
	// should (clock can move backward/pause without any bound). ReadIndex is not safe
	// in that case.
	// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
	ReadOnlyOption ReadOnlyOption

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	Logger Logger

	// DisableProposalForwarding set to true means that followers will drop
	// proposals, rather than forwarding them to the leader. One use case for
	// this feature would be in a situation where the Raft leader is used to
	// compute the data of a proposal, for example, adding a timestamp from a
	// hybrid logical clock to data in a monotonically increasing way. Forwarding
	// should be disabled to prevent a follower with an inaccurate hybrid
	// logical clock from assigning the timestamp and then forwarding the data
	// to the leader.
	DisableProposalForwarding bool
}

func (c *Config) validate() error {
	// 当前节点 ID 不能为 None
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	// 必须设置心跳超时时间
	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	// 选举时间必须大于心跳超时时间
	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxUncommittedEntriesSize == 0 {
		c.MaxUncommittedEntriesSize = noLimit
	}

	// default MaxCommittedSizePerReady to MaxSizePerMsg because they were
	// previously the same parameter.
	if c.MaxCommittedSizePerReady == 0 {
		c.MaxCommittedSizePerReady = c.MaxSizePerMsg
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}

	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased")
	}

	return nil
}

type raft struct {
	// 当前节点在集群中的 ID
	id uint64

	// 当前任期号. 如果 Message 的 Term 为 0, 则表示该消息是本地消息, 如 MsgHup、MsgProp、MsgReadIndex 等
	Term uint64
	// 当前任期中当前节点将选票投给了哪个节点, 未投票时, 该字段为 None.
	Vote uint64

	// 如果是客户端直接发送到 Leader 节点的只读请求 MsgReadIndex 消息, 则将 MsgReadIndex 消息对应的已提交位置
	// 以及其消息 ID 封装成 ReadState 实例, 添加到 raft.readStates 中保存. 后续会有其他 goroutine
	// 读取该数组, 并对相应的 MsgReadIndex 消息进行响应.
	readStates []ReadState

	// the log
	// 在 Raft 协议中的每个节点都会记录本地 Log, 在 etcd-raft 模块中, 使用该结构体 raftLog 表示本地 Log.
	raftLog *raftLog

	// 单条消息的最大字节数
	maxMsgSize         uint64
	// 限制 raftLog 中未提交的 Entry 记录的总字节数最大值
	maxUncommittedSize uint64
	// TODO(tbg): rename to trk.
	// Leader 节点使用该对象跟踪记录所有 Follower 节点的状态信息
	prs tracker.ProgressTracker

	// 当前节点在集群中的角色, 可选值分为 StateFollower、StateCandidate、StateLeader 和 StatePreCandidate
	state StateType

	// isLearner is true if the local raft node is a learner.
	// 当前节点是否为 Learner 节点
	isLearner bool

	// 缓存了当前节点等待发送的消息
	msgs []pb.Message

	// the leader id
	// 当前集群中 Leader 节点的 ID
	lead uint64
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	//
	// 用于集群中 Leader 节点的转移, leadTransferee 记录了此次 Leader 角色转移的目标节点的 ID.
	// TODO: 在进行 Leader 节点转移时, Leader 节点会停止处理客户端发来的 MsgProp 消息, 这是因为
	//  如果客户端不断追加 Entry 记录, 则可能使目标 Follower 节点与 Leader 节点的 raftLog 始终不
	//  匹配, 从而导致 Leader 节点的迁移超时.
	leadTransferee uint64
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	pendingConfIndex uint64
	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
	//
	// 记录 raftLog 中当前未提交的 Entry 记录的总字节数大小
	uncommittedSize uint64

	// 批量处理只读请求
	readOnly *readOnly

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	//
	// 选举计时器的指针, 其单位是逻辑时钟的刻度, 逻辑时钟每推进一次, 该字段值就会增加 1.
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	//
	// 心跳计时器的指针, 其单位也是逻辑时钟的刻度, 逻辑时钟每推进一次, 该字段值就会加 1.
	heartbeatElapsed int

	// Leader 节点只有在收到更大 Term 值的消息时才会切换成 Follower 状态. 故在发生网络分区时, 即使在其他分区
	// 里新的 Leader 节点已经被选举出来, 旧的 Leader 节点由于接收不到新 Leader 节点的心跳消息, 依然会认为自己
	// 是当前集群的 Leader 节点（与其在同一网络分区的 Follower 节点也认为它是当前集群的 Leader 节点）, 它依然
	// 会接收客户端的请求, 但无法向客户端返回任何响应.
	//
	// checkQuorum 机制的意思是: 每隔一段时间, Leader 节点会尝试连接集群中的其他节点（发送心跳消息）, 如果发现
	// 自己可以连接到节点个数没有超过半数（即没有收到足够的心跳响应）, 则主动切换为 Follower 状态. 这样, 在上述
	// 网络分区的场景中, 旧的 Leader 节点可以很快知道自己已经过期, 可以减少 client 连接旧 Leader 节点的等待时间.
	checkQuorum bool
	// Follower 节点在选举计时器超时后, 会切换成 Candidate 状态并发起选举. 然而 Follower 节点超时没有收到心跳
	// 消息时, 也可能是由于 Follower 节点自身的网络问题导致的, 如, 网络分区. 即使如此, 该 Follower 节点还是会
	// 不断地发起选举, 其 Term 值也会不断递增. 待该 Follower 节点的网络故障恢复并收到 Leader 节点的心跳消息时,
	// 由于其 Term 值已经增加, 该 Follower 节点会丢弃掉 Term 值比自己小的心跳消息, 之后就会触发一次没有必要进
	// 行的 Leader 选举.
	//
	// PreVote 机制优化避免上述情况. 当 Follower 节点准备发起一次选举前, 会先连接集群中的其他节点, 并询问它们
	// 是否愿意参与选举, 如果集群中的其他节点能够正常收到 Leader 节点的心跳消息, 则会拒绝参与选举, 反之则参与
	// 选举. 当在 PreVote 过程中, 有超过半数的节点响应并参与新一轮选举, 则可以发起新一轮的选举.
	preVote     bool

	// 心跳超时时间, 当 heartbeatElapsed 字段值达到该值时, 就会触发 Leader 节点发送一条心跳消息.
	heartbeatTimeout int
	// 选举超时时间, 当 electionElapsed 字段达到该值时, 就会触发新一轮的选举.
	electionTimeout  int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	//
	// 该字段是 [electiontimeout, 2 * electiontimeout - 1] 之间的随机值, 也是选举计时器的上限, 当 electionElapsed
	// 超过该值时即为超时.
	randomizedElectionTimeout int
	disableProposalForwarding bool

	// 当前节点推进逻辑时钟的函数. 如果当前节点是 Leader, 则指向 raft.tickHeartbeat 函数, 如果当前节点是
	// Follower 或是 Candidate, 则指向 raft.tickElection() 函数.
	tick func()
	// 当前节点收到消息时的处理函数. 如果是 Leader 节点, 则该字段指向 stepLeader() 函数, 如果是 Follower 节点,
	// 则该字段指向 stepFollower() 函数, 如果是处于 preVote 节点的节点或是 Candidate 节点, 则该字段指向
	// stepCandidate() 函数.
	step stepFunc

	logger Logger
}

// newRaft 根据传入的 Config 实例中携带的参数创建 raft 实例并初始化 raft 使用到的相关组件
func newRaft(c *Config) *raft {
	// 检查参数 Config 中各字段的合法性
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// 创建 raftLog 实例, 用于记录 Entry 记录
	raftlog := newLogWithSize(c.Storage, c.Logger, c.MaxCommittedSizePerReady)
	// 获取 raftLog.storage 的初始状态（HardState 和 ConfState）
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}

	// 多节点情况下, 记录其他几个节点的 ID
	if len(c.peers) > 0 || len(c.learners) > 0 {
		if len(cs.Voters) > 0 || len(cs.Learners) > 0 {
			// TODO(bdarnell): the peers argument is always nil except in
			// tests; the argument should be removed and these tests should be
			// updated to specify their nodes through a snapshot.
			panic("cannot specify both newRaft(peers, learners) and ConfState.(Voters, Learners)")
		}
		cs.Voters = c.peers
		cs.Learners = c.learners
	}

	// 创建 raft 实例
	r := &raft{
		id:                        c.ID,
		lead:                      None, // 当前集群中 Leader 节点的 ID, 初始化时先被设置为 0
		isLearner:                 false,
		raftLog:                   raftlog,
		maxMsgSize:                c.MaxSizePerMsg,
		maxUncommittedSize:        c.MaxUncommittedEntriesSize,
		prs:                       tracker.MakeProgressTracker(c.MaxInflightMsgs),
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		logger:                    c.Logger,
		checkQuorum:               c.CheckQuorum,
		preVote:                   c.PreVote,
		readOnly:                  newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: c.DisableProposalForwarding,
	}

	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prs,
		LastIndex: raftlog.lastIndex(),
	}, cs)
	if err != nil {
		panic(err)
	}
	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))

	// 若 HardState 不为空, 则加载 HardState 记录的状态信息
	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	// 如果 Config 中配置 Applied, 即将 raftLog.applied 字段重置为指定的 Applied 值
	// 上层模块自己来控制正确的已应用位置时使用该配置
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	// 当前节点切换成 Follower 状态
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.prs.VoterNodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r
}

func (r *raft) hasLeader() bool { return r.lead != None }

func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

// send persists state to stable storage and then sends to its mailbox.
func (r *raft) send(m pb.Message) {
	// 设置消息的发送节点 ID, 即当前节点 ID
	m.From = r.id
	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		// 选举相关的消息的 Term 值必须不能为 0
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			// - MsgPreVote: m.Term is the term the node will campaign,
			//   non-zero as we use m.Term to indicate the next term we'll be
			//   campaigning for
			// - MsgPreVoteResp: m.Term is the term received in the original
			//   MsgPreVote if the pre-vote was granted, non-zero for the
			//   same reasons MsgPreVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		//
		// 除了 MsgProp 和 MsgReadIndex 两类消息（这两类消息的 Term 为 0, 即为本地消息）之外, 其他类型消息
		// 的 Term 字段值在这里统一设置.
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}
	// 将消息添加到 r.msgs 切片中等待上层模块将其发送出去
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer.
//
// sendAppend 主要负责向指定的目标节点发送 MsgApp 消息（或 MsgSnap 消息）, 在消息发送之前会检测当前节点的状态,
// 然后查找待发送的 Entry 记录并封装成 MsgApp 消息, 之后根据对应节点的 Progress.State 值决定发送消息之后的操作.
//
// 注意: 如果查找不到待发送的 Entry 记录（即该 Follower 节点对应的 Progress.Next 指定的 Entry 记录）, 则会尝试
// 通过 MsgSnap 消息将快照数据发送到 Follower 节点, Follower 节点之后会通过快照数据恢复其自身状态, 从而可以与
// Leader 节点进行正常的 Entry 记录复制. 如, 当 Follower 节点宕机时间较长时, 就可能出现这种情况.
func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

// maybeSendAppend sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The sendIfEmpty
// argument controls whether messages with no entries will be sent
// ("empty" messages are useful to convey updated Commit indexes, but
// are undesirable when we're sending multiple messages in a batch).
func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	// 获取待发送消息的目标节点的 Progress 实例
	pr := r.prs.Progress[to]
	// 检测当前节点是否可以向目标节点发送消息
	if pr.IsPaused() {
		return false
	}
	// 创建待发送的消息
	m := pb.Message{}
	// 设置目标节点的 ID
	m.To = to

	// 根据当前 Leader 节点记录的 Next 查找发往指定节点的 Entry 记录（ents）及 Next 索引对应的记录的 Term 值（term）
	term, errt := r.raftLog.term(pr.Next - 1)
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	// 上述两次 raftLog 查找出现异常时, 就会形成 MsgSnap 消息, 将快照数据发送到指定节点
	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		// 从当前的 Leader 节点来看, 目标 Follower 节点已经不再存活了
		if !pr.RecentActive {
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return false
		}

		// 将消息类型设置成 MsgSnap, 为后续发送快照数据做准备
		m.Type = pb.MsgSnap
		// 获取快照数据
		snapshot, err := r.raftLog.snapshot()
		// 若获取快照数据出现异常, 则终止整个程序
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err) // TODO(bdarnell)
		}
		// 若快照数据为空, 则终止整个程序
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		// 设置 MsgSnap 消息的 Snapshot 字段
		m.Snapshot = snapshot
		// 获取快照的相关信息
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		// 将目标节点对应的 Progress 切换成 StateSnapshot 状态
		pr.BecomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		// 设置消息的类型为 MsgApp
		m.Type = pb.MsgApp
		// 对于 MsgApp 消息, 该 Index 字段记录了其携带的 Entry 记录（即 Entries）中前一条记录的 Index 值
		m.Index = pr.Next - 1
		m.LogTerm = term // 设置为当前消息携带的 Entry 记录中上一个 Entry 记录的 Term 值
		m.Entries = ents // 设置消息携带的 Entry 记录集合
		m.Commit = r.raftLog.committed // 设置消息的 Commit 字段, 即当前节点的 raftLog 中最后一条已提交的记录索引值
		if n := len(m.Entries); n != 0 {
			// 根据目标节点对应的 Progress.State 状态决定其发送消息后的行为
			switch pr.State {
			// optimistically increase the next when in StateReplicate
			case tracker.StateReplicate:
				// 获取待发送 Entry 记录的最后一条 Entry 记录的索引值
				last := m.Entries[n-1].Index
				// 更新目标节点对应的 Next 值（这里不会更新 Match 值）
				pr.OptimisticUpdate(last)
				// 记录已发送但是未收到响应的消息
				pr.Inflights.Add(last)
			case tracker.StateProbe:
				// 此状态下, 消息发送后, 就将 Progress.ProbeSent 字段设置为 true, 暂定后续消息的发送
				pr.ProbeSent = true
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	// 发送前面创建的 MsgApp 消息, raft.send() 方法会设置 MsgApp 消息的 Term 字段值,
	// 并将其追加到 raft.msgs 中等待发送.
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	//
	// 注意 MsgHeartbeat 消息中 Commit 字段的设置, 这主要是因为在发送该 MsgHeartbeat 消息时,
	// Follower 节点并不一定已经收到了全部已提交的 Entry 记录.
	commit := min(r.prs.Progress[to].Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}

	r.send(m)
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
//
// bcastAppend 主要负责向集群中其他节点广播 MsgApp 消息（或 MsgSnap 消息）
func (r *raft) bcastAppend() {
	// 遍历向所有节点发送消息（除自己外）
	r.prs.Visit(func(id uint64, _ *tracker.Progress) {
		// 跳过当前节点本身, 只向集群中其他节点发送消息
		if id == r.id {
			return
		}
		// 向指定节点发送消息
		r.sendAppend(id)
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
//
// bcastHeartbeat 向集群中其他节点发送 MsgHeartbeat 心跳消息
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

// bcastHeartbeatWithCtx 向当前集群中所有节点发送心跳消息
func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.prs.Visit(func(id uint64, _ *tracker.Progress) {
		if id == r.id { // 略过当前节点自身（Leader 节点）
			return
		}
		// 向指定的节点发送 MsgHeartbeat 消息
		r.sendHeartbeat(id, ctx)
	})
}

func (r *raft) advance(rd Ready) {
	// If entries were applied (or a snapshot), update our cursor for
	// the next Ready. Note that if the current HardState contains a
	// new Commit index, this does not mean that we're also applying
	// all of the new entries due to commit pagination by size.
	if index := rd.appliedCursor(); index > 0 {
		// 更新 raftLog.applied 字段（即已应用的记录位置）
		r.raftLog.appliedTo(index)
		if r.prs.Config.AutoLeave && index >= r.pendingConfIndex && r.state == StateLeader {
			// If the current (and most recent, at least for this leader's term)
			// configuration should be auto-left, initiate that now.
			ccdata, err := (&pb.ConfChangeV2{}).Marshal()
			if err != nil {
				panic(err)
			}
			ent := pb.Entry{
				Type: pb.EntryConfChangeV2,
				Data: ccdata,
			}
			if !r.appendEntry(ent) {
				// If we could not append the entry, bump the pending conf index
				// so that we'll try again later.
				//
				// TODO(tbg): test this case.
				r.pendingConfIndex = r.raftLog.lastIndex()
			} else {
				r.logger.Infof("initiating automatic transition out of joint configuration %s", r.prs.Config)
			}
		}
	}
	// 已提交待应用的 Entry 记录被上层模块持久化后, 这里 raft 中未应用的总字节数减去这些已应用的 Entry 记录总字节数
	r.reduceUncommittedSize(rd.CommittedEntries)

	// Ready.Entries 中的记录已经被持久化了, 这里会将 unstable 中的对应记录清理掉, 并更新其 offset,
	// 也可能缩小 unstable 底层保存 Entry 记录的数组.
	if len(rd.Entries) > 0 {
		// 获取最后一个 Entry 记录
		e := rd.Entries[len(rd.Entries)-1]
		r.raftLog.stableTo(e.Index, e.Term)
	}
	// Ready 中封装的快照数据已经被持久化, 这里会清空 unstable 中记录的快照数据
	if !IsEmptySnap(rd.Snapshot) {
		r.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
//
// maybeCommit 如果该 Entry 记录已经复制到了半数以上的节点中, 则该方法中会尝试提交该 Entry 记录.
func (r *raft) maybeCommit() bool {
	// 返回集群中半数节点已提交的 Entry 记录的最大索引值
	// 举个例子:
	// 如果集群节点数为 5, 半数节点数为 3, 那么这里将返回第 3 个节点对应的 Match 值（这些节点的 Match 值都是按从大到小排序的）
	mci := r.prs.Committed()
	// 尝试更新 raftLog.commited 字段, 若没有超过半数节点都已提交该 Entry, 则更新失败, 返回 false
	return r.raftLog.maybeCommit(mci, r.Term)
}

func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	// 清空 lead 字段
	r.lead = None

	// 重置选举计时器和心跳计时器
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// 重置选举计时器的过期时间（随机值）
	r.resetRandomizedElectionTimeout()

	// 清空 leadTransferee 字段
	r.abortLeaderTransfer()

	r.prs.ResetVotes()
	// 重置 prs, 其中每个 Progress 中的 Next 设置为 raftLog.lastIndex + 1
	r.prs.Visit(func(id uint64, pr *tracker.Progress) {
		*pr = tracker.Progress{
			Match:     0,
			Next:      r.raftLog.lastIndex() + 1, // 下一个待复制的 Entry 记录的索引值
			Inflights: tracker.NewInflights(r.prs.MaxInflight),
			IsLearner: pr.IsLearner,
		}
		// 当前节点的 Match 设置为 raftLog.lastIndex
		if id == r.id {
			pr.Match = r.raftLog.lastIndex() // 当前已成功复制的 Entry 记录的索引值
		}
	})

	r.pendingConfIndex = 0
	r.uncommittedSize = 0
	r.readOnly = newReadOnly(r.readOnly.option)
}

func (r *raft) appendEntry(es ...pb.Entry) (accepted bool) {
	// 获取 raftLog 中最后一条 Entry 记录的索引值
	li := r.raftLog.lastIndex()
	// 设置待追加 Entry 记录的 Term 和 Index 值
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	// Track the size of this uncommitted proposal.
	//
	// 更新当前 raftLog 中未提交 Entry 记录的总字节数, 返回 true 表示允许追加
	if !r.increaseUncommittedSize(es) {
		r.logger.Debugf(
			"%x appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
			r.id,
		)
		// Drop the proposal.
		return false
	}
	// use latest "last" index after truncate/append
	//
	// 向 raftLog 中追加记录（这里实际先追加到 raftLog 的 unstable 中）
	li = r.raftLog.append(es...)
	// 更新当前节点对应的 Progress, 主要更新 Next 和 Match
	r.prs.Progress[r.id].MaybeUpdate(li)
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	//
	// 尝试提交 Entry 记录, 即修改 raftLog.committed 字段
	r.maybeCommit()
	return true
}

// tickElection is run by followers and candidates after r.electionTimeout.
//
// tickElection 当节点变为 Follower 状态后, 会周期性调用该方法推进 electionElapsed 并检测是否超时
func (r *raft) tickElection() {
	// 递增 electionElapsed
	r.electionElapsed++

	// promotable() 方法会检查 prs 字段中是否还存在当前节点对应的 Progress 实例, 这是为了检测当前节点是否被从集群中移除了
	// pastElectionTimeout() 方法是检测当前的选举计时器是否超时
	//
	// 当当前节点未被从集群中移除且选举计时器超时了, 则发起选举
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		// 集群启动一段时间后, 会有一个 Follower 节点的选举计时器超时, 此时就会创建 MsgHup 消息（其中 Term 为 0）并调用
		// raft.Step 方法.
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	}
}

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
func (r *raft) tickHeartbeat() {
	// 递增心跳计时器
	r.heartbeatElapsed++
	// 递增选举计时器
	r.electionElapsed++

	// 选举计时器超时
	if r.electionElapsed >= r.electionTimeout {
		// 重置选举计时器, Leader 节点不会主动发起选举
		r.electionElapsed = 0
		// 进行多数检测
		if r.checkQuorum {
			// 当选举计时器超过 electionTimeout 时, 会触发一次 checkQuorum 操作,
			// 该操作并不会发送网络消息（虽然这里通过 raft.Step 方法处理消息）
			// 它只是检测当前节点是否与集群中的大部分节点连通.
			r.Step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum})
		}
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		//
		// 选举计时器处于 electionTimeout~randomizedElectionTimeout 时间段时, 不能进行 Leader 节点转移.
		if r.state == StateLeader && r.leadTransferee != None {
			// 清空 raft.leadTransferee 字段, 放弃转移
			r.abortLeaderTransfer()
		}
	}

	// 检测当前节点是否为 Leader 节点
	if r.state != StateLeader {
		return
	}

	// 心跳计时器超时
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		// 重置心跳计时器
		r.heartbeatElapsed = 0
		// 发送 MsgBeat 消息
		r.Step(pb.Message{From: r.id, Type: pb.MsgBeat})
	}
}

// becomeFollower 将当前节点切换成 Follower 状态
func (r *raft) becomeFollower(term uint64, lead uint64) {
	// 将 step 字段设置成 stepFollower 回调函数, 该函数中封装了 Follower 节点处理消息的行为
	r.step = stepFollower
	// 重置 raft 实例 Term、Vote 等字段
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	// 设置当前节点的角色为 Follower
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate 当 PreCandidate 节点可以连接到集群中半数以上的节点时, 会调用 becomeCandidate 方法切换到 Candidate 状态.
func (r *raft) becomeCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	//
	// 检测当前节点的状态, 禁止直接从 Leader 状态切换到 Candidate 状态
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	// 将 step 设置成 stepCandidate 回调函数, 该函数封装了 Candidate 节点处理消息的行为
	r.step = stepCandidate
	// 递增当前任期号, 并重置其他字段
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	// 在此次选举中, Candidate 节点会将选票投给自己
	r.Vote = r.id
	// 修改当前节点的角色为 Candidate
	r.state = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomePreCandidate 如果当前集群开启了 PreVote 模式, 当 Follower 节点的选举计时器超时时, 会先调用 becomePreCandidate()
// 方法切换到 PreCandidate 状态.
func (r *raft) becomePreCandidate() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	//
	// 检测当前节点的状态, 禁止直接从 Leader 状态切换到 PreCandidate 状态
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	// Becoming a pre-candidate changes our step functions and state,
	// but doesn't change anything else. In particular it does not increase
	// r.Term or change r.Vote.
	//
	// 将 step 字段设置成 stepCandidate 回调函数, 该函数中封装了 PreCandidate 节点处理消息的行为.
	// 注意, 仅更改 step 和 state, 不要更改其他字段, 尤其是不要递增当前节点的任期号 Term
	r.step = stepCandidate
	r.prs.ResetVotes()
	r.tick = r.tickElection
	// 重置 lead 字段
	r.lead = None
	r.state = StatePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

// becomeLeader 当 Candidate 节点得到集群半数以上节点的选票时, 会调用该方法切换成 Leader 状态.
func (r *raft) becomeLeader() {
	// TODO(xiangli) remove the panic when the raft implementation is stable
	//
	// 检测当前节点的状态, 禁止直接从 Follower 状态切换到 Leader 状态
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	// 将 step 字段设置成 stepLeader, 该函数中封装了 Leader 节点处理消息的行为
	r.step = stepLeader
	// 重置 raft 实例的状态, 如重置选举和心跳超时时间
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	// 将 lead 字段设置为当前节点的 id
	r.lead = r.id
	// 更新当前节点的角色为 Leader
	r.state = StateLeader
	// Followers enter replicate mode when they've been successfully probed
	// (perhaps after having received a snapshot as a result). The leader is
	// trivially in this state. Note that r.reset() has initialized this
	// progress with the last index already.
	//
	// Followers 成功探测后（可能是在收到快照后）将进入复制模式. Leader 处于该状态.
	// 注意 r.reset() 已使用 raftLog 中最后一个 Entry 的索引值初始化此 Progress.
	r.prs.Progress[r.id].BecomeReplicate() // Leader 节点将进入复制模式

	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	//
	// 将 pendingConfIndex 字段设置为 raftLog 中最后一个 Entry 的索引值. 可能有也可能没有 pending 的配置更改,
	// 但可放心的推迟任何将来的更改, 直到我们提交所有 pending 的日志项为止.
	r.pendingConfIndex = r.raftLog.lastIndex()

	// 向当前 Leader 节点的 raftLog 中追加一条空的 Entry 记录（该记录接下来需要复制该其他 Follower 节点）
	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		// This won't happen because we just called reset() above.
		r.logger.Panic("empty entry was dropped")
	}
	// As a special case, don't count the initial empty entry towards the
	// uncommitted log quota. This is because we want to preserve the
	// behavior of allowing one entry larger than quota if the current
	// usage is zero.
	r.reduceUncommittedSize([]pb.Entry{emptyEnt})
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
//
// campaign 切换当前节点的角色为 PreCandidate 或 Candidate, 并向集群中的其他节点发送相应类型的消息
func (r *raft) campaign(t CampaignType) {
	if !r.promotable() {
		// This path should not be hit (callers are supposed to check), but
		// better safe than sorry.
		r.logger.Warningf("%x is unpromotable; campaign() should have been called", r.id)
	}
	// 在该方法的最后, 会发送一条消息, 这里 term 和 voteMsg 分别用于确定该消息的 Term 值和类型
	var term uint64
	var voteMsg pb.MessageType
	// 若集群开启了 PreVote, 则将当前节点由 Follower 切换到 PreCandidate 状态
	if t == campaignPreElection {
		/// 将当前节点切换成 PreCandidate 状态
		r.becomePreCandidate()
		// 确定发送的消息类型是 MsgPreVote 类型
		voteMsg = pb.MsgPreVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		// 确定最后发送消息的 Term 值, 这里只是增加了消息的 Term 值, 并未增加 raft.Term 字段的值
		term = r.Term + 1
	} else {
		// 切换为 Candidate 状态, 这里会递增 raft.Term 值, 并将选票投给自己
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}
	// 统计当前节点收到的选票, 并统计其得票数是否超过半数, 这次检测主要是为单节点设置的
	if _, _, res := r.poll(r.id, voteRespMsgType(voteMsg), true); res == quorum.VoteWon {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		//
		// 当得到足够的选票时, 则将 PreCandidate 状态的节点切换为 Candidate 状态, Candidate 状态的节点切换为 Leader 状态.
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}
	// 下面是集群多节点情况下的处理逻辑.

	// 状态切换完成之后, 当前节点会向集群中所有节点发送指定类型的消息
	var ids []uint64
	{
		// 对所有节点的 ID 进行由小到大的排序
		idMap := r.prs.Voters.IDs()
		ids = make([]uint64, 0, len(idMap))
		for id := range idMap {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	for _, id := range ids {
		// 跳过当前节点自身
		if id == r.id {
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		// 在进行 Leader 节点转移时, MsgPreVote 或 MsgVote 消息会在 Context 字段中设置该特殊值,
		// 而其他节点在处理 MsgVote 消息时, 会因为该标识立即参与此次选举.
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		// 发送指定类型的消息, 其中 Index 和 LogTerm 分别是当前节点的 raftLog 中最后一条消息的 Index 值和 Term 值
		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
	}
}

// poll 记录收到的投票结果并统计收到的各节点的投票结果
func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result quorum.VoteResult) {
	// 为 true 表示接收到赞同票
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	// 记录接收到的对应节点的投票结果
	r.prs.RecordVote(id, v)
	// 统计收到的各节点的投票结果
	return r.prs.TallyVotes()
}

// Step 是 etcd-raft 模块处理各类消息的入口.
func (r *raft) Step(m pb.Message) error {
	// Handle the message term, which may result in our stepping down to a follower.
	//
	// 根据 Term 值对消息进行分类处理
	switch {
	case m.Term == 0: // 消息的 Term 为 0 则表示为本地消息, 这里不进行处理
		// local message
	case m.Term > r.Term:
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			// 根据消息的 Context 字段判断收到的 MsgPreVote（或 MsgVote）消息是否为 Leader 节点
			// 转移场景下产生的, 如果是, 则强制当前节点参与本次预选（或选举）
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			// 通过一系列条件判断当前节点是否参与此次选举（或预选）, 其中主要检测集群是否开启了 CheckQuorum 模式、
			// 当前节点是否有已知的 Lead 节点, 以及其选举计时器的时间
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				// 当前节点不参与此次选举
				return nil
			}
		}
		switch {
		case m.Type == pb.MsgPreVote: // 收到 MsgPreVote 消息时, 不会引起当前节点的状态切换
			// Never change our term in response to a PreVote
		case m.Type == pb.MsgPreVoteResp && !m.Reject:
			// We send pre-vote requests with a term in our future. If the
			// pre-vote is granted, we will increment our term when we get a
			// quorum. If it is not, the term comes from the node that
			// rejected our vote so we should become a follower at the new
			// term.
		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}

	case m.Term < r.Term:
		if (r.checkQuorum || r.preVote) && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			// We have received messages from a leader at a lower term. It is possible
			// that these messages were simply delayed in the network, but this could
			// also mean that this node has advanced its term number during a network
			// partition, and it is now unable to either win an election or to rejoin
			// the majority on the old term. If checkQuorum is false, this will be
			// handled by incrementing term numbers in response to MsgVote with a
			// higher term, but if checkQuorum is true we may not advance the term on
			// MsgVote and must generate other messages to advance the term. The net
			// result of these two features is to minimize the disruption caused by
			// nodes that have been removed from the cluster's configuration: a
			// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
			// but it will not receive MsgApp or MsgHeartbeat, so it will not create
			// disruptive term increases, by notifying leader of this node's activeness.
			// The above comments also true for Pre-Vote
			//
			// When follower gets isolated, it soon starts an election ending
			// up with a higher term than leader, although it won't receive enough
			// votes to win the election. When it regains connectivity, this response
			// with "pb.MsgAppResp" of higher term would force leader to step down.
			// However, this disruption is inevitable to free this stuck node with
			// fresh election. This can be prevented with Pre-Vote phase.
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else if m.Type == pb.MsgPreVote {
			// Before Pre-Vote enable, there may have candidate with higher term,
			// but less log. After update to Pre-Vote, the cluster may deadlock if
			// we drop messages with a lower term.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, Type: pb.MsgPreVoteResp, Reject: true})
		} else {
			// ignore other cases
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		return nil
	}

	// 根据消息的 Type 进行分类处理
	switch m.Type {
	case pb.MsgHup:
		// 只有非 Leader 状态的节点才会处理 MsgHup 消息
		if r.state != StateLeader {
			// 检测当前节点是否还在集群中
			if !r.promotable() {
				r.logger.Warningf("%x is unpromotable and can not campaign; ignoring MsgHup", r.id)
				return nil
			}
			// 获取 raftLog 中已提交但未应用（即 applied~committed）的 Entry 记录
			ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
			if err != nil {
				r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			// 检测是否有未应用的 EntryConfChange 记录, 如果有就放弃发起选举的机会
			if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
				r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
			// 若当前集群开启了 PreVote 模式, 则先切换到 PreCandidate 状态
			if r.preVote {
				// 调用该方法切换当前节点的角色, 并发起 PreVote
				r.campaign(campaignPreElection)
			} else {
				r.campaign(campaignElection)
			}
		} else { // 如果当前节点是 Leader 状态, 则仅仅输出一条 debug 日志
			r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		}

	case pb.MsgVote, pb.MsgPreVote:
		// We can vote if this is a repeat of a vote we've already cast...
		// 当前节点在参与预选时, 会综合下面几个条件决定是否投票：
		// 1. 当前节点投票给了对方节点
		// 2. 当前节点是否已经投过票
		// 3. MsgPreVote 消息发送者的任期号是否更大
		canVote := r.Vote == m.From ||
			// ...we haven't voted and we don't think there's a leader yet in this term...
			(r.Vote == None && r.lead == None) ||
			// ...or this is a PreVote for a future term...
			(m.Type == pb.MsgPreVote && m.Term > r.Term)
		// ...and we believe the candidate is up to date.
		// MsgVote 或 MsgPreVote 消息发送者的 raftLog 中是否包含当前节点的全部 Entry 记录
		if canVote && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			// Note: it turns out that that learners must be allowed to cast votes.
			// This seems counter- intuitive but is necessary in the situation in which
			// a learner has been promoted (i.e. is now a voter) but has not learned
			// about this yet.
			// For example, consider a group in which id=1 is a learner and id=2 and
			// id=3 are voters. A configuration change promoting 1 can be committed on
			// the quorum `{2,3}` without the config change being appended to the
			// learner's log. If the leader (say 2) fails, there are de facto two
			// voters remaining. Only 3 can win an election (due to its log containing
			// all committed entries), but to do so it will need 1 to vote. But 1
			// considers itself a learner and will continue to do so until 3 has
			// stepped up as leader, replicates the conf change to 1, and 1 applies it.
			// Ultimately, by receiving a request to vote, the learner realizes that
			// the candidate believes it to be a voter, and that it should act
			// accordingly. The candidate's config may be stale, too; but in that case
			// it won't win the election, at least in the absence of the bug discussed
			// in:
			// https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			// When responding to Msg{Pre,}Vote messages we include the term
			// from the message, not the local term. To see why, consider the
			// case where a single node was previously partitioned away and
			// it's local term is now out of date. If we include the local term
			// (recall that for pre-votes we don't update the local term), the
			// (pre-)campaigning node on the other end will proceed to ignore
			// the message (it ignores all out of date messages).
			// The term in the original message and current local term are the
			// same in the case of regular votes, but different for pre-votes.
			//
			// 将票投给 MsgPreVote 或 MsgVote 消息的发送者
			r.send(pb.Message{To: m.From, Term: m.Term, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				// Only record real votes.
				r.electionElapsed = 0
				// 记录当前节点将票投给的指定节点
				r.Vote = m.From
			}
		} else {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			// 不满足上述投赞同票时, 当前节点会返回拒绝票（即响应消息中的 Reject 字段会设置成 true）
			r.send(pb.Message{To: m.From, Term: r.Term, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		err := r.step(r, m)
		if err != nil {
			return err
		}
	}
	return nil
}

type stepFunc func(r *raft, m pb.Message) error

func stepLeader(r *raft, m pb.Message) error {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case pb.MsgBeat:
		// 向所有节点发送心跳消息
		r.bcastHeartbeat()
		return nil
	case pb.MsgCheckQuorum:
		// 当 Leader 节点选举超时并开启了 checkQuorum 机制时, 会调用 raft.Step 处理 MsgCheckQuorum 消息,
		// 该消息是本地消息, 用于检测当前 Leader 节点是否与集群中大部分节点连通, 如果不连通, 则切换成 Follower 状态.
		//
		// The leader should always see itself as active. As a precaution, handle
		// the case in which the leader isn't in the configuration any more (for
		// example if it just removed itself).
		//
		// TODO(tbg): I added a TODO in removeNode, it doesn't seem that the
		// leader steps down when removing itself. I might be missing something.
		//
		// 获取当前节点（即 Leader）对应的 Progress 实例
		if pr := r.prs.Progress[r.id]; pr != nil {
			pr.RecentActive = true // 表示当前节点存活
		}
		// 检测当前 Leader 节点是否与集群中大部分节点连通, 如果不连通, 则切换成 Follower 状态
		if !r.prs.QuorumActive() {
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		// Mark everyone (but ourselves) as inactive in preparation for the next
		// CheckQuorum.
		//
		// 重置所有集群中所有节点（除自己）的 Progress.RecentActive 都为 false
		r.prs.Visit(func(id uint64, pr *tracker.Progress) {
			if id != r.id {
				pr.RecentActive = false
			}
		})
		return nil
	case pb.MsgProp:
		// Leader 节点处理客户端发来的（或 Follower 节点转发来的）写请求 MsgProp 消息

		// 检测 MsgProp 消息是否携带了 Entry 记录, 如果未携带, 则输出异常日志并终止程序
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp", r.id)
		}
		// 检测当前节点是否被移除集群, 如果当前节点以 Leader 状态被移出集群, 则不再处理 MsgProp 消息
		if r.prs.Progress[r.id] == nil {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		}
		// 检测当前节点是否正在进行 Leader 节点的转移, 是则不再处理 MsgProp 消息
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}

		// 遍历 MsgProp 消息携带的全部 Entry 记录
		for i := range m.Entries {
			e := &m.Entries[i]
			var cc pb.ConfChangeI
			// 检测是否存在 EntryConfChange 类型的 Entry 记录, 若存在则按类型解析数据
			if e.Type == pb.EntryConfChange {
				var ccc pb.ConfChange
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			} else if e.Type == pb.EntryConfChangeV2 {
				var ccc pb.ConfChangeV2
				if err := ccc.Unmarshal(e.Data); err != nil {
					panic(err)
				}
				cc = ccc
			}
			// 对 EntryConfChange 类型的消息的处理
			if cc != nil {
				alreadyPending := r.pendingConfIndex > r.raftLog.applied
				alreadyJoint := len(r.prs.Config.Voters[1]) > 0
				wantsLeaveJoint := len(cc.AsV2().Changes) == 0

				var refused string
				if alreadyPending {
					refused = fmt.Sprintf("possible unapplied conf change at index %d (applied to %d)", r.pendingConfIndex, r.raftLog.applied)
				} else if alreadyJoint && !wantsLeaveJoint {
					refused = "must transition out of joint config first"
				} else if !alreadyJoint && wantsLeaveJoint {
					refused = "not in joint state; refusing empty conf change"
				}

				if refused != "" {
					r.logger.Infof("%x ignoring conf change %v at config %s: %s", r.id, cc, r.prs.Config, refused)
					// 如果存在多条 EntryConfChange 类型的记录, 则只保留第一条
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				} else {
					r.pendingConfIndex = r.raftLog.lastIndex() + uint64(i) + 1
				}
			}
		}

		// 将携带的 Entry 记录追加到当前节点的 raftLog 中
		if !r.appendEntry(m.Entries...) {
			return ErrProposalDropped
		}
		// 通过 MsgApp 消息向集群中其他节点复制 Entry 记录
		r.bcastAppend()
		return nil

	// 处理客户端的线性读请求
	case pb.MsgReadIndex:
		// If more than the local vote is needed, go through a full broadcast,
		// otherwise optimize.
		//
		// 集群场景
		if !r.prs.IsSingleton() {
			// Leader 节点检测自身在当前任期中是否已提交过 Entry 记录, 如果没有, 则无法进行读取操作, 直接返回
			if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term {
				// Reject read only request when this leader has not committed any log entry at its term.
				return nil
			}

			// thinking: use an interally defined context instead of the user given context.
			// We can express this in terms of the term and index instead of a user-supplied value.
			// This would allow multiple reads to piggyback on the same message.
			//
			// 处理读请求的模式
			switch r.readOnly.option {
			case ReadOnlySafe:
				// 记录当前节点的 raftLog.committed 字段值, 即已提交的位置
				r.readOnly.addRequest(r.raftLog.committed, m)
				// The local node automatically acks the request.
				r.readOnly.recvAck(r.id, m.Entries[0].Data)
				// 发送心跳, 传入参数是 MsgReadIndex 消息的消息 ID
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)
			case ReadOnlyLeaseBased:
				ri := r.raftLog.committed
				// 检测该 MsgReadIndex 消息是否为客户端直接发送到 Leader 节点
				if m.From == None || m.From == r.id { // from local member
					// 根据 MsgReadIndex 消息的 From 字段, 判断该 MsgReadIndex 消息是否为 Follower 节点转发到
					// Leader 节点的消息.
					// 如果是客户端直接发送到 Leader 节点的消息, 则将 MsgReadIndex 消息对应的已提交位置
					// 以及其消息 ID 封装成 ReadState 实例, 添加到 raft.readStates 中保存. 后续会有其他 goroutine
					// 读取该数组, 并对相应的 MsgReadIndex 消息进行响应.
					r.readStates = append(r.readStates, ReadState{Index: ri, RequestCtx: m.Entries[0].Data})
				} else {
					// 如果是其他 Follower 节点转发到 Leader 节点的 MsgReadIndex 消息, 则 Leader 节点会向 Follower
					// 节点返回相应的 MsgReadIndexResp 消息, 并由 Follower 节点响应 client.
					r.send(pb.Message{To: m.From, Type: pb.MsgReadIndexResp, Index: ri, Entries: m.Entries})
				}
			}
		} else { // only one voting member (the leader) in the cluster: 单节点的情况
			if m.From == None || m.From == r.id { // from leader itself
				r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
			} else { // from learner member
				r.send(pb.Message{To: m.From, Type: pb.MsgReadIndexResp, Index: r.raftLog.committed, Entries: m.Entries})
			}
		}

		return nil
	}

	// All other message types require a progress for m.From (pr).
	//
	// 根据消息的 From 字段获取对应的 Progress 实例, 为后面的消息处理做准备.
	pr := r.prs.Progress[m.From]
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.Type {
	case pb.MsgAppResp:
		// 更新对应 Progress 实例的 RecentActive 字段, 从 Leader 角度来看, MsgAppResp 消息的发送节点还是存活的.
		pr.RecentActive = true

		// MsgApp 消息被拒绝了
		if m.Reject {
			r.logger.Debugf("%x received MsgAppResp(MsgApp was rejected, lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			// 通过 MsgAppResp 消息携带的信息及对应的 Progress 状态, 重新设置其 Next 值
			if pr.MaybeDecrTo(m.Index, m.RejectHint) {
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				// 如果对应的 Progress 处于 StateReplicate 状态, 则切换成 StateProbe 状态, 试探 Follower 的匹配位置
				// (这里的试探是指发送一条消息并等待其响应后, 再发送后续的消息)
				if pr.State == tracker.StateReplicate {
					pr.BecomeProbe()
				}
				// 再次向对应 Follower 节点发送 MsgApp 消息, 在该方法中会将 Progress.ProbeSent 设置为 true, 从而暂停
				// 后续消息的发送, 从而实现 "试探" 效果.
				r.sendAppend(m.From)
			}
		} else { // 之前发送的 MsgApp 消息已经被对应的 Follower 节点接收（Entry 记录被成功追加）
			oldPaused := pr.IsPaused()
			// MsgAppResp 消息的 Index 字段是对应 Follower 节点 raftLog 中最后一条 Entry 记录的索引, 这里会根据
			// 该值更新对其对应 Progress 实例的 Match 和 Next.
			if pr.MaybeUpdate(m.Index) {
				switch {
				case pr.State == tracker.StateProbe:
					// 一旦 MsgApp 被 Follower 节点接收, 则表示已经找到其正确的 Next 和 Match,
					// 不必再进行 "试探", 这里将对应 Progress.state 切换成 StateReplicate.
					pr.BecomeReplicate()
				case pr.State == tracker.StateSnapshot && pr.Match >= pr.PendingSnapshot:
					// TODO(tbg): we should also enter this branch if a snapshot is
					// received that is below pr.PendingSnapshot but which makes it
					// possible to use the log again.
					r.logger.Debugf("%x recovered from needing snapshot, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					// Transition back to replicating state via probing state
					// (which takes the snapshot into account). If we didn't
					// move to replicating state, that would only happen with
					// the next round of appends (but there may not be a next
					// round for a while, exposing an inconsistent RaftStatus).
					//
					// 之前由于某些原因, Leader 节点通过发送快照的方式恢复 Follower 节点, 但在发送
					// MsgSnap 消息的过程中, Follower 节点恢复, 并正常接收了 Leader 节点的 MsgApp 消
					// 息, 此时会丢弃 MsgApp 消息, 并开始 "试探" 该 Follower节点正确的 Match 和 Next 值.
					pr.BecomeProbe()
					pr.BecomeReplicate()
				case pr.State == tracker.StateReplicate:
					// 之前向某个 Follower 节点发送 MsgApp 消息时, 会将其相关信息保存到对应的
					// Progress.Inflights 中, 在这里收到相应的 MsgAppResp 相应之后, 会将其从 Inflights
					// 中删除, 这样可以实现了限流的效果, 避免网络出现延迟时, 继续发送消息, 从而导致网络
					// 更加拥堵.
					pr.Inflights.FreeLE(m.Index)
				}

				// 收到一个 Follower 节点的 MsgAppResp 消息后, 除了修改相应的 Match 和 Next 值, 还会尝试
				// 更新 raftLog.committed, 因为有些 Entry 记录可能在此次复制中被保存到了半数以上节点中,
				// 因此调用 raftLog.maybyCommit 方法尝试提交该 Entry 记录.
				if r.maybeCommit() {
					// 向所有节点发送 MsgApp 消息, 注意, 此次 MsgApp 消息的 Commit 字段与上次 MsgApp 消息
					// 已经不同
					r.bcastAppend()
				} else if oldPaused { // 之前是 pause 状态, 现在可以任性地发送消息了
					// If we were paused before, this node may be missing the
					// latest commit index, so send it.
					//
					// 之前 Leader 节点暂停向该 Follower 节点发送消息, 收到 MsgAppResp 消息后, 在上面的代码中
					// 已经重置了相应状态, 所以可以继续发送 MsgApp 消息.
					r.sendAppend(m.From)
				}
				// We've updated flow control information above, which may
				// allow us to send multiple (size-limited) in-flight messages
				// at once (such as when transitioning from probe to
				// replicate, or when freeTo() covers multiple messages). If
				// we have more entries to send, send as many messages as we
				// can (without sending empty messages for the commit index)
				for r.maybeSendAppend(m.From, false) {
				}
				// Transfer leadership is in progress.
				//
				// 当收到目标 Follower 节点的 MsgAppResp 消息并且两者的 raftLog 完全匹配,
				// 则发送 MsgTimeoutNow 消息.
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgHeartbeatResp:
		// 更新对应 Progress.RecentActive, 表示 Follower 节点与自己连通
		pr.RecentActive = true
		// 重置 Progress.ProbeSent 字段, 表示可以继续向 Follower 节点发送消息
		pr.ProbeSent = false

		// free one slot for the full inflights window to allow progress.
		//
		// 若该 Progress 实例对应的 Follower 节点处于 StateReplicate 状态且其 inflights 已满
		if pr.State == tracker.StateReplicate && pr.Inflights.Full() {
			// 释放 inflights 中第一个消息, 这样就可以开始后续消息的发送
			pr.Inflights.FreeFirstOne()
		}
		// 当 Leader 节点收到 Follower 节点的 MsgHeartbeat 消息后, 会比较对应的 Match 值与 Leader 节点的 raftLog,
		// 从而判断 Follower 节点是否已拥有全部的 Entry 记录.
		if pr.Match < r.raftLog.lastIndex() {
			// 通过向指定节点发送 MsgApp 消息完成 Entry 记录的复制.
			r.sendAppend(m.From)
		}

		// 下面只处理 ReadOnlySafe 模式下, 只读请求相关的 MsgHeartbeatResp 消息
		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return nil
		}

		// 记录接收到了对应节点的只读请求相关的心跳响应, 同时统计是否有超过半数以上的节点响应了携带消息 ID 的
		// MsgHeartbeatResp 消息.
		if r.prs.Voters.VoteResult(r.readOnly.recvAck(m.From, m.Context)) != quorum.VoteWon {
			return nil
		}

		// 响应节点超过半数后, 会清空 readOnly 中指定消息 ID 及其之前的所有相关记录.
		rss := r.readOnly.advance(m)
		for _, rs := range rss {
			// req 记录了对应的 MsgReadIndex 请求
			req := rs.req
			// 检测该 MsgReadIndex 消息是否为客户端直接发送到 Leader 节点
			if req.From == None || req.From == r.id { // from local member
				// 根据 MsgReadIndex 消息的 From 字段, 判断该 MsgReadIndex 消息是否为 Follower 节点转发到
				// Leader 节点的消息.
				// 如果是客户端直接发送到 Leader 节点的消息, 则将 MsgReadIndex 消息对应的已提交位置
				// 以及其消息 ID 封装成 ReadState 实例, 添加到 raft.readStates 中保存. 后续会有其他 goroutine
				// 读取该数组, 并对相应的 MsgReadIndex 消息进行响应.
				r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				// 如果是其他 Follower 节点转发到 Leader 节点的 MsgReadIndex 消息, 则 Leader 节点会向 Follower
				// 节点返回相应的 MsgReadIndexResp 消息, 并由 Follower 节点响应 client.
				r.send(pb.Message{To: req.From, Type: pb.MsgReadIndexResp, Index: rs.index, Entries: req.Entries})
			}
		}
	case pb.MsgSnapStatus:
		// 检测 Follower 节点对应状态是否为 StateSnapshot
		if pr.State != tracker.StateSnapshot {
			return nil
		}
		// TODO(tbg): this code is very similar to the snapshot handling in
		// MsgAppResp above. In fact, the code there is more correct than the
		// code here and should likely be updated to match (or even better, the
		// logic pulled into a newly created Progress state machine handler).
		//
		// 之前的发送 MsgSnap 消息时出现异常
		if !m.Reject {
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			// NB: the order here matters or we'll be probing erroneously from
			// the snapshot index, but the snapshot never applied.
			//
			// 发送 MsgSnap 消息失败, 这里清空 PendingSnapshot 字段
			pr.PendingSnapshot = 0
			pr.BecomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		// 无论 MsgSnap 消息发送是否失败, 都会将对应 Progress 切换成 StateProbe 状态, 之后单条发送消息进行试探

		// If snapshot finish, wait for the MsgAppResp from the remote node before sending
		// out the next MsgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		//
		// 暂停 Leader 节点向该 Follower 节点继续发送消息, 如果发送 MsgSnap 消息成功了, 则待 Leader 节点收到
		// 相应的响应消息（MsgAppResp 消息）, 即可继续发送后续 MsgApp 消息; 如果发送 MsgSnap 失败了, 则 Leader
		// 节点会等到收到 MsgHeartbeatResp 消息时, 才会重新开始发送后续消息.
		pr.ProbeSent = true
	case pb.MsgUnreachable:
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		//
		// 当 Follower 节点变得不可达时, 如果继续发送 MsgApp 消息, 则会有大量消息丢失
		if pr.State == tracker.StateReplicate {
			// 将 Follower 节点对应的 Progress 实例切换成 StateProbe 状态
			pr.BecomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
	case pb.MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignored transferring leadership", r.id)
			return nil
		}
		// 在 MsgTransferLeader 消息中, From 字段记录了此次 Leader 节点迁移操作的目标 Follower 节点 ID.
		leadTransferee := m.From
		// 检测当前是否有一次未处理完的 Leader 节点转移操作
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			// 目标节点相同, 则忽略此次 Leader 迁移操作
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				return nil
			}
			// 若目标节点不同, 则清空上次记录的 ID（即 raft.leadTransferee）
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		// 目标节点已经是 Leader 节点, 放弃此次迁移操作
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return nil
		}
		// Transfer leadership to third party.
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		//
		// Leader 迁移操作应该在 electionTimeout 时间内完成, 这里会重置选举计时器
		r.electionElapsed = 0
		// 记录此次 Leader 节点迁移的目标节点 ID
		r.leadTransferee = leadTransferee
		// 检测目标 Follower 节点是否与当前 Leader 节点的 raftLog 是否完全一致
		if pr.Match == r.raftLog.lastIndex() {
			// 向目标 Follower 节点发送 MsgTimeoutNow 消息, 这会导致 Follower 节点的选举计时器立即过期, 并发起新一轮选举
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			// 如果 raftLog 中的 Entry 记录没有完全匹配, 则 Leader 节点通过发送 MsgApp 消息向目标节点进行复制
			r.sendAppend(leadTransferee)
		}
	}
	return nil
}

// stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
// whether they respond to MsgVoteResp or MsgPreVoteResp.
//
// stepCandidate:
// 1. PreCandidate 节点收到 Follower 节点的 MsgPreVoteResp 消息时, 通过 raft.Step 最终将调用该方法进行处理;
// 2. Candidate 节点收到 Follower 节点的 MsgVoteResp 消息时, 通过 raft.Step 最终将调用该方法进行处理.
func stepCandidate(r *raft, m pb.Message) error {
	// Only handle vote responses corresponding to our candidacy (while in
	// StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	// our pre-candidate state).
	var myVoteRespType pb.MessageType
	// 根据当前节点的状态决定其能够处理的选举响应消息的类型
	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}
	switch m.Type {
	case pb.MsgProp:
		// 当 Candidate 节点接收到客户端发来的写请求 MsgProp 消息时, 直接忽略该消息
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MsgApp:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleSnapshot(m)
	case myVoteRespType: // 处理收到的选举响应消息
		// 记录并统计投票结果
		gr, rj, res := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.Type, rj)
		switch res {
		case quorum.VoteWon: // 得票数过半, 赢得了选票
			// 当 PreCandidate 节点在预选中收到半数以上的选票后, 会发起正式的选举
			if r.state == StatePreCandidate {
				r.campaign(campaignElection)
			} else {
				// 当前节点切换成 Leader 状态, 其中会重置每个节点对应的 Next 和 Match 两个索引.
				r.becomeLeader()
				// 向集群中其他节点广播 MsgApp 消息
				r.bcastAppend()
			}
		case quorum.VoteLost:
			// pb.MsgPreVoteResp contains future term of pre-candidate
			// m.Term > r.Term; reuse r.Term
			//
			// 无法获得半数以上的选票, 当前节点将切换为 Follower 状态, 等待下一轮的选举（或预选）
			r.becomeFollower(r.Term, None)
		}
	case pb.MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.state, m.From)
	}
	return nil
}

func stepFollower(r *raft, m pb.Message) error {
	switch m.Type {
	case pb.MsgProp:
		// 当 Follower 节点接收到了客户端的写请求 MsgProp 消息时, 会将该消息转发给 Leader 节点进行处理

		// 当前集群没有 Leader 节点, 则忽略该 MsgProp 消息
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		} else if r.disableProposalForwarding {
			r.logger.Infof("%x not forwarding to leader %x at term %d; dropping proposal", r.id, r.lead, r.Term)
			return ErrProposalDropped
		}
		// 将消息的 To 字段设置为当前 Leader 节点的 id
		m.To = r.lead
		// 将 MsgProp 消息发送到当前的 Leader 节点
		r.send(m)
	case pb.MsgApp:
		// 重置选举计时器, 防止当前 Follower 节点发起新一轮选举
		r.electionElapsed = 0
		// 设置 raft.Lead 记录, 保存当前集群的 Leader 节点 ID
		r.lead = m.From
		// 将 MsgApp 消息中携带的 Entry 记录追加到 raftLog 中, 并且向 Leader 节点发送 MsgAppResp
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		// 重置选举计时器, 防止当前 Follower 节点发起新一轮的选举
		r.electionElapsed = 0
		r.lead = m.From
		// 其中会修改 raft.committed 字段的值（注意, 在 Leader 节点发送消息时, 已经确定了当前 Follower 节点
		// 有 committed 之前的全部日志记录）, 然后发送 MsgHeartbeatResp 类型消息, 响应此次心跳.
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		// 重置选举计时器逻辑时钟, 防止发生选举
		r.electionElapsed = 0
		r.lead = m.From
		// 通过 MsgSnap 消息中的快照数据, 重建当前节点的 raftLog
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		// 当前任期没有 Leader 节点, 则不处理该消息
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		// Follower 节点直接将 MsgTransferLeader 消息转发给 Leader 节点
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		// 检测当前节点是否已被移出当前集群
		if r.promotable() {
			r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
			// Leadership transfers never use pre-vote even if r.preVote is true; we
			// know we are not recovering from a partition so there is no need for the
			// extra round trip.
			//
			// 即使当前集群开启了 PreVote 模式, 目标 Follower 节点也会直接发起新一轮选举.
			// TODO: 这里传入的参数 campaignTransfer 会被记录到 MsgVote 消息中并发送到集群中的其他节点, 而其他节点在处理
			//  MsgVote 消息时, 会因为该标识立即参与此次选举.
			r.campaign(campaignTransfer)
		} else {
			r.logger.Infof("%x received MsgTimeoutNow from %x but is not promotable", r.id, m.From)
		}
	case pb.MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return nil
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgReadIndexResp:
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return nil
		}
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
	}
	return nil
}

func (r *raft) handleAppendEntries(m pb.Message) {
	// m.Index 表示 Leader 发送给 Follower 的上一条日志的索引值,
	// 如果 Follower 节点在 Index 位置的 Entry 记录已经提交了,
	// 则不能进行追加操作, 因为在 Raft 协议中, 已提交的记录不能被覆盖,
	// 所以 Follower 节点会将其 committed 位置通过 MsgAppResp 消息（Index 字段）通知 Leader 节点.
	if m.Index < r.raftLog.committed {
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
		return
	}

	// 尝试将消息携带的 Entry 记录追加到 raftLog 中
	if mlastIndex, ok := r.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		// 如果追加成功, 则将最后一条记录的索引值通过 MsgAppResp 消息返回给 Leader 节点,
		// 这样 Leader 节点就可以根据此值更新其对应的 Next 和 Match 值.
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: mlastIndex})
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		// 如果追加记录失败, 则将失败信息返回给 Leader 节点（即 MsgAppResp 消息的 Reject 字段为 true）,
		// 同时返回的还有一些提示信息（RejectHint 字段保存当前节点 raftLog 中最后一条记录的索引）.
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: m.Index, Reject: true, RejectHint: r.raftLog.lastIndex()})
	}
}

func (r *raft) handleHeartbeat(m pb.Message) {
	// 根据 MsgHeartbeat 消息的 Commit 字段, 更新 raftLog 中记录的已提交位置, 注意, 在 Leader 节点发送 MsgHeartbeat 消息时,
	// 已经确定了当前 Follower 节点中 raftLog.committed 字段的合适位置.
	r.raftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, Type: pb.MsgHeartbeatResp, Context: m.Context})
}

// handleSnapshot 通过 MsgSnap 消息中的快照数据, 重建当前节点的 raftLog
func (r *raft) handleSnapshot(m pb.Message) {
	// 获取快照数据的元数据
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	// 返回值表示是否通过快照数据进行了重建
	if r.restore(m.Snapshot) {
		r.logger.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		// 向 Leader 节点返回 MsgAppResp 消息（Reject 始终为 false）. 该 MsgAppResp 消息作为 MsgSnap 消息的响应,
		// 与 MsgApp 消息并无差别.
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()})
	} else {
		r.logger.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, sindex, sterm)
		// 若没有通过快照数据进行重建, 则 Index 值为当前 raftLog 已提交的位置
		r.send(pb.Message{To: m.From, Type: pb.MsgAppResp, Index: r.raftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine. If this method returns false, the snapshot was
// ignored, either because it was obsolete or because of an error.
//
// restore 根据快照数据进行重建
func (r *raft) restore(s pb.Snapshot) bool {
	// 若快照中最后一个 Entry 记录的索引值比当前节点 raftLog 中已提交位置小, 则表示该快照数据是旧的, 因此直接返回, 不进行重建
	if s.Metadata.Index <= r.raftLog.committed {
		return false
	}
	// 仅 Follower 节点才会处理快照数据, 进行重建操作
	if r.state != StateFollower {
		// This is defense-in-depth: if the leader somehow ended up applying a
		// snapshot, it could move into a new term without moving into a
		// follower state. This should never fire, but if it did, we'd have
		// prevented damage by returning early, so log only a loud warning.
		//
		// At the time of writing, the instance is guaranteed to be in follower
		// state when this method is called.
		r.logger.Warningf("%x attempted to restore snapshot as leader; should never happen", r.id)
		// 切换为 Follower 节点, 任期加一
		r.becomeFollower(r.Term+1, None)
		return false
	}

	// More defense-in-depth: throw away snapshot if recipient is not in the
	// config. This shouuldn't ever happen (at the time of writing) but lots of
	// code here and there assumes that r.id is in the progress tracker.
	found := false
	cs := s.Metadata.ConfState
	for _, set := range [][]uint64{
		cs.Voters,
		cs.Learners,
	} {
		for _, id := range set {
			if id == r.id {
				found = true
				break
			}
		}
	}
	if !found {
		r.logger.Warningf(
			"%x attempted to restore snapshot but it is not in the ConfState %v; should never happen",
			r.id, cs,
		)
		return false
	}

	// Now go ahead and actually restore.

	// 根据快照数据的元数据查找匹配的 Entry 记录, 如果存在, 则表示当前节点已经拥有了
	// 该快照中的全部数据, 所以无须进行后续重建操作
	if r.raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
		// 快照中全部的 Entry 记录都已经提交了, 所以尝试修改当前节点的 raftLog.committed（raftLog.committed 只增不减）
		r.raftLog.commitTo(s.Metadata.Index)
		return false
	}

	// 通过 raftLog.unstable 记录该快照数据, 同时重置相关字段
	r.raftLog.restore(s)

	// Reset the configuration and add the (potentially updated) peers in anew.
	// 重置 prs
	r.prs = tracker.MakeProgressTracker(r.prs.MaxInflight)
	// 并根据快照的元数据进行重建
	cfg, prs, err := confchange.Restore(confchange.Changer{
		Tracker:   r.prs,
		LastIndex: r.raftLog.lastIndex(),
	}, cs)

	if err != nil {
		// This should never happen. Either there's a bug in our config change
		// handling or the client corrupted the conf change.
		panic(fmt.Sprintf("unable to restore config %+v: %s", cs, err))
	}

	assertConfStatesEquivalent(r.logger, cs, r.switchToConfig(cfg, prs))

	pr := r.prs.Progress[r.id]
	pr.MaybeUpdate(pr.Next - 1) // TODO(tbg): this is untested and likely unneeded

	r.logger.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] restored snapshot [index: %d, term: %d]",
		r.id, r.raftLog.committed, r.raftLog.lastIndex(), r.raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term)
	return true
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
//
// promotable 会检查 prs 字段中是否还存在当前节点对应的 Progress 实例, 这是为了检测当前节点是否被从集群中移除了
func (r *raft) promotable() bool {
	pr := r.prs.Progress[r.id]
	return pr != nil && !pr.IsLearner
}

func (r *raft) applyConfChange(cc pb.ConfChangeV2) pb.ConfState {
	cfg, prs, err := func() (tracker.Config, tracker.ProgressMap, error) {
		changer := confchange.Changer{
			Tracker:   r.prs,
			LastIndex: r.raftLog.lastIndex(),
		}
		if cc.LeaveJoint() {
			return changer.LeaveJoint()
		} else if autoLeave, ok := cc.EnterJoint(); ok {
			return changer.EnterJoint(autoLeave, cc.Changes...)
		}
		return changer.Simple(cc.Changes...)
	}()

	if err != nil {
		// TODO(tbg): return the error to the caller.
		panic(err)
	}

	return r.switchToConfig(cfg, prs)
}

// switchToConfig reconfigures this node to use the provided configuration. It
// updates the in-memory state and, when necessary, carries out additional
// actions such as reacting to the removal of nodes or changed quorum
// requirements.
//
// The inputs usually result from restoring a ConfState or applying a ConfChange.
func (r *raft) switchToConfig(cfg tracker.Config, prs tracker.ProgressMap) pb.ConfState {
	r.prs.Config = cfg
	r.prs.Progress = prs

	r.logger.Infof("%x switched to configuration %s", r.id, r.prs.Config)
	cs := r.prs.ConfState()
	pr, ok := r.prs.Progress[r.id]

	// Update whether the node itself is a learner, resetting to false when the
	// node is removed.
	r.isLearner = ok && pr.IsLearner

	if (!ok || r.isLearner) && r.state == StateLeader {
		// This node is leader and was removed or demoted. We prevent demotions
		// at the time writing but hypothetically we handle them the same way as
		// removing the leader: stepping down into the next Term.
		//
		// TODO(tbg): step down (for sanity) and ask follower with largest Match
		// to TimeoutNow (to avoid interruption). This might still drop some
		// proposals but it's better than nothing.
		//
		// TODO(tbg): test this branch. It is untested at the time of writing.
		return cs
	}

	// The remaining steps only make sense if this node is the leader and there
	// are other nodes.
	if r.state != StateLeader || len(cs.Voters) == 0 {
		return cs
	}

	if r.maybeCommit() {
		// If the configuration change means that more entries are committed now,
		// broadcast/append to everyone in the updated config.
		r.bcastAppend()
	} else {
		// Otherwise, still probe the newly added replicas; there's no reason to
		// let them wait out a heartbeat interval (or the next incoming
		// proposal).
		r.prs.Visit(func(id uint64, pr *tracker.Progress) {
			r.maybeSendAppend(id, false /* sendIfEmpty */)
		})
	}
	// If the the leadTransferee was removed, abort the leadership transfer.
	if _, tOK := r.prs.Progress[r.leadTransferee]; !tOK && r.leadTransferee != 0 {
		r.abortLeaderTransfer()
	}

	return cs
}

func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.raftLog.committed, r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
//
// pastElectionTimeout 检测当前的选举计时器是否超时
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, Type: pb.MsgTimeoutNow})
}

// abortLeaderTransfer 清空 raft.leadTransferee 字段, 放弃转移
func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// increaseUncommittedSize computes the size of the proposed entries and
// determines whether they would push leader over its maxUncommittedSize limit.
// If the new entries would exceed the limit, the method returns false. If not,
// the increase in uncommitted entry size is recorded and the method returns
// true.
//
// increaseUncommittedSize 更新当前 raftLog 中未提交 Entry 记录的总字节数, 返回 true 表示允许追加
func (r *raft) increaseUncommittedSize(ents []pb.Entry) bool {
	var s uint64
	// 计算待追加 Entry 记录的总字节数大小
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}

	// 若加上待追加的 Entry 的总字节数后, 当前未提交 Entry 记录的总字节数超过了最大值限制, 则返回 false, 表示禁止追加
	if r.uncommittedSize > 0 && r.uncommittedSize+s > r.maxUncommittedSize {
		// If the uncommitted tail of the Raft log is empty, allow any size
		// proposal. Otherwise, limit the size of the uncommitted tail of the
		// log and drop any proposal that would push the size over the limit.
		return false
	}
	r.uncommittedSize += s
	return true
}

// reduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
func (r *raft) reduceUncommittedSize(ents []pb.Entry) {
	if r.uncommittedSize == 0 {
		// Fast-path for followers, who do not track or enforce the limit.
		return
	}

	var s uint64
	for _, e := range ents {
		s += uint64(PayloadSize(e))
	}
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}

// numOfPendingConf 检测是否有未应用的 EntryConfChange 记录, 若有, 则返回其未应用的数量
func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].Type == pb.EntryConfChange {
			n++
		}
	}
	return n
}
