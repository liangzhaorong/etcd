// Copyright 2019 The etcd Authors
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

package tracker

import (
	"fmt"
	"sort"
	"strings"
)

// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
//
// Leader 节点会记录集群中其他节点的日志复制情况（NextIndex 和 MatchIndex）. 在 etcd-raft 模块中,
// 每个 Follower 节点对应的 NextIndex 值和 MatchIndex 值都封装在 Progress 实例中, 除此之外, 每个
// Progress 实例中还封装了对应 Follower 节点的相关信息.
type Progress struct {
	// Match: 对应 Follower 节点当前已经成功复制的 Entry 记录的索引值; (即已追加到 raftLog 中)
	// Next: 对应 Follower 节点下一个待复制的 Entry 记录的索引值.
	Match, Next uint64
	// State defines how the leader should interact with the follower.
	//
	// When in StateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in StateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in StateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	//
	// 对应 Follower 节点的复制状态.
	State StateType

	// PendingSnapshot is used in StateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	//
	// 当前正在发送的快照数据信息, 实际为当前发送的快照数据中的最后一条 Entry 记录的索引值.
	PendingSnapshot uint64

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	//
	// TODO(tbg): the leader should always have this set to true.
	//
	// 从当前 Leader 节点的角度来看, 该 Progress 实例对应的 Follower 节点是否存活.
	RecentActive bool

	// ProbeSent is used while this follower is in StateProbe. When ProbeSent is
	// true, raft should pause sending replication message to this peer until
	// ProbeSent is reset. See ProbeAcked() and IsPaused().
	//
	// StateProbe 此状态下, 消息发送后, 该字段将设置为 true, 表示暂停后续消息的发送
	ProbeSent bool

	// Inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is Full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.FreeLE with the index of the last
	// received entry.
	//
	// 记录了已经发送出去但未收到响应的消息信息.
	Inflights *Inflights

	// IsLearner is true if this progress is tracked for a learner.
	//
	// 该 Progress 实例对应的节点是否是一个 Learner 节点
	IsLearner bool
}

// ResetState moves the Progress into the specified State, resetting ProbeSent,
// PendingSnapshot, and Inflights.
//
// ResetState 将 Progress 切换到指定的状态, 并重置 ProbeSent, PendingSnapshot, Inflights 等字段
func (pr *Progress) ResetState(state StateType) {
	// 重置 ProbeSent 为 false, 则表示允许该 Progress 向 peer 发送复制消息
	pr.ProbeSent = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.Inflights.reset()
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// ProbeAcked is called when this peer has accepted an append. It resets
// ProbeSent to signal that additional append messages should be sent without
// further delay.
//
// ProbeAcked 将 ProbeSent 设置为 false, 表示 Leader 节点可以继续向对应 Follower 节点发送 MsgApp 消息（即复制 Entry 记录）
func (pr *Progress) ProbeAcked() {
	pr.ProbeSent = false
}

// BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
// optionally and if larger, the index of the pending snapshot.
func (pr *Progress) BecomeProbe() {
	// If the original state is StateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	if pr.State == StateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.ResetState(StateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.ResetState(StateProbe)
		pr.Next = pr.Match + 1
	}
}

// BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
//
// BecomeReplicate 将当前 Progress 对应的节点切换为复制模式
func (pr *Progress) BecomeReplicate() {
	// 重置 Progress 为复制模式, 即允许该 Progress 向 peer 发送复制消息
	pr.ResetState(StateReplicate)
	pr.Next = pr.Match + 1
}

// BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
// snapshot index.
func (pr *Progress) BecomeSnapshot(snapshoti uint64) {
	pr.ResetState(StateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// MaybeUpdate is called when an MsgAppResp arrives from the follower, with the
// index acked by it. The method returns false if the given n index comes from
// an outdated message. Otherwise it updates the progress and returns true.
//
// MaybeUpdate 尝试修改 Match 和 Next 字段, 用来标识对应节点 Entry 记录复制的情况. Leader 节点除了在向自身 raftLog
// 中追加消息时（即 appendEntry() 方法）会调用该方法, 当 Leader 节点收到 Follower 节点的 MsgAppResp 消息（即 MsgApp
// 消息的响应消息）时, 也会调用该方法尝试修改 Follower 节点对应的 Progress 实例.
func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		// n 之前的成功发送所有 Entry 记录已经写入对应节点的 raftLog 中
		pr.Match = n
		updated = true
		// 将 ProbeSent 设置为 false, 表示 Leader 节点可以继续向对应 Follower 节点发送 MsgApp 消息（即复制 Entry 记录）
		pr.ProbeAcked()
	}
	if pr.Next < n+1 {
		// 移动 Next 字段, 下次要复制的 Entry 记录从 Next 开始
		pr.Next = n + 1
	}
	return updated
}

// OptimisticUpdate signals that appends all the way up to and including index n
// are in-flight. As a result, Next is increased to n+1.
func (pr *Progress) OptimisticUpdate(n uint64) { pr.Next = n + 1 }

// MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
// arguments are the index the follower rejected to append to its log, and its
// last index.
//
// Rejections can happen spuriously as messages are sent out of order or
// duplicated. In such cases, the rejection pertains to an index that the
// Progress already knows were previously acknowledged, and false is returned
// without changing the Progress.
//
// If the rejection is genuine, Next is lowered sensibly, and the Progress is
// cleared for sending log entries.
//
// MaybeDecrTo 根据对应 Progress 的状态和 MsgAppResp 消息携带的提示信息, 完成 Progress.Next 的更新.
// rejected: 是被拒绝 MsgApp 消息的 Index 字段值;
// last: 是被拒绝 MsgAppResp 消息的 RejectHint 字段值（即对应 Follower 节点 raftLog 中最后一条 Entry 记录的索引）.
func (pr *Progress) MaybeDecrTo(rejected, last uint64) bool {
	if pr.State == StateReplicate {
		// The rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match { // 出现过时的 MsgAppResp 消息, 直接忽略
			return false
		}
		// Directly decrease next to match + 1.
		//
		// TODO(tbg): why not use last if it's larger?
		//
		// 处于 StateReplicate 状态时, 发送 MsgApp 消息的同时会直接调用 Progress.optimisticUpdate() 方法增加 Next,
		// 这就使得 Next 可能会比 Match 大很多, 这里回退 Next 至 Match 位置, 并在后面重新发送 MsgApp 消息进行尝试.
		pr.Next = pr.Match + 1
		return true
	}

	// The rejection must be stale if "rejected" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
	if pr.Next-1 != rejected { // 出现过时的 MsgAppResp 消息, 直接忽略
		return false
	}

	// 根据 MsgAppResp 携带的信息重置 Next
	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1 // 将 Next 重置为 1
	}
	// Next 重置完成, 恢复消息发送, 并在后面重新发送 MsgApp 消息
	pr.ProbeSent = false
	return true
}

// IsPaused returns whether sending log entries to this node has been throttled.
// This is done when a node has rejected recent MsgApps, is currently waiting
// for a snapshot, or has reached the MaxInflightMsgs limit. In normal
// operation, this is false. A throttled node will be contacted less frequently
// until it has reached a state in which it's able to accept a steady stream of
// log entries again.
//
// IsPaused 检测目标节点当前所处的状态, 确定是否可以向该目标节点发送消息
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case StateProbe:
		return pr.ProbeSent
	case StateReplicate:
		return pr.Inflights.Full()
	case StateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s match=%d next=%d", pr.State, pr.Match, pr.Next)
	if pr.IsLearner {
		fmt.Fprint(&buf, " learner")
	}
	if pr.IsPaused() {
		fmt.Fprint(&buf, " paused")
	}
	if pr.PendingSnapshot > 0 {
		fmt.Fprintf(&buf, " pendingSnap=%d", pr.PendingSnapshot)
	}
	if !pr.RecentActive {
		fmt.Fprintf(&buf, " inactive")
	}
	if n := pr.Inflights.Count(); n > 0 {
		fmt.Fprintf(&buf, " inflight=%d", n)
		if pr.Inflights.Full() {
			fmt.Fprint(&buf, "[full]")
		}
	}
	return buf.String()
}

// ProgressMap is a map of *Progress.
type ProgressMap map[uint64]*Progress

// String prints the ProgressMap in sorted key order, one Progress per line.
func (m ProgressMap) String() string {
	ids := make([]uint64, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	var buf strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&buf, "%d: %s\n", id, m[id])
	}
	return buf.String()
}
