// Copyright 2016 The etcd Authors
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

import pb "go.etcd.io/etcd/raft/raftpb"

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
//
// 创建 ReadState 实例是在 Leader 节点已经接收到超过半数以上节点响应它的心跳消息后创建的.
//
// 如果是客户端直接发送到 Leader 节点的消息, 则将 MsgReadIndex 消息对应的已提交位置
// 以及其消息 ID 封装成 ReadState 实例, 添加到 raft.readStates 中保存. 后续会有其他 goroutine
// 读取该数组, 并对相应的 MsgReadIndex 消息进行响应.
type ReadState struct {
	// 该 MsgReadIndex 请求到达时, 对应的 Leader 节点中 raftLog 的已提交位置（即 raftLog.committed）
	Index      uint64
	// 该 MsgReadIndex 消息的消息 ID
	RequestCtx []byte
}

type readIndexStatus struct {
	// 记录了对应的 MsgReadIndex 请求
	req   pb.Message
	// 该 MsgReadIndex 请求到达时, 对应的 Leader 节点中 raftLog 的已提交位置（即 raftLog.committed）
	index uint64
	// NB: this never records 'false', but it's more convenient to use this
	// instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
	// this becomes performance sensitive enough (doubtful), quorum.VoteResult
	// can change to an API that is closer to that of CommittedIndex.
	//
	// 记录了该 MsgReadIndex 相关的 MsgHeartbeatResp 响应信息
	// 就是用于记录集群中有哪些节点响应了 Leader 节点发出的心跳消息
	acks map[uint64]bool
}

type readOnly struct {
	// 当前只读请求的处理模式, 有 ReadOnlySafe 和 ReadOnlyLeaseBased 两种模式
	option           ReadOnlyOption
	// 在 etcd 服务端收到 MsgReadIndex 消息时, 会为其创建一个消息 ID（该 ID 是唯一的）, 并作为 MsgReadIndex
	// 消息的第一条 Entry 记录. 在 pendingReadIndex 中记录了消息 ID 与对应请求的 readIndexStatus 实例的映射.
	pendingReadIndex map[string]*readIndexStatus
	// 记录了 MsgReadIndex 请求对应的消息 ID
	readIndexQueue   []string
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}

// addRequest adds a read only reuqest into readonly struct.
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `m` is the original read only request message from the local or remote node.
//
// addRequest 接收到客户端只读请求时, 在 ReadOnlySafe 模式下, 首先会调用该方法将已提交位置（raftLog.committed）及
// MsgReadIndex 消息的相关信息记录到 raft.readOnly 中.
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	// 在 MsgReadIndex 消息的第一个记录中, 记录了消息 ID
	s := string(m.Entries[0].Data)
	// 检测是否存在 ID 相同的 MsgReadIndex, 如果存在, 则不再记录该 MsgReadIndex 请求
	if _, ok := ro.pendingReadIndex[s]; ok {
		return
	}
	// 否则创建 MsgReadIndex 对应的 readIndexStatus 实例, 并记录到 pendingReadIndex
	ro.pendingReadIndex[s] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]bool)}
	// 记录消息 ID
	ro.readIndexQueue = append(ro.readIndexQueue, s)
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
//
// recvAck 统计指定的消息 ID 收到了多少 MsgHeartbeatResp 响应消息, 从而确定在收到对应的 MsgReadIndex 消息时,
// 当前 Leader 节点是否为集群的 Leader 节点.
//
// id: Leader 节点接收到 MsgReadIndex 消息时调用该方法传入的 Leader 节点 ID;
// context: 接收到的 MsgReadIndex 消息的消息 ID
func (ro *readOnly) recvAck(id uint64, context []byte) map[uint64]bool {
	// 获取只读请求 MsgReadIndex 消息 ID 对应的 readIndexStatus 实例
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return nil
	}

	// 表示 MsgHeartbeatResp 消息的发送节点与当前节点连通
	rs.acks[id] = true
	return rs.acks
}

// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
//
// advance 客户端只读请求下, 当 MsgHeartbeatResp 消息的响应节点超过半数后, 会清空 readOnly
// 中指定消息 ID 及其之前的所有相关记录.
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	// MsgHeartbeat 消息对应的 MsgReadIndex 消息 ID
	ctx := string(m.Context)
	rss := []*readIndexStatus{}

	// 遍历 readOnly.readIndexQueue 中记录的消息 ID
	for _, okctx := range ro.readIndexQueue {
		i++
		// 查找消息 ID 对应的 readIndexStatus 实例
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		// 将 readIndexStatus 实例保存到 rss 数组中
		rss = append(rss, rs)
		// 查找到指定的 MsgReadIndex 消息 ID, 则设置 found 变量跳出循环
		if okctx == ctx {
			found = true
			break
		}
	}

	// 查找到指定的 MsgReadIndex 消息 ID, 则清空 readOnly 中所有在它之前的消息 ID 及相关内容
	if found {
		// 清理 readonly.readIndexQueue 队列中前 0~i 的内容
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, rs := range rss {
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
