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

package rafthttp

import (
	"context"
	"sync"
	"time"

	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	// ConnReadTimeout and ConnWriteTimeout are the i/o timeout set on each connection rafthttp pkg creates.
	// A 5 seconds timeout is good enough for recycling bad connections. Or we have to wait for
	// tcp keepalive failing to detect a bad connection, which is at minutes level.
	// For long term streaming connections, rafthttp pkg sends application level linkHeartbeatMessage
	// to keep the connection alive.
	// For short term pipeline connections, the connection MUST be killed to avoid it being
	// put back to http pkg connection pool.
	ConnReadTimeout  = 5 * time.Second
	ConnWriteTimeout = 5 * time.Second

	recvBufSize = 4096
	// maxPendingProposals holds the proposals during one leader election process.
	// Generally one leader election takes at most 1 sec. It should have
	// 0-2 election conflicts, and each one takes 0.5 sec.
	// We assume the number of concurrent proposers is smaller than 4096.
	// One client blocks on its proposal for at least 1 sec, so 4096 is enough
	// to hold all proposals.
	maxPendingProposals = 4096

	streamAppV2 = "streamMsgAppV2"
	streamMsg   = "streamMsg"
	pipelineMsg = "pipeline"
	sendSnap    = "sendMsgSnap"
)

type Peer interface {
	// send sends the message to the remote peer. The function is non-blocking
	// and has no promise that the message will be received by the remote.
	// When it fails to send message out, it will report the status to underlying
	// raft.
	//
	// 发送单个消息, 该方法是非阻塞的, 如果出现发送失败, 则会将失败信息报告给底层的 Raft 接口.
	send(m raftpb.Message)

	// sendSnap sends the merged snapshot message to the remote peer. Its behavior
	// is similar to send.
	//
	// 发送 snap.Message, 其他行为与 send() 方法类似
	sendSnap(m snap.Message)

	// update updates the urls of remote peer.
	//
	// 更新对应节点暴露的 URL 地址
	update(urls types.URLs)

	// attachOutgoingConn attaches the outgoing connection to the peer for
	// stream usage. After the call, the ownership of the outgoing
	// connection hands over to the peer. The peer will close the connection
	// when it is no longer used.
	//
	// 将指定的连接与当前 Peer 绑定, Peer 会将该连接作为 Stream 消息通道使用
	// 当 Peer 不再使用该连接时, 会将该连接关闭.
	attachOutgoingConn(conn *outgoingConn)
	// activeSince returns the time that the connection with the
	// peer becomes active.
	activeSince() time.Time
	// stop performs any necessary finalization and terminates the peer
	// elegantly.
	//
	// 关闭当前 Peer 实例, 会关闭底层的网络连接
	stop()
}

// peer is the representative of a remote raft node. Local raft node sends
// messages to the remote through peer.
// Each peer has two underlying mechanisms to send out a message: stream and
// pipeline.
// A stream is a receiver initialized long-polling connection, which
// is always open to transfer messages. Besides general stream, peer also has
// a optimized stream for sending msgApp since msgApp accounts for large part
// of all messages. Only raft leader uses the optimized stream to send msgApp
// to the remote follower node.
// A pipeline is a series of http clients that send http requests to the remote.
// It is only used when the stream has not been established.
//
// peer 是 Peer 接口的实现.
type peer struct {
	lg *zap.Logger

	// 当前节点 ID
	localID types.ID
	// id of the remote raft peer node
	// 该 peer 实例对应的节点的 ID
	id types.ID

	// 指向 Raft 实例, 其底层实现封装了 etcd-raft 模块
	r Raft

	// 记录了当前节点与 peer 的连接状态
	status *peerStatus

	// 每个节点可能提供多个 URL 供其他节点方法, 当其中一个访问失败时, 应可以尝试使用另一个.
	// 而 urlPicker 提供的主要功能就是在这些 URL 之间进行切换.
	picker *urlPicker

	// 负责向 Stream 通道写入 msgAppV2 版本的消息
	msgAppV2Writer *streamWriter
	// 负责向 Stream 消息通道写入消息
	writer         *streamWriter
	// Pipeline 消息通道
	pipeline       *pipeline
	// 负责发送快照数据
	snapSender     *snapshotSender // snapshot sender to send v3 snapshot messages
	msgAppV2Reader *streamReader
	// 负责从 Stream 消息通道读取消息
	msgAppReader   *streamReader

	// 从 Stream 消息通道中读取到消息后, 会通过该通道将消息交给 Raft 接口, 然后由它返回给底层 etcd-raft 模块进行处理.
	// 缓冲区大小默认为 4096
	recvc chan raftpb.Message
	// 从 Stream 消息通道中读取到 MsgProp 类型的消息后, 会通过该通道将 MsgProp 消息交给 Raft 接口, 然后由它返回给
	// 底层 etcd-raft 模块进行处理. 缓冲区大小默认为 4096
	propc chan raftpb.Message

	mu     sync.Mutex
	// 是否暂停向对应节点发送消息
	paused bool

	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	stopc  chan struct{}
}

func startPeer(t *Transport, urls types.URLs, peerID types.ID, fs *stats.FollowerStats) *peer {
	if t.Logger != nil {
		t.Logger.Info("starting remote peer", zap.String("remote-peer-id", peerID.String()))
	} else {
		plog.Infof("starting peer %s...", peerID)
	}
	defer func() {
		if t.Logger != nil {
			t.Logger.Info("started remote peer", zap.String("remote-peer-id", peerID.String()))
		} else {
			plog.Infof("started peer %s", peerID)
		}
	}()

	// 为该 peer 创建对应的 peerStatus 实例
	status := newPeerStatus(t.Logger, t.ID, peerID)
	// 根据节点提供的 URL 创建 urlPicker
	picker := newURLPicker(urls)
	errorc := t.ErrorC
	// 底层的 Raft 状态机
	r := t.Raft
	// 创建 pipeline 实例
	pipeline := &pipeline{
		peerID:        peerID,
		tr:            t,
		picker:        picker,
		status:        status,
		followerStats: fs,
		raft:          r,
		errorc:        errorc,
	}
	// 启动 pipeline
	pipeline.start()

	// 创建 peer 实例
	p := &peer{
		lg:             t.Logger,
		localID:        t.ID,
		id:             peerID,
		r:              r,
		status:         status,
		picker:         picker,
		msgAppV2Writer: startStreamWriter(t.Logger, t.ID, peerID, status, fs, r),
		// 创建并启动 streamWriter
		writer:         startStreamWriter(t.Logger, t.ID, peerID, status, fs, r),
		pipeline:       pipeline,
		snapSender:     newSnapshotSender(t, picker, peerID, status),
		// 创建 recvc 通道, 缓冲区大小默认为 4096
		recvc:          make(chan raftpb.Message, recvBufSize),
		// 创建 propc 通道, 缓冲区大小默认为 4096
		propc:          make(chan raftpb.Message, maxPendingProposals),
		stopc:          make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	// 启动单独的 goroutine, 它主要负责从 recvc 通道中读取消息, 该通道中的消息就是从对端节点发送过来的消息,
	// 然后将读取到的消息交给底层的 Raft 状态机处理.
	go func() {
		for {
			select {
			// 从 recvc 通道中获取连接上读取到的 Message
			case mm := <-p.recvc:
				// 调用 Raft.Process() 方法将指定的消息实例传递到底层的 etcd-raft 模块进行处理
				if err := r.Process(ctx, mm); err != nil {
					if t.Logger != nil {
						t.Logger.Warn("failed to process Raft message", zap.Error(err))
					} else {
						plog.Warningf("failed to process raft message (%v)", err)
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// r.Process might block for processing proposal when there is no leader.
	// Thus propc must be put into a separate routine with recvc to avoid blocking
	// processing other raft messages.
	//
	// 在底层 Raft 状态机处理 MsgProp 类型的 Message 时, 可能会阻塞, 所以启动单独的 goroutine 来处理
	go func() {
		for {
			select {
			// 从 propc 通道中读取 MsgProp 类型的 Message
			case mm := <-p.propc:
				// 将 MsgProp 消息实例传递到底层的 etcd-raft 模块进行处理
				if err := r.Process(ctx, mm); err != nil {
					plog.Warningf("failed to process raft message (%v)", err)
				}
			case <-p.stopc:
				return
			}
		}
	}()

	p.msgAppV2Reader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMsgAppV2,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}
	// 创建并启动 streamReader 实例, 它主要负责从 Stream 消息通道上读取消息
	p.msgAppReader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMessage,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}

	p.msgAppV2Reader.start()
	p.msgAppReader.start()

	return p
}

func (p *peer) send(m raftpb.Message) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()

	// 检测 paused 字段, 是否暂停对指定节点发送消息
	if paused {
		return
	}

	// 根据消息的类型选择合适的消息通道
	writec, name := p.pick(m)
	select {
	// 将 Message 写入 writec 通道中, 等待发送
	case writec <- m:
	default:
		// 如果发送出现阻塞, 则将信息报告给底层 Raft 状态机, 这里也会根据消息类型选择合适的报告方法
		p.r.ReportUnreachable(m.To)
		if isMsgSnap(m) {
			p.r.ReportSnapshot(m.To, raft.SnapshotFailure)
		}
		if p.status.isActive() {
			if p.lg != nil {
				p.lg.Warn(
					"dropped internal Raft message since sending buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", p.localID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", p.id.String()),
					zap.Bool("remote-peer-active", p.status.isActive()),
				)
			} else {
				plog.MergeWarningf("dropped internal raft message to %s since %s's sending buffer is full (bad/overloaded network)", p.id, name)
			}
		} else {
			if p.lg != nil {
				p.lg.Warn(
					"dropped internal Raft message since sending buffer is full (overloaded network)",
					zap.String("message-type", m.Type.String()),
					zap.String("local-member-id", p.localID.String()),
					zap.String("from", types.ID(m.From).String()),
					zap.String("remote-peer-id", p.id.String()),
					zap.Bool("remote-peer-active", p.status.isActive()),
				)
			} else {
				plog.Debugf("dropped %s to %s since %s's sending buffer is full", m.Type, p.id, name)
			}
		}
		sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
	}
}

// sendSnap 单独启动一个 goroutine 并调用 snapSender.send() 完成快照数据的发送.
func (p *peer) sendSnap(m snap.Message) {
	go p.snapSender.send(m)
}

func (p *peer) update(urls types.URLs) {
	p.picker.update(urls)
}

// attachOutgoingConn 调用 streamWriter.attach() 方法将底层网络连接 conn 传递到 streamWriter 中
func (p *peer) attachOutgoingConn(conn *outgoingConn) {
	var ok bool
	switch conn.t {
	case streamTypeMsgAppV2:
		ok = p.msgAppV2Writer.attach(conn)
	case streamTypeMessage:
		ok = p.writer.attach(conn)
	default:
		if p.lg != nil {
			p.lg.Panic("unknown stream type", zap.String("type", conn.t.String()))
		} else {
			plog.Panicf("unhandled stream type %s", conn.t)
		}
	}
	// 出现异常则关闭连接
	if !ok {
		conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

// Pause pauses the peer. The peer will simply drops all incoming
// messages without returning an error.
func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	p.msgAppReader.pause()
	p.msgAppV2Reader.pause()
}

// Resume resumes a paused peer.
func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	p.msgAppReader.resume()
	p.msgAppV2Reader.resume()
}

func (p *peer) stop() {
	if p.lg != nil {
		p.lg.Info("stopping remote peer", zap.String("remote-peer-id", p.id.String()))
	} else {
		plog.Infof("stopping peer %s...", p.id)
	}

	defer func() {
		if p.lg != nil {
			p.lg.Info("stopped remote peer", zap.String("remote-peer-id", p.id.String()))
		} else {
			plog.Infof("stopped peer %s", p.id)
		}
	}()

	close(p.stopc)
	p.cancel()
	p.msgAppV2Writer.stop()
	p.writer.stop()
	p.pipeline.stop()
	p.snapSender.stop()
	p.msgAppV2Reader.stop()
	p.msgAppReader.stop()
}

// pick picks a chan for sending the given message. The picked chan and the picked chan
// string name are returned.
//
// pick 根据消息的类型选择合适的消息通道, 并返回相应的通道供 send() 方法写入待发送的消息.
func (p *peer) pick(m raftpb.Message) (writec chan<- raftpb.Message, picked string) {
	var ok bool
	// Considering MsgSnap may have a big size, e.g., 1G, and will block
	// stream for a long time, only use one of the N pipelines to send MsgSnap.
	//
	// 如果是 MsgSnap 类型的消息, 则返回 Pipeline 消息通道对应的 channel, 否则返回 Stream 消息通道
	// 对应的 channel, 如果 stream 消息通道不可用, 则使用 Pipeline 消息通道发送所有类型的消息.
	if isMsgSnap(m) {
		return p.pipeline.msgc, pipelineMsg
	} else if writec, ok = p.msgAppV2Writer.writec(); ok && isMsgApp(m) {
		return writec, streamAppV2
	} else if writec, ok = p.writer.writec(); ok {
		return writec, streamMsg
	}
	return p.pipeline.msgc, pipelineMsg
}

func isMsgApp(m raftpb.Message) bool { return m.Type == raftpb.MsgApp }

func isMsgSnap(m raftpb.Message) bool { return m.Type == raftpb.MsgSnap }
