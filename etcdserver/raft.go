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

package etcdserver

import (
	"encoding/json"
	"expvar"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"go.etcd.io/etcd/etcdserver/api/membership"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/contention"
	"go.etcd.io/etcd/pkg/logutil"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

const (
	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	maxSizePerMsg = 1 * 1024 * 1024
	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: a better const?
	maxInflightMsgs = 4096 / 8
)

var (
	// protects raftStatus
	raftStatusMu sync.Mutex
	// indirection for expvar func interface
	// expvar panics when publishing duplicate name
	// expvar does not support remove a registered name
	// so only register a func that calls raftStatus
	// and change raftStatus as we need.
	raftStatus func() raft.Status
)

func init() {
	expvar.Publish("raft.status", expvar.Func(func() interface{} {
		raftStatusMu.Lock()
		defer raftStatusMu.Unlock()
		return raftStatus()
	}))
}

// apply contains entries, snapshot to be applied. Once
// an apply is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type apply struct {
	// 已提交、待应用的 Entry 记录
	entries  []raftpb.Entry
	// 待持久化的快照数据
	snapshot raftpb.Snapshot
	// notifyc synchronizes etcd server applies with the raft node
	// 上层应用处理完 apply 中的数据后, 通过该通道通知底层 raft 模块
	notifyc chan struct{}
}

// raftNode 结构体充当 etcd-raft 模块与上层模块之间交互的桥梁.
type raftNode struct {
	lg *zap.Logger

	tickMu *sync.Mutex
	// 内嵌 raftNodeConfig
	raftNodeConfig

	// a chan to send/receive snapshot
	//
	// etcd-raft 模块通过返回 Ready 实例与上层模块进行交互, 其中 Ready.Message 字段记录了待发送的消息,
	// 其中可能会包含 MsgSnap 类型的消息, 该类型消息中封装了需要发送到其他节点的快照数据. 当 raftNode 收到
	// MsgSnap 消息之后, 会将其写入 msgSnapC 通道中, 并等待上层模块进行发送.
	msgSnapC chan raftpb.Message

	// a chan to send out apply
	//
	// 在 etcd-raft 模块返回的 Ready 实例中, 除了封装了待持久化的 Entry 记录和待持久化的快照数据, 还封装了
	// 待应用的 Entry 记录. raftNode 会将待应用的记录和快照数据封装成 apply 实例, 之后写入 applyc 通道等待
	// 上层模块处理.
	applyc chan apply

	// a chan to send out readState
	//
	// Ready.ReadStates 中封装了只读请求相关的 ReadState 实例, 其中的最后一项将会被写入 readStateC 通道中等待
	// 上层模块处理.
	readStateC chan raft.ReadState

	// utility
	//
	// 逻辑时钟, 每触发一次就会推进一次底层的选举计时器和心跳计时器.
	ticker *time.Ticker
	// contention detectors for raft heartbeat message
	//
	// 检测发往同一节点的两次心跳消息是否超时, 如果超时, 则会打印相关警告信息.
	td *contention.TimeoutDetector

	stopped chan struct{}
	done    chan struct{}
}

type raftNodeConfig struct {
	lg *zap.Logger

	// to check if msg receiver is removed from cluster
	//
	// 该函数用来检测指定节点是否已经被移出当前集群.
	isIDRemoved func(id uint64) bool
	// 内嵌 etcd-raft 模块的 Node
	raft.Node
	// 与 raftLog.storage 字段指定的 MemoryStorage 为同一实例, 主要用来保存持久化的 Entry 记录和快照数据.
	raftStorage *raft.MemoryStorage
	// 注意该字段的类型（etcdserver.Storage）, 在 etcd-raft 模块中有一个与之同名的接口（raft.Storage 接口）, MemoryStorage 就是
	// raft.Storage 接口的实现之一.
	storage     Storage
	// 逻辑时钟的刻度
	heartbeat   time.Duration // for logging
	// transport specifies the transport to send and receive msgs to members.
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If transport is nil, server will panic.
	//
	// 通过网络将消息发送到集群中其他节点
	transport rafthttp.Transporter
}

func newRaftNode(cfg raftNodeConfig) *raftNode {
	var lg raft.Logger
	if cfg.lg != nil {
		lg = logutil.NewRaftLoggerZap(cfg.lg)
	} else {
		lcfg := logutil.DefaultZapLoggerConfig
		var err error
		lg, err = logutil.NewRaftLogger(&lcfg)
		if err != nil {
			log.Fatalf("cannot create raft logger %v", err)
		}
	}
	raft.SetLogger(lg)
	r := &raftNode{
		lg:             cfg.lg,
		tickMu:         new(sync.Mutex),
		raftNodeConfig: cfg,
		// set up contention detectors for raft heartbeat message.
		// expect to send a heartbeat within 2 heartbeat intervals.
		td:         contention.NewTimeoutDetector(2 * cfg.heartbeat),
		readStateC: make(chan raft.ReadState, 1),
		msgSnapC:   make(chan raftpb.Message, maxInFlightMsgSnap), // 注意该通道的大小默认为 16
		applyc:     make(chan apply),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	if r.heartbeat == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.heartbeat)
	}
	return r
}

// raft.Node does not have locks in Raft package
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.tickMu.Unlock()
}

// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
func (r *raftNode) start(rh *raftReadyHandler) {
	internalTimeout := time.Second

	// 启动后台的 goroutine 提供服务
	go func() {
		defer r.onStop()
		islead := false // 刚启动时会将当前节点标识为 Follower

		for {
			select {
			// 计时器到期被触发, 调用 tick() 方法推进选举计时器和心跳计时器
			case <-r.ticker.C:
				r.tick()
			case rd := <-r.Ready():
				// 处理 Ready.SoftState 字段, 该字段主要封装了当前集群的 Leader 信息和当前节点角色
				if rd.SoftState != nil {
					// 检测集群的 Leader 节点是否发生变化, 并记录相关监控信息
					newLeader := rd.SoftState.Lead != raft.None && rh.getLead() != rd.SoftState.Lead
					if newLeader {
						leaderChanges.Inc()
					}

					// 记录当前集群是否存在 Leader 的监控信息
					if rd.SoftState.Lead == raft.None {
						hasLeader.Set(0)
					} else {
						hasLeader.Set(1)
					}

					// 调用 raftReadyHandler.updateLead() 方法更新 etcdserver.lead 字段, 将其更新为新的 Leader 节点 ID,
					// 注意, 这里的读取或更新 etcdserver.lead 字段都是原子操作.
					rh.updateLead(rd.SoftState.Lead)
					// 记录当当前节点是否为 Leader 节点
					islead = rd.RaftState == raft.StateLeader
					if islead {
						isLeader.Set(1)
					} else {
						isLeader.Set(0)
					}
					// 调用 raftReadyHandler.updateLeadership 方法, 其中会根据 Leader 节点是否发生变化, 完成一些操作.
					rh.updateLeadership(newLeader)
					// 重置全部探测器中的全部记录
					r.td.Reset()
				}

				// 处理 Ready 实例中返回的 ReadStates 字段信息, 该字段记录了当前节点中等待处理的只读请求
				if len(rd.ReadStates) != 0 {
					select {
					// 将 Ready.ReadStates 中的最后一项写入 readStateC 通道中
					case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:

					// 如果上层模块一直没有读取写入 readStateC 通道中的 ReadState 实例, 会导致本次写入阻塞,
					// 这里会等待 1s, 如果依然无法写入, 则放弃写入并输出警告日志.
					case <-time.After(internalTimeout):
						if r.lg != nil {
							r.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
						} else {
							plog.Warningf("timed out sending read state")
						}
					case <-r.stopped:
						return
					}
				}

				// 下面是对 Ready 实例中待应用的 Entry 记录, 以及快照数据的处理
				notifyc := make(chan struct{}, 1) // 创建 notifyc 通道
				// 将 Ready 实例中的待应用 Entry 记录以及快照数据封装成 apply 实例, 其中封装了 notifyc 通道,
				// 该通道用来协调当前 goroutine 和 EtcdServer 启动的后台 goroutine 的执行.
				ap := apply{
					entries:  rd.CommittedEntries, // 已提交、待应用的 Entry 记录
					snapshot: rd.Snapshot,         // 待持久化的快照数据
					notifyc:  notifyc,
				}

				// 更新 EtcdServer 中记录的已提交位置（EtcdServer.committedIndex 字段）
				updateCommittedIndex(&ap, rh)

				select {
				// 将 apply 实例写入 applyc 通道中, 等待上层应用读取并进行处理
				case r.applyc <- ap:
				case <-r.stopped:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and them
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				//
				// 如果当前节点处理 Leader 状态, 则 raftNode.start() 方法会先调用 raftNode.processMessages()
				// 方法对待发送的消息进行过滤, 然后调用 raftNode.transport.Send() 方法完成消息的发送.
				if islead {
					// gofail: var raftBeforeLeaderSend struct{}
					//
					// Ready.Messages 字段中保存了当前节点中等待发送到集群中其他节点的 Message 消息
					r.transport.Send(r.processMessages(rd.Messages))
				}

				// Must save the snapshot file and WAL snapshot entry before saving any other entries or hardstate to
				// ensure that recovery after a snapshot restore is possible.
				if !raft.IsEmptySnap(rd.Snapshot) {
					// gofail: var raftBeforeSaveSnap struct{}
					//
					// 将快照数据持久化到底层的持久化存储上
					if err := r.storage.SaveSnap(rd.Snapshot); err != nil {
						if r.lg != nil {
							r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
						} else {
							plog.Fatalf("failed to save Raft snapshot %v", err)
						}
					}
					// gofail: var raftAfterSaveSnap struct{}
				}

				// gofail: var raftBeforeSave struct{}
				//
				// 通过 raftNode.storage 将 Ready 实例中携带的 HardState 信息和待持久化的 Entry 记录写入 WAL 日志文件中
				// 注意, 这里实际调用的是 WAL.Save() 方法.
				if err := r.storage.Save(rd.HardState, rd.Entries); err != nil {
					if r.lg != nil {
						r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
					} else {
						plog.Fatalf("failed to save state and entries error: %v", err)
					}
				}
				// 根据 HardState 信息, 记录相关的监控信息
				if !raft.IsEmptyHardState(rd.HardState) {
					proposalsCommitted.Set(float64(rd.HardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				if !raft.IsEmptySnap(rd.Snapshot) {
					// Force WAL to fsync its hard state before Release() releases
					// old data from the WAL. Otherwise could get an error like:
					// panic: tocommit(107) is out of range [lastIndex(84)]. Was the raft log corrupted, truncated, or lost?
					// See https://github.com/etcd-io/etcd/issues/10219 for more details.
					//
					// 实际调用 WAL.Sync() 方法
					if err := r.storage.Sync(); err != nil {
						if r.lg != nil {
							r.lg.Fatal("failed to sync Raft snapshot", zap.Error(err))
						} else {
							plog.Fatalf("failed to sync Raft snapshot %v", err)
						}
					}

					// etcdserver now claim the snapshot has been persisted onto the disk
					//
					// EtcdServer 中会启动后台 goroutine 读取 applyc 通道, 并处理 apply 中封装的快照数据.
					// 这里使用 notifyc 通道通知该后台 goroutine, 该 apply 实例中的快照数据已经被持久化到磁盘,
					// 后台 goroutine 可以开始应用该快照数据了.
					notifyc <- struct{}{}

					// gofail: var raftBeforeApplySnap struct{}
					//
					// 将快照数据保存到 MemoryStorage 中
					r.raftStorage.ApplySnapshot(rd.Snapshot)
					if r.lg != nil {
						r.lg.Info("applied incoming Raft snapshot", zap.Uint64("snapshot-index", rd.Snapshot.Metadata.Index))
					} else {
						plog.Infof("raft applied incoming snapshot at index %d", rd.Snapshot.Metadata.Index)
					}
					// gofail: var raftAfterApplySnap struct{}

					if err := r.storage.Release(rd.Snapshot); err != nil {
						if r.lg != nil {
							r.lg.Fatal("failed to release Raft wal", zap.Error(err))
						} else {
							plog.Fatalf("failed to release Raft wal %v", err)
						}
					}
					// gofail: var raftAfterWALRelease struct{}
				}

				// 将待持久化的 Entry 记录写入 MemoryStorage 中
				r.raftStorage.Append(rd.Entries)

				// 非 Leader 节点, 则处理待发送的消息
				if !islead {
					// finish processing incoming messages before we signal raftdone chan
					//
					// 过滤消息
					msgs := r.processMessages(rd.Messages)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					//
					// 处理 Ready 实例的过程基本结束, 这里会通知 EtcdServer 启动的后台 goroutine, 检测是否生成快照
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before apply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.
					waitApply := false
					for _, ent := range rd.CommittedEntries {
						if ent.Type == raftpb.EntryConfChange {
							waitApply = true
							break
						}
					}
					if waitApply {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-r.stopped:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					//
					// 发送消息
					r.transport.Send(msgs)
				} else { // 当前为 Leader 节点
					// leader already processed 'MsgSnap' and signaled
					//
					// 处理 Ready 实例的过程基本结束, 这里会通知 EtcdServer 启动的后台 goroutine, 检测是否生成快照
					notifyc <- struct{}{}
				}

				// 最后调用 raft.node.Advance() 方法, 通知 etcd-raft 模块此次 Ready 实例已经处理完成,
				// etcd-raft 模块更新相应信息（如, 已应用 Entry 的最大索引值）之后, 可以继续返回 Ready 实例.
				r.Advance()
			case <-r.stopped:
				return
			}
		}
	}()
}

func updateCommittedIndex(ap *apply, rh *raftReadyHandler) {
	var ci uint64
	// 获取已提交、待应用的 Entry 记录中最后一个 Entry 记录的索引值
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].Index
	}
	if ap.snapshot.Metadata.Index > ci {
		ci = ap.snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.updateCommittedIndex(ci)
	}
}

// processMessages 对消息进行过滤, 去除目标节点已被移出集群的消息, 然后分别过滤 MsgAppResp 消息、
// MsgSnap 消息和 MsgHeartbeat 消息.
func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	// 从后向前遍历全部待发送的消息
	for i := len(ms) - 1; i >= 0; i-- {
		// 若消息的目标节点已从集群中移除
		if r.isIDRemoved(ms[i].To) {
			// 将消息的目标节点 ID 设置为 0, 这样在 etcd-raft 模块发送消息的过程中, 会忽略目标节点为 0 的消息
			ms[i].To = 0
		}

		// 只会发送最后一条 MsgAppResp 消息, 没有必要同时发送多条 MsgAppResp 消息
		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		// 处理 MsgSnap 消息
		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			// 将 MsgSnap 消息写入 msgSnapC 通道中
			case r.msgSnapC <- ms[i]:

			// 如果 msgSnapC 通道的缓冲区满了, 则放弃此次快照的发送
			default:
				// drop msgSnap if the inflight chan if full.
			}
			// 将目标节点设置为 0, 则 raftNode.transport 后续不会发送该消息
			ms[i].To = 0
		}
		// 对 MsgHeartbeat 消息的处理
		if ms[i].Type == raftpb.MsgHeartbeat {
			// 通过 TimeoutDetector 进行检测, 检测发往目标节点的心跳消息间隔是否过大
			ok, exceed := r.td.Observe(ms[i].To)
			// 输出警告日志, 表示当前节点可能已经过载了
			if !ok {
				// TODO: limit request rate.
				if r.lg != nil {
					r.lg.Warn(
						"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
						zap.String("to", fmt.Sprintf("%x", ms[i].To)),
						zap.Duration("heartbeat-interval", r.heartbeat),
						zap.Duration("expected-duration", 2*r.heartbeat),
						zap.Duration("exceeded-duration", exceed),
					)
				} else {
					plog.Warningf("failed to send out heartbeat on time (exceeded the %v timeout for %v, to %x)", r.heartbeat, exceed, ms[i].To)
					plog.Warningf("server is likely overloaded")
				}
				heartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

func (r *raftNode) apply() chan apply {
	return r.applyc
}

func (r *raftNode) stop() {
	r.stopped <- struct{}{}
	<-r.done
}

func (r *raftNode) onStop() {
	r.Stop()
	r.ticker.Stop()
	r.transport.Stop()
	if err := r.storage.Close(); err != nil {
		if r.lg != nil {
			r.lg.Panic("failed to close Raft storage", zap.Error(err))
		} else {
			plog.Panicf("raft close storage error: %v", err)
		}
	}
	close(r.done)
}

// for testing
func (r *raftNode) pauseSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Pause()
}

func (r *raftNode) resumeSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Resume()
}

// advanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
func (r *raftNode) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
}

func startNode(cfg ServerConfig, cl *membership.RaftCluster, ids []types.ID) (id types.ID, n raft.Node, s *raft.MemoryStorage, w *wal.WAL) {
	var err error
	// 根据当前节点的名称, 在 RaftCluster 中查找对应的 Member 实例
	member := cl.MemberByName(cfg.Name)
	// 将节点的 id 和当前集群的 id 封装后进行序列化
	metadata := pbutil.MustMarshal(
		&pb.Metadata{
			NodeID:    uint64(member.ID), // 节点的 id
			ClusterID: uint64(cl.ID()),   // 集群的 id
		},
	)
	// 创建 WAL 日志文件, 并将上述元数据信息作为第一条日志记录写入 WAL 日志文件中
	if w, err = wal.Create(cfg.Logger, cfg.WALDir(), metadata); err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Panic("failed to create WAL", zap.Error(err))
		} else {
			plog.Panicf("create wal error: %v", err)
		}
	}
	if cfg.UnsafeNoFsync {
		w.SetUnsafeNoFsync()
	}
	// 为集群中每个节点, 创建对应的 Peer 实例
	peers := make([]raft.Peer, len(ids))
	for i, id := range ids {
		var ctx []byte
		// 直接将 Member 实例序列化
		ctx, err = json.Marshal((*cl).Member(id))
		if err != nil {
			if cfg.Logger != nil {
				cfg.Logger.Panic("failed to marshal member", zap.Error(err))
			} else {
				plog.Panicf("marshal member should never fail: %v", err)
			}
		}
		// 记录了节点 id 和 Member 实例序列化后的数据
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}
	// 当前节点的 id
	id = member.ID
	if cfg.Logger != nil {
		cfg.Logger.Info(
			"starting local member",
			zap.String("local-member-id", id.String()),
			zap.String("cluster-id", cl.ID().String()),
		)
	} else {
		plog.Infof("starting member %s in cluster %s", id, cl.ID())
	}
	// 新建 MemoryStorage 实例
	s = raft.NewMemoryStorage()
	// 初始化 Node 实例时使用的相关配置
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	if cfg.Logger != nil {
		// called after capnslog setting in "init" function
		if cfg.LoggerConfig != nil {
			c.Logger, err = logutil.NewRaftLogger(cfg.LoggerConfig)
			if err != nil {
				log.Fatalf("cannot create raft logger %v", err)
			}
		} else if cfg.LoggerCore != nil && cfg.LoggerWriteSyncer != nil {
			c.Logger = logutil.NewRaftLoggerFromZapCore(cfg.LoggerCore, cfg.LoggerWriteSyncer)
		}
	}

	if len(peers) == 0 {
		n = raft.RestartNode(c)
	} else {
		// 创建并启动 raft.Node 实例
		n = raft.StartNode(c, peers)
	}
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return id, n, s, w
}

// restartNode 根据配置信息和快照数据, 重启 etcd-raft 模块的 Node 实例.
func restartNode(cfg ServerConfig, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	// 根据快照的元数据, 查找合适的 WAL 日志并完成 WAL 日志的回放
	w, id, cid, st, ents := readWAL(cfg.Logger, cfg.WALDir(), walsnap, cfg.UnsafeNoFsync)

	if cfg.Logger != nil {
		cfg.Logger.Info(
			"restarting local member",
			zap.String("cluster-id", cid.String()),
			zap.String("local-member-id", id.String()),
			zap.Uint64("commit-index", st.Commit),
		)
	} else {
		plog.Infof("restarting member %s in cluster %s at commit index %d", id, cid, st.Commit)
	}
	cl := membership.NewCluster(cfg.Logger, "")
	cl.SetID(id, cid)
	// 创建 MemoryStorage 实例
	s := raft.NewMemoryStorage()
	if snapshot != nil {
		// 将快照数据记录到 MemoryStorage 实例中
		s.ApplySnapshot(*snapshot)
	}
	// 根据 WAL 日志文件回放的结果设置 HardState
	s.SetHardState(st)
	// 向 MemoryStorage 实例中追加快照数据之后的 Entry 记录
	s.Append(ents)
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	if cfg.Logger != nil {
		// called after capnslog setting in "init" function
		var err error
		if cfg.LoggerConfig != nil {
			c.Logger, err = logutil.NewRaftLogger(cfg.LoggerConfig)
			if err != nil {
				log.Fatalf("cannot create raft logger %v", err)
			}
		} else if cfg.LoggerCore != nil && cfg.LoggerWriteSyncer != nil {
			c.Logger = logutil.NewRaftLoggerFromZapCore(cfg.LoggerCore, cfg.LoggerWriteSyncer)
		}
	}

	// 根据上面的 raft.Config, 重建 raft.Node 实例
	n := raft.RestartNode(c)
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return id, cl, n, s, w
}

func restartAsStandaloneNode(cfg ServerConfig, snapshot *raftpb.Snapshot) (types.ID, *membership.RaftCluster, raft.Node, *raft.MemoryStorage, *wal.WAL) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, id, cid, st, ents := readWAL(cfg.Logger, cfg.WALDir(), walsnap, cfg.UnsafeNoFsync)

	// discard the previously uncommitted entries
	for i, ent := range ents {
		if ent.Index > st.Commit {
			if cfg.Logger != nil {
				cfg.Logger.Info(
					"discarding uncommitted WAL entries",
					zap.Uint64("entry-index", ent.Index),
					zap.Uint64("commit-index-from-wal", st.Commit),
					zap.Int("number-of-discarded-entries", len(ents)-i),
				)
			} else {
				plog.Infof("discarding %d uncommitted WAL entries ", len(ents)-i)
			}
			ents = ents[:i]
			break
		}
	}

	// force append the configuration change entries
	toAppEnts := createConfigChangeEnts(
		cfg.Logger,
		getIDs(cfg.Logger, snapshot, ents),
		uint64(id),
		st.Term,
		st.Commit,
	)
	ents = append(ents, toAppEnts...)

	// force commit newly appended entries
	err := w.Save(raftpb.HardState{}, toAppEnts)
	if err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Fatal("failed to save hard state and entries", zap.Error(err))
		} else {
			plog.Fatalf("%v", err)
		}
	}
	if len(ents) != 0 {
		st.Commit = ents[len(ents)-1].Index
	}

	if cfg.Logger != nil {
		cfg.Logger.Info(
			"forcing restart member",
			zap.String("cluster-id", cid.String()),
			zap.String("local-member-id", id.String()),
			zap.Uint64("commit-index", st.Commit),
		)
	} else {
		plog.Printf("forcing restart of member %s in cluster %s at commit index %d", id, cid, st.Commit)
	}

	cl := membership.NewCluster(cfg.Logger, "")
	cl.SetID(id, cid)
	s := raft.NewMemoryStorage()
	if snapshot != nil {
		s.ApplySnapshot(*snapshot)
	}
	s.SetHardState(st)
	s.Append(ents)
	c := &raft.Config{
		ID:              uint64(id),
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
	}
	if cfg.Logger != nil {
		// called after capnslog setting in "init" function
		if cfg.LoggerConfig != nil {
			c.Logger, err = logutil.NewRaftLogger(cfg.LoggerConfig)
			if err != nil {
				log.Fatalf("cannot create raft logger %v", err)
			}
		} else if cfg.LoggerCore != nil && cfg.LoggerWriteSyncer != nil {
			c.Logger = logutil.NewRaftLoggerFromZapCore(cfg.LoggerCore, cfg.LoggerWriteSyncer)
		}
	}

	n := raft.RestartNode(c)
	raftStatus = n.Status
	return id, cl, n, s, w
}

// getIDs returns an ordered set of IDs included in the given snapshot and
// the entries. The given snapshot/entries can contain three kinds of
// ID-related entry:
// - ConfChangeAddNode, in which case the contained ID will be added into the set.
// - ConfChangeRemoveNode, in which case the contained ID will be removed from the set.
// - ConfChangeAddLearnerNode, in which the contained ID will be added into the set.
func getIDs(lg *zap.Logger, snap *raftpb.Snapshot, ents []raftpb.Entry) []uint64 {
	ids := make(map[uint64]bool)
	if snap != nil {
		for _, id := range snap.Metadata.ConfState.Voters {
			ids[id] = true
		}
	}
	for _, e := range ents {
		if e.Type != raftpb.EntryConfChange {
			continue
		}
		var cc raftpb.ConfChange
		pbutil.MustUnmarshal(&cc, e.Data)
		switch cc.Type {
		case raftpb.ConfChangeAddLearnerNode:
			ids[cc.NodeID] = true
		case raftpb.ConfChangeAddNode:
			ids[cc.NodeID] = true
		case raftpb.ConfChangeRemoveNode:
			delete(ids, cc.NodeID)
		case raftpb.ConfChangeUpdateNode:
			// do nothing
		default:
			if lg != nil {
				lg.Panic("unknown ConfChange Type", zap.String("type", cc.Type.String()))
			} else {
				plog.Panicf("ConfChange Type should be either ConfChangeAddNode or ConfChangeRemoveNode!")
			}
		}
	}
	sids := make(types.Uint64Slice, 0, len(ids))
	for id := range ids {
		sids = append(sids, id)
	}
	sort.Sort(sids)
	return []uint64(sids)
}

// createConfigChangeEnts creates a series of Raft entries (i.e.
// EntryConfChange) to remove the set of given IDs from the cluster. The ID
// `self` is _not_ removed, even if present in the set.
// If `self` is not inside the given ids, it creates a Raft entry to add a
// default member with the given `self`.
func createConfigChangeEnts(lg *zap.Logger, ids []uint64, self uint64, term, index uint64) []raftpb.Entry {
	found := false
	for _, id := range ids {
		if id == self {
			found = true
		}
	}

	var ents []raftpb.Entry
	next := index + 1

	// NB: always add self first, then remove other nodes. Raft will panic if the
	// set of voters ever becomes empty.
	if !found {
		m := membership.Member{
			ID:             types.ID(self),
			RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"http://localhost:2380"}},
		}
		ctx, err := json.Marshal(m)
		if err != nil {
			if lg != nil {
				lg.Panic("failed to marshal member", zap.Error(err))
			} else {
				plog.Panicf("marshal member should never fail: %v", err)
			}
		}
		cc := &raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  self,
			Context: ctx,
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  pbutil.MustMarshal(cc),
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
		next++
	}

	for _, id := range ids {
		if id == self {
			continue
		}
		cc := &raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: id,
		}
		e := raftpb.Entry{
			Type:  raftpb.EntryConfChange,
			Data:  pbutil.MustMarshal(cc),
			Term:  term,
			Index: next,
		}
		ents = append(ents, e)
		next++
	}

	return ents
}
