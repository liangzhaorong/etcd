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
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"sync"
	"time"

	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"

	"go.uber.org/zap"
)

const (
	connPerPipeline = 4
	// pipelineBufSize is the size of pipeline buffer, which helps hold the
	// temporary network latency.
	// The size ensures that pipeline does not drop messages when the network
	// is out of work for less than 1 second in good path.
	pipelineBufSize = 64
)

var errStopped = errors.New("stopped")

// pipeline 负责传输快照数据
type pipeline struct {
	// peer ID
	peerID types.ID

	// 指向 rafthttp.Transport 实例
	tr     *Transport
	// peer 节点的 URL 选择器
	picker *urlPicker
	// 记录 peer 节点的状态
	status *peerStatus
	// 指向 Raft 实例, 其底层实现封装了 etcd-raft 模块
	raft   Raft
	errorc chan error
	// deprecate when we depercate v2 API
	followerStats *stats.FollowerStats

	// pipeline 实例从该通道中获取待发送的消息
	msgc chan raftpb.Message
	// wait for the handling routines
	//
	// 负责同步多个 goroutine 结束. 每个 pipeline 实例会启动多个后台 goroutine（默认 4 个）来处理 msgc 通道中
	// 的消息, 在 pipeline.stop() 方法中必须等待这些 goroutine 都结束, 才能真正关闭该 pipeline 实例.
	wg    sync.WaitGroup
	stopc chan struct{}
}

// start 初始化 pipeline 实例, 并启动用于发送消息的后台 goroutine.
func (p *pipeline) start() {
	p.stopc = make(chan struct{})
	// 注意缓存, 默认是 64, 主要是为了防止瞬间网络延迟造成消息丢失.
	p.msgc = make(chan raftpb.Message, pipelineBufSize)
	p.wg.Add(connPerPipeline)
	// 启动用于发送消息的 goroutine, 默认 4 个
	for i := 0; i < connPerPipeline; i++ {
		go p.handle()
	}

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"started HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	} else {
		plog.Infof("started HTTP pipelining with peer %s", p.peerID)
	}
}

func (p *pipeline) stop() {
	close(p.stopc)
	p.wg.Wait()

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"stopped HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	} else {
		plog.Infof("stopped HTTP pipelining with peer %s", p.peerID)
	}
}

// handle 会从 msgc 通道中读取待发送的 Message 消息, 然后调用 pipeline.post() 方法将其发送出去,
// 发送结束后会调用底层 Raft 接口的相应方法报告发送结果.
func (p *pipeline) handle() {
	defer p.wg.Done()

	for {
		select {
		// 获取待发送的 MsgProp 类型的 Message
		case m := <-p.msgc:
			start := time.Now()
			// 将消息序列化, 然后创建 HTTP 请求并发送出去
			err := p.post(pbutil.MustMarshal(&m))
			end := time.Now()

			if err != nil {
				p.status.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())

				if m.Type == raftpb.MsgApp && p.followerStats != nil {
					p.followerStats.Fail()
				}
				// 向底层的 Raft 状态机报告失败信息
				p.raft.ReportUnreachable(m.To)
				if isMsgSnap(m) {
					p.raft.ReportSnapshot(m.To, raft.SnapshotFailure)
				}
				sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
				continue
			}

			p.status.activate()
			if m.Type == raftpb.MsgApp && p.followerStats != nil {
				p.followerStats.Succ(end.Sub(start))
			}
			// 向底层的 Raft 状态机报告发送成功的信息
			if isMsgSnap(m) {
				p.raft.ReportSnapshot(m.To, raft.SnapshotFinish)
			}
			sentBytes.WithLabelValues(types.ID(m.To).String()).Add(float64(m.Size()))
		case <-p.stopc:
			return
		}
	}
}

// post POSTs a data payload to a url. Returns nil if the POST succeeds,
// error on any failure.
//
// post 是真正完成消息发送的地方, 其中会启动一个后台 goroutine 监听控制发送过程及获取发送结果.
func (p *pipeline) post(data []byte) (err error) {
	// 获取对端暴露的 URL 地址
	u := p.picker.pick()
	// 创建 HTTP POST 请求
	req := createPostRequest(u, RaftPrefix, bytes.NewBuffer(data), "application/protobuf", p.tr.URLs, p.tr.ID, p.tr.ClusterID)

	// done 主要用于通知下面的 goroutine 请求是否已经发送完成
	done := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	// 该 goroutine 主要用于监听请求是否需要取消
	go func() {
		select {
		case <-done:
		case <-p.stopc: // 如果在请求的发送过程中, pipeline 关闭了, 则取消该请求
			waitSchedule()
			cancel()
		}
	}()

	// 发送上述 HTTP POST 请求, 并获取到对应的响应
	resp, err := p.tr.pipelineRt.RoundTrip(req)
	// 通知上述 goroutine, 请求已经发送完毕
	done <- struct{}{}
	if err != nil {
		// 出现异常时, 则将该 URL 标识为不可用, 再尝试其他 URL 地址
		p.picker.unreachable(u)
		return err
	}
	defer resp.Body.Close()
	// 读取 HttpResponse.Body 内容
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// 出现异常时, 则将该 URL 标识为不可用, 再尝试其他 URL 地址
		p.picker.unreachable(u)
		return err
	}

	// 检测响应的内容
	err = checkPostResponse(resp, b, req, p.peerID)
	if err != nil {
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
func waitSchedule() { time.Sleep(time.Millisecond) }
