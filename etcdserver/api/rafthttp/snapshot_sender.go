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
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/httputil"
	pioutil "go.etcd.io/etcd/pkg/ioutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

var (
	// timeout for reading snapshot response body
	snapResponseReadTimeout = 5 * time.Second
)

// snapshotSender 用于传输快照数据
type snapshotSender struct {
	// from 和 to 分别记录当前节点的 ID 及对端节点的 ID
	from, to types.ID
	// 记录当前集群的 ID
	cid      types.ID

	// 关联的 rafthttp.Transport 实例
	tr     *Transport
	// 负责获取对端节点可用的 URL 地址
	picker *urlPicker
	// 记录当前节点与对端节点的连通情况
	status *peerStatus
	// 底层的 Raft 状态机
	r      Raft
	errorc chan error

	stopc chan struct{}
}

func newSnapshotSender(tr *Transport, picker *urlPicker, to types.ID, status *peerStatus) *snapshotSender {
	return &snapshotSender{
		from:   tr.ID,
		to:     to,
		cid:    tr.ClusterID,
		tr:     tr,
		picker: picker,
		status: status,
		r:      tr.Raft,
		errorc: tr.ErrorC,
		stopc:  make(chan struct{}),
	}
}

func (s *snapshotSender) stop() { close(s.stopc) }

func (s *snapshotSender) send(merged snap.Message) {
	start := time.Now()

	m := merged.Message
	to := types.ID(m.To).String()

	// 根据传入的 snap.Message 实例创建请求 Body
	body := createSnapBody(s.tr.Logger, merged)
	defer body.Close()

	// 选择对端节点可用的 URL 地址
	u := s.picker.pick()
	// 创建 POST 请求, 注意, 这里请求的路径是 "/raft/snapshot", 而 pipeline 发出的请求对应的路径是 "/raft"
	req := createPostRequest(u, RaftSnapshotPrefix, body, "application/octet-stream", s.tr.URLs, s.from, s.cid)

	snapshotTotalSizeVal := uint64(merged.TotalSize)
	snapshotTotalSize := humanize.Bytes(snapshotTotalSizeVal)
	if s.tr.Logger != nil {
		s.tr.Logger.Info(
			"sending database snapshot",
			zap.Uint64("snapshot-index", m.Snapshot.Metadata.Index),
			zap.String("remote-peer-id", to),
			zap.Int64("bytes", merged.TotalSize),
			zap.String("size", snapshotTotalSize),
		)
	} else {
		plog.Infof("start to send database snapshot [index: %d, to %s, size %s]...", m.Snapshot.Metadata.Index, types.ID(m.To), snapshotTotalSize)
	}

	snapshotSendInflights.WithLabelValues(to).Inc()
	defer func() {
		snapshotSendInflights.WithLabelValues(to).Dec()
	}()

	// 发送请求
	err := s.post(req)
	defer merged.CloseWithError(err)
	if err != nil {
		if s.tr.Logger != nil {
			s.tr.Logger.Warn(
				"failed to send database snapshot",
				zap.Uint64("snapshot-index", m.Snapshot.Metadata.Index),
				zap.String("remote-peer-id", to),
				zap.Int64("bytes", merged.TotalSize),
				zap.String("size", snapshotTotalSize),
				zap.Error(err),
			)
		} else {
			plog.Warningf("database snapshot [index: %d, to: %s] failed to be sent out (%v)", m.Snapshot.Metadata.Index, types.ID(m.To), err)
		}

		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, s.errorc)
		}

		// 将此次请求的 URL 设置为不可用
		s.picker.unreachable(u)
		// 设置与对端的连接状态为 false
		s.status.deactivate(failureType{source: sendSnap, action: "post"}, err.Error())
		// 调用该方法通知底层的 etcd-raft 模块对端节点不可达
		s.r.ReportUnreachable(m.To)
		// report SnapshotFailure to raft state machine. After raft state
		// machine knows about it, it would pause a while and retry sending
		// new snapshot message.
		//
		// 将此次发送失败的信息通知底层的 etcd-raft 模块
		s.r.ReportSnapshot(m.To, raft.SnapshotFailure)
		sentFailures.WithLabelValues(to).Inc()
		snapshotSendFailures.WithLabelValues(to).Inc()
		return
	}
	// 发送成功, 设置连接状态为 true
	s.status.activate()
	// 通知底层的 etcd-raft 模块, 此次成功发送快照数据
	s.r.ReportSnapshot(m.To, raft.SnapshotFinish)

	if s.tr.Logger != nil {
		s.tr.Logger.Info(
			"sent database snapshot",
			zap.Uint64("snapshot-index", m.Snapshot.Metadata.Index),
			zap.String("remote-peer-id", to),
			zap.Int64("bytes", merged.TotalSize),
			zap.String("size", snapshotTotalSize),
		)
	} else {
		plog.Infof("database snapshot [index: %d, to: %s] sent out successfully", m.Snapshot.Metadata.Index, types.ID(m.To))
	}

	sentBytes.WithLabelValues(to).Add(float64(merged.TotalSize))
	snapshotSend.WithLabelValues(to).Inc()
	snapshotSendSeconds.WithLabelValues(to).Observe(time.Since(start).Seconds())
}

// post posts the given request.
// It returns nil when request is sent out and processed successfully.
//
// post 发送快照数据给对端
func (s *snapshotSender) post(req *http.Request) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	defer cancel()

	type responseAndError struct {
		resp *http.Response
		body []byte
		err  error
	}
	// 该通道用于返回该请求对应的响应（或是异常信息）
	result := make(chan responseAndError, 1)

	// 启动一个单独的 goroutine, 用于发送请求并读取响应
	go func() {
		// 发送请求
		resp, err := s.tr.pipelineRt.RoundTrip(req)
		if err != nil {
			// 将异常写入 result 通道中
			result <- responseAndError{resp, nil, err}
			return
		}

		// close the response body when timeouts.
		// prevents from reading the body forever when the other side dies right after
		// successfully receives the request body.
		//
		// 超时处理
		time.AfterFunc(snapResponseReadTimeout, func() { httputil.GracefulClose(resp) })
		// 读取响应数据
		body, err := ioutil.ReadAll(resp.Body)
		// 将响应数据进行封装, 并写入 result 通道中
		result <- responseAndError{resp, body, err}
	}()

	select {
	case <-s.stopc:
		return errStopped
	case r := <-result: // 读取 result 通道, 获取响应信息
		if r.err != nil {
			return r.err
		}
		// 检测该 Post 请求是否成功被对端接收并处理
		return checkPostResponse(r.resp, r.body, req, s.to)
	}
}

func createSnapBody(lg *zap.Logger, merged snap.Message) io.ReadCloser {
	buf := new(bytes.Buffer)
	enc := &messageEncoder{w: buf}
	// encode raft message
	if err := enc.encode(&merged.Message); err != nil {
		if lg != nil {
			lg.Panic("failed to encode message", zap.Error(err))
		} else {
			plog.Panicf("encode message error (%v)", err)
		}
	}

	return &pioutil.ReaderAndCloser{
		Reader: io.MultiReader(buf, merged.ReadCloser),
		Closer: merged.ReadCloser,
	}
}
