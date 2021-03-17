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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"time"

	"go.etcd.io/etcd/etcdserver/api/snap"
	pioutil "go.etcd.io/etcd/pkg/ioutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/version"

	humanize "github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

const (
	// connReadLimitByte limits the number of bytes
	// a single read can read out.
	//
	// 64KB should be large enough for not causing
	// throughput bottleneck as well as small enough
	// for not causing a read timeout.
	connReadLimitByte = 64 * 1024
)

var (
	RaftPrefix         = "/raft"
	ProbingPrefix      = path.Join(RaftPrefix, "probing")
	RaftStreamPrefix   = path.Join(RaftPrefix, "stream")
	RaftSnapshotPrefix = path.Join(RaftPrefix, "snapshot")

	errIncompatibleVersion = errors.New("incompatible version")
	errClusterIDMismatch   = errors.New("cluster ID mismatch")
)

type peerGetter interface {
	Get(id types.ID) Peer
}

type writerToResponse interface {
	WriteTo(w http.ResponseWriter)
}

type pipelineHandler struct {
	lg      *zap.Logger
	// 当前节点 ID
	localID types.ID
	// 指向 rafthttp.Transport 实例
	tr      Transporter
	// 指向 Raft 实例, 其实现的底层封装了 etcd-raft 模块
	r       Raft
	// 当前节点所在集群的 ID
	cid     types.ID
}

// newPipelineHandler returns a handler for handling raft messages
// from pipeline for RaftPrefix.
//
// The handler reads out the raft message from request body,
// and forwards it to the given raft state machine for processing.
func newPipelineHandler(t *Transport, r Raft, cid types.ID) http.Handler {
	return &pipelineHandler{
		lg:      t.Logger,
		localID: t.ID,
		tr:      t,
		r:       r,
		cid:     cid,
	}
}

// ServeHTTP 读取对端节点发来的请求得到相应的消息实例, 然后将其交给底层的 etcd-raft 模块进行处理.
func (h *pipelineHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 检测请求是否为 POST 请求
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// 设置当前集群的 ID 到响应头部中
	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	// 检测集群 ID 是否合法
	if err := checkClusterCompatibilityFromHeader(h.lg, h.localID, r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	addRemoteFromRequest(h.tr, r)

	// Limit the data size that could be read from the request body, which ensures that read from
	// connection will not time out accidentally due to possible blocking in underlying implementation.
	//
	// 限制每次从底层连接读取的字节数上限, 默认是 64KB, 因为快照数据可能非常大, 为了防止读取超时,
	// 只能每次读取一部分数据到缓冲区中, 最后将全部数据拼接起来, 得到完整的快照数据.
	limitedr := pioutil.NewLimitedBufferReader(r.Body, connReadLimitByte)
	// 读取 HTTP 请求的 Body 的全部内容
	b, err := ioutil.ReadAll(limitedr)
	if err != nil {
		if h.lg != nil {
			h.lg.Warn(
				"failed to read Raft message",
				zap.String("local-member-id", h.localID.String()),
				zap.Error(err),
			)
		} else {
			plog.Errorf("failed to read raft message (%v)", err)
		}
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	// 反序列化得到 raftpb.Message 实例
	var m raftpb.Message
	if err := m.Unmarshal(b); err != nil {
		if h.lg != nil {
			h.lg.Warn(
				"failed to unmarshal Raft message",
				zap.String("local-member-id", h.localID.String()),
				zap.Error(err),
			)
		} else {
			plog.Errorf("failed to unmarshal raft message (%v)", err)
		}
		http.Error(w, "error unmarshalling raft message", http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		return
	}

	receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(len(b)))

	// 将读取到的消息实例交给底层的 Raft 状态机进行处理
	if err := h.r.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		case writerToResponse:
			v.WriteTo(w)
		default:
			if h.lg != nil {
				h.lg.Warn(
					"failed to process Raft message",
					zap.String("local-member-id", h.localID.String()),
					zap.Error(err),
				)
			} else {
				plog.Warningf("failed to process raft message (%v)", err)
			}
			http.Error(w, "error processing raft message", http.StatusInternalServerError)
			w.(http.Flusher).Flush()
			// disconnect the http stream
			panic(err)
		}
		return
	}

	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	//
	// 向对端节点返回合适的状态码, 表示请求已经被处理
	w.WriteHeader(http.StatusNoContent)
}

// snapshotHandler 用来接收对端节点发来的快照数据
type snapshotHandler struct {
	lg          *zap.Logger
	// 指向 rafthttp.Transport 实例
	tr          Transporter
	// 实现 Raft 实例, 其实现的底层封装了 etcd-raft 模块
	r           Raft
	// 负责管理快照文件
	snapshotter *snap.Snapshotter

	// 当前节点 ID
	localID types.ID
	// 当前节点所在集群的 ID
	cid     types.ID
}

func newSnapshotHandler(t *Transport, r Raft, snapshotter *snap.Snapshotter, cid types.ID) http.Handler {
	return &snapshotHandler{
		lg:          t.Logger,
		tr:          t,
		r:           r,
		snapshotter: snapshotter,
		localID:     t.ID,
		cid:         cid,
	}
}

const unknownSnapshotSender = "UNKNOWN_SNAPSHOT_SENDER"

// ServeHTTP serves HTTP request to receive and process snapshot message.
//
// If request sender dies without closing underlying TCP connection,
// the handler will keep waiting for the request body until TCP keepalive
// finds out that the connection is broken after several minutes.
// This is acceptable because
// 1. snapshot messages sent through other TCP connections could still be
// received and processed.
// 2. this case should happen rarely, so no further optimization is done.
//
// ServeHTTP 除了读取对端节点发来的快照数据, 还会在本地生成相应的快照文件, 并将快照数据通过 Raft 接口
// 传递给底层的 etcd-raft 模块进行处理.
func (h *snapshotHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// 必须为 POST 请求
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		snapshotReceiveFailures.WithLabelValues(unknownSnapshotSender).Inc()
		return
	}

	// 将集群 ID 设置到响应头部中
	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	// 检测集群 ID 是否合法
	if err := checkClusterCompatibilityFromHeader(h.lg, h.localID, r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		snapshotReceiveFailures.WithLabelValues(unknownSnapshotSender).Inc()
		return
	}

	addRemoteFromRequest(h.tr, r)

	dec := &messageDecoder{r: r.Body}
	// let snapshots be very large since they can exceed 512MB for large installations
	//
	// 限制每次从底层连接读取的字节数上限, 默认是 64KB, 因为快照数据可能非常大, 为了防止读取超时,
	// 只能每次读取一部分数据到缓冲区中, 最后将全部数据拼接起来, 得到完整的快照数据.
	m, err := dec.decodeLimit(uint64(1 << 63))
	// 获取对端节点的 ID
	from := types.ID(m.From).String()
	if err != nil {
		msg := fmt.Sprintf("failed to decode raft message (%v)", err)
		if h.lg != nil {
			h.lg.Warn(
				"failed to decode Raft message",
				zap.String("local-member-id", h.localID.String()),
				zap.String("remote-snapshot-sender-id", from),
				zap.Error(err),
			)
		} else {
			plog.Error(msg)
		}
		http.Error(w, msg, http.StatusBadRequest)
		recvFailures.WithLabelValues(r.RemoteAddr).Inc()
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}

	msgSizeVal := m.Size()
	msgSize := humanize.Bytes(uint64(msgSizeVal))
	receivedBytes.WithLabelValues(from).Add(float64(msgSizeVal))

	// 检测读取到的消息类型, 是否为 MsgSnap, 若不是, 则直接返回异常信息
	if m.Type != raftpb.MsgSnap {
		if h.lg != nil {
			h.lg.Warn(
				"unexpected Raft message type",
				zap.String("local-member-id", h.localID.String()),
				zap.String("remote-snapshot-sender-id", from),
				zap.String("message-type", m.Type.String()),
			)
		} else {
			plog.Errorf("unexpected raft message type %s on snapshot path", m.Type)
		}
		http.Error(w, "wrong raft message type", http.StatusBadRequest)
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}

	snapshotReceiveInflights.WithLabelValues(from).Inc()
	defer func() {
		snapshotReceiveInflights.WithLabelValues(from).Dec()
	}()

	if h.lg != nil {
		h.lg.Info(
			"receiving database snapshot",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
			zap.Int("incoming-snapshot-message-size-bytes", msgSizeVal),
			zap.String("incoming-snapshot-message-size", msgSize),
		)
	} else {
		plog.Infof("receiving database snapshot [index: %d, from: %s, raft message size: %s]", m.Snapshot.Metadata.Index, types.ID(m.From), msgSize)
	}

	// save incoming database snapshot.
	// 使用 Snapshotter 将快照数据保存到本地文件中
	n, err := h.snapshotter.SaveDBFrom(r.Body, m.Snapshot.Metadata.Index)
	if err != nil {
		msg := fmt.Sprintf("failed to save KV snapshot (%v)", err)
		if h.lg != nil {
			h.lg.Warn(
				"failed to save incoming database snapshot",
				zap.String("local-member-id", h.localID.String()),
				zap.String("remote-snapshot-sender-id", from),
				zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
				zap.Error(err),
			)
		} else {
			plog.Error(msg)
		}
		http.Error(w, msg, http.StatusInternalServerError)
		snapshotReceiveFailures.WithLabelValues(from).Inc()
		return
	}

	dbSize := humanize.Bytes(uint64(n))
	receivedBytes.WithLabelValues(from).Add(float64(n))

	downloadTook := time.Since(start)
	if h.lg != nil {
		h.lg.Info(
			"received and saved database snapshot",
			zap.String("local-member-id", h.localID.String()),
			zap.String("remote-snapshot-sender-id", from),
			zap.Uint64("incoming-snapshot-index", m.Snapshot.Metadata.Index),
			zap.Int64("incoming-snapshot-size-bytes", n),
			zap.String("incoming-snapshot-size", dbSize),
			zap.String("download-took", downloadTook.String()),
		)
	} else {
		plog.Infof("successfully received and saved database snapshot [index: %d, from: %s, raft message size: %s, db size: %s, took: %s]", m.Snapshot.Metadata.Index, types.ID(m.From), msgSize, dbSize, downloadTook.String())
	}

	// 调用 Raft.Process() 方法, 将 MsgSnap 消息传递给底层的 etcd-raft 模块进行处理
	if err := h.r.Process(context.TODO(), m); err != nil {
		switch v := err.(type) {
		// Process may return writerToResponse error when doing some
		// additional checks before calling raft.Node.Step.
		case writerToResponse:
			v.WriteTo(w)
		default:
			msg := fmt.Sprintf("failed to process raft message (%v)", err)
			if h.lg != nil {
				h.lg.Warn(
					"failed to process Raft message",
					zap.String("local-member-id", h.localID.String()),
					zap.String("remote-snapshot-sender-id", from),
					zap.Error(err),
				)
			} else {
				plog.Error(msg)
			}
			http.Error(w, msg, http.StatusInternalServerError)
			snapshotReceiveFailures.WithLabelValues(from).Inc()
		}
		return
	}

	// Write StatusNoContent header after the message has been processed by
	// raft, which facilitates the client to report MsgSnap status.
	w.WriteHeader(http.StatusNoContent) // 返回 204 状态码

	snapshotReceive.WithLabelValues(from).Inc()
	snapshotReceiveSeconds.WithLabelValues(from).Observe(time.Since(start).Seconds())
}

// streamHandler 主要负责在接收到对端的网络连接后, 将其与对应的 streamWriter 实例进行关联.
// 这样, streamWriter 就可以开始向对端节点发送消息了.
type streamHandler struct {
	lg         *zap.Logger
	// 指向 rafthttp.Transport 实例
	tr         *Transport
	// 与 tr 字段一样, 都指向同一个 rafthttp.Transport 实例.
	// 其接口的 Get() 方法用于获取指定 ID 的 Peer 实例.
	peerGetter peerGetter
	// 指向 Raft 实例, 其实现的底层封装了 etcd-raft 模块
	r          Raft
	// 当前节点 ID
	id         types.ID
	// 当前节点所在集群的 ID
	cid        types.ID
}

func newStreamHandler(t *Transport, pg peerGetter, r Raft, id, cid types.ID) http.Handler {
	return &streamHandler{
		lg:         t.Logger,
		tr:         t,
		peerGetter: pg,
		r:          r,
		id:         id,
		cid:        cid,
	}
}

func (h *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 检测请求是否为 GET
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// 将版本和集群 ID 写入到响应头部中
	w.Header().Set("X-Server-Version", version.Version)
	w.Header().Set("X-Etcd-Cluster-ID", h.cid.String())

	// 检测集群 ID 是否合法
	if err := checkClusterCompatibilityFromHeader(h.lg, h.tr.ID, r.Header, h.cid); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}

	var t streamType
	// 确定使用的协议版本
	switch path.Dir(r.URL.Path) {
	case streamTypeMsgAppV2.endpoint():
		t = streamTypeMsgAppV2
	case streamTypeMessage.endpoint():
		t = streamTypeMessage
	default:
		if h.lg != nil {
			h.lg.Debug(
				"ignored unexpected streaming request path",
				zap.String("local-member-id", h.tr.ID.String()),
				zap.String("remote-peer-id-stream-handler", h.id.String()),
				zap.String("path", r.URL.Path),
			)
		} else {
			plog.Debugf("ignored unexpected streaming request path %s", r.URL.Path)
		}
		http.Error(w, "invalid path", http.StatusNotFound)
		return
	}

	// 获取对端节点的 ID
	fromStr := path.Base(r.URL.Path)
	from, err := types.IDFromString(fromStr)
	if err != nil {
		if h.lg != nil {
			h.lg.Warn(
				"failed to parse path into ID",
				zap.String("local-member-id", h.tr.ID.String()),
				zap.String("remote-peer-id-stream-handler", h.id.String()),
				zap.String("path", fromStr),
				zap.Error(err),
			)
		} else {
			plog.Errorf("failed to parse from %s into ID (%v)", fromStr, err)
		}
		http.Error(w, "invalid from", http.StatusNotFound)
		return
	}
	// 检测指定节点是否从当前集群中移出
	if h.r.IsIDRemoved(uint64(from)) {
		if h.lg != nil {
			h.lg.Warn(
				"rejected stream from remote peer because it was removed",
				zap.String("local-member-id", h.tr.ID.String()),
				zap.String("remote-peer-id-stream-handler", h.id.String()),
				zap.String("remote-peer-id-from", from.String()),
			)
		} else {
			plog.Warningf("rejected the stream from peer %s since it was removed", from)
		}
		http.Error(w, "removed member", http.StatusGone)
		return
	}
	// 根据对端节点 ID 获取对应的 Peer 实例
	p := h.peerGetter.Get(from)
	if p == nil {
		// This may happen in following cases:
		// 1. user starts a remote peer that belongs to a different cluster
		// with the same cluster ID.
		// 2. local etcd falls behind of the cluster, and cannot recognize
		// the members that joined after its current progress.
		if urls := r.Header.Get("X-PeerURLs"); urls != "" {
			h.tr.AddRemote(from, strings.Split(urls, ","))
		}
		if h.lg != nil {
			h.lg.Warn(
				"failed to find remote peer in cluster",
				zap.String("local-member-id", h.tr.ID.String()),
				zap.String("remote-peer-id-stream-handler", h.id.String()),
				zap.String("remote-peer-id-from", from.String()),
				zap.String("cluster-id", h.cid.String()),
			)
		} else {
			plog.Errorf("failed to find member %s in cluster %s", from, h.cid)
		}
		http.Error(w, "error sender not found", http.StatusNotFound)
		return
	}

	// 获取当前节点 ID
	wto := h.id.String()
	// 检测请求的目标节点是否为当前节点
	if gto := r.Header.Get("X-Raft-To"); gto != wto {
		if h.lg != nil {
			h.lg.Warn(
				"ignored streaming request; ID mismatch",
				zap.String("local-member-id", h.tr.ID.String()),
				zap.String("remote-peer-id-stream-handler", h.id.String()),
				zap.String("remote-peer-id-header", gto),
				zap.String("remote-peer-id-from", from.String()),
				zap.String("cluster-id", h.cid.String()),
			)
		} else {
			plog.Errorf("streaming request ignored (ID mismatch got %s want %s)", gto, wto)
		}
		http.Error(w, "to field mismatch", http.StatusPreconditionFailed)
		return
	}

	// 返回 200 状态码
	w.WriteHeader(http.StatusOK)
	// 调用 Flush() 方法将响应数据发送到对端节点
	w.(http.Flusher).Flush()

	c := newCloseNotifier()
	conn := &outgoingConn{ // 创建 outgoingConn 实例
		t:       t,
		Writer:  w,
		Flusher: w.(http.Flusher),
		Closer:  c,
		localID: h.tr.ID,
		peerID:  h.id,
	}
	// 将 outgoingConn 实例与对应的 streamWriter 实例绑定
	p.attachOutgoingConn(conn)
	// 在这里阻塞, 直到当前连接关闭
	<-c.closeNotify()
}

// checkClusterCompatibilityFromHeader checks the cluster compatibility of
// the local member from the given header.
// It checks whether the version of local member is compatible with
// the versions in the header, and whether the cluster ID of local member
// matches the one in the header.
func checkClusterCompatibilityFromHeader(lg *zap.Logger, localID types.ID, header http.Header, cid types.ID) error {
	remoteName := header.Get("X-Server-From")

	remoteServer := serverVersion(header)
	remoteVs := ""
	if remoteServer != nil {
		remoteVs = remoteServer.String()
	}

	remoteMinClusterVer := minClusterVersion(header)
	remoteMinClusterVs := ""
	if remoteMinClusterVer != nil {
		remoteMinClusterVs = remoteMinClusterVer.String()
	}

	localServer, localMinCluster, err := checkVersionCompatibility(remoteName, remoteServer, remoteMinClusterVer)

	localVs := ""
	if localServer != nil {
		localVs = localServer.String()
	}
	localMinClusterVs := ""
	if localMinCluster != nil {
		localMinClusterVs = localMinCluster.String()
	}

	if err != nil {
		if lg != nil {
			lg.Warn(
				"failed to check version compatibility",
				zap.String("local-member-id", localID.String()),
				zap.String("local-member-cluster-id", cid.String()),
				zap.String("local-member-server-version", localVs),
				zap.String("local-member-server-minimum-cluster-version", localMinClusterVs),
				zap.String("remote-peer-server-name", remoteName),
				zap.String("remote-peer-server-version", remoteVs),
				zap.String("remote-peer-server-minimum-cluster-version", remoteMinClusterVs),
				zap.Error(err),
			)
		} else {
			plog.Errorf("request version incompatibility (%v)", err)
		}
		return errIncompatibleVersion
	}
	if gcid := header.Get("X-Etcd-Cluster-ID"); gcid != cid.String() {
		if lg != nil {
			lg.Warn(
				"request cluster ID mismatch",
				zap.String("local-member-id", localID.String()),
				zap.String("local-member-cluster-id", cid.String()),
				zap.String("local-member-server-version", localVs),
				zap.String("local-member-server-minimum-cluster-version", localMinClusterVs),
				zap.String("remote-peer-server-name", remoteName),
				zap.String("remote-peer-server-version", remoteVs),
				zap.String("remote-peer-server-minimum-cluster-version", remoteMinClusterVs),
				zap.String("remote-peer-cluster-id", gcid),
			)
		} else {
			plog.Errorf("request cluster ID mismatch (got %s want %s)", gcid, cid)
		}
		return errClusterIDMismatch
	}
	return nil
}

type closeNotifier struct {
	done chan struct{}
}

func newCloseNotifier() *closeNotifier {
	return &closeNotifier{
		done: make(chan struct{}),
	}
}

func (n *closeNotifier) Close() error {
	close(n.done)
	return nil
}

func (n *closeNotifier) closeNotify() <-chan struct{} { return n.done }
