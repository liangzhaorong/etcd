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
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"go.etcd.io/etcd/auth"
	"go.etcd.io/etcd/etcdserver/api/membership"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/lease/leasehttp"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/pkg/traceutil"
	"go.etcd.io/etcd/raft"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

const (
	// In the health case, there might be a small gap (10s of entries) between
	// the applied index and committed index.
	// However, if the committed entries are very heavy to apply, the gap might grow.
	// We should stop accepting new proposals if the gap growing to a certain point.
	maxGapBetweenApplyAndCommitIndex = 5000
	traceThreshold                   = 100 * time.Millisecond
)

type RaftKV interface {
	Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error)
	Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error)
	DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error)
	Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error)
	Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error)
}

type Lessor interface {
	// LeaseGrant sends LeaseGrant request to raft and apply it after committed.
	LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error)
	// LeaseRevoke sends LeaseRevoke request to raft and apply it after committed.
	LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error)

	// LeaseRenew renews the lease with given ID. The renewed TTL is returned. Or an error
	// is returned.
	LeaseRenew(ctx context.Context, id lease.LeaseID) (int64, error)

	// LeaseTimeToLive retrieves lease information.
	LeaseTimeToLive(ctx context.Context, r *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error)

	// LeaseLeases lists all leases.
	LeaseLeases(ctx context.Context, r *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error)
}

type Authenticator interface {
	AuthEnable(ctx context.Context, r *pb.AuthEnableRequest) (*pb.AuthEnableResponse, error)
	AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error)
	Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error)
	UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error)
	UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error)
	UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error)
	UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error)
	UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error)
	UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error)
	RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error)
	RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error)
	RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error)
	RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error)
	RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error)
	UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error)
	RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error)
}

func (s *EtcdServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	trace := traceutil.New("range",
		s.getLogger(),
		traceutil.Field{Key: "range_begin", Value: string(r.Key)},
		traceutil.Field{Key: "range_end", Value: string(r.RangeEnd)},
	)
	ctx = context.WithValue(ctx, traceutil.TraceKey, trace)

	var resp *pb.RangeResponse
	var err error
	defer func(start time.Time) {
		warnOfExpensiveReadOnlyRangeRequest(s.getLogger(), start, r, resp, err)
		if resp != nil {
			trace.AddField(
				traceutil.Field{Key: "response_count", Value: len(resp.Kvs)},
				traceutil.Field{Key: "response_revision", Value: resp.Header.Revision},
			)
		}
		trace.LogIfLong(traceThreshold)
	}(time.Now())

	// 判断此次请求是否为 linearizable read（线性读）
	if !r.Serializable {
		// 对于 linearizable read 请求来说, 会在这里阻塞等待.
		err = s.linearizableReadNotify(ctx)
		trace.Step("agreement among raft nodes before linearized reading")
		if err != nil {
			return nil, err
		}
	}
	// 用于检测 RangeRequest 请求权限的回调函数
	chk := func(ai *auth.AuthInfo) error {
		return s.authStore.IsRangePermitted(ai, r.Key, r.RangeEnd)
	}

	// 真正查询键值对的回调函数
	get := func() { resp, err = s.applyV3Base.Range(ctx, nil, r) }
	if serr := s.doSerialize(ctx, chk, get); serr != nil {
		err = serr
		return nil, err
	}
	return resp, err
}

func (s *EtcdServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	ctx = context.WithValue(ctx, traceutil.StartTimeKey, time.Now())
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{Put: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.PutResponse), nil
}

func (s *EtcdServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{DeleteRange: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.DeleteRangeResponse), nil
}

// Txn 在 TxnRequest 中可以封装多个操作, 这些操作将会批量执行. 该 Txn() 方法就用来处理客户端发送的 TxnRequest,
// 其中会根据 TxnRequest 中封装的操作类型进行分类处理. 如果 TxnRequest 中封装的都是只读操作, 则其处理流程与 Range()
// 方法类似; 如果 TxnRequest 中包含写操作, 则其处理流程与 Put() 方法类似.
func (s *EtcdServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	// 检测 TxnRequest 中是否全部为只读操作
	if isTxnReadonly(r) {
		// 如果 TxnRequest 中全部是只读操作, 则检测这些操作是否全部为 serializable read
		if !isTxnSerializable(r) {
			// 如果存在 linearizable read, 则调用 linearizableReadNotify() 方法处理, 并阻塞等待
			// 整个 linearizable read 处理流程结束.
			err := s.linearizableReadNotify(ctx)
			if err != nil {
				return nil, err
			}
		}
		var resp *pb.TxnResponse
		var err error
		// 权限检测回调函数
		chk := func(ai *auth.AuthInfo) error {
			return checkTxnAuth(s.authStore, ai, r)
		}

		defer func(start time.Time) {
			warnOfExpensiveReadOnlyTxnRequest(s.getLogger(), start, r, resp, err)
		}(time.Now())

		// 真正完成 TxnRequest 处理的回调函数
		get := func() { resp, err = s.applyV3Base.Txn(r) }
		// 调用 doSerialize() 方法, 完成 Txn() 处理
		if serr := s.doSerialize(ctx, chk, get); serr != nil {
			return nil, serr
		}
		return resp, err
	}

	// 如果 TxnRequest 中存在写操作, 则需要调用 raftRequest() 方法发送 MsgProp 消息, 并等待其中
	// 的 Entry 记录被应用.
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{Txn: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.TxnResponse), nil
}

func isTxnSerializable(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil || !r.Serializable {
			return false
		}
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil || !r.Serializable {
			return false
		}
	}
	return true
}

func isTxnReadonly(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil {
			return false
		}
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil {
			return false
		}
	}
	return true
}

// Compact 处理客户端压缩请求.
func (s *EtcdServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	startTime := time.Now()
	// 将 CompactionRequest 请求封装成 MsgProp 消息并发送出去, 并等待其中的 Entry 被应用
	result, err := s.processInternalRaftRequestOnce(ctx, pb.InternalRaftRequest{Compaction: r})
	trace := traceutil.TODO()
	if result != nil && result.trace != nil {
		trace = result.trace
		defer func() {
			trace.LogIfLong(traceThreshold)
		}()
		applyStart := result.trace.GetStartTime()
		result.trace.SetStartTime(startTime)
		trace.InsertStep(0, applyStart, "process raft request")
	}
	if r.Physical && result != nil && result.physc != nil {
		// 等待压缩操作结束
		<-result.physc
		// The compaction is done deleting keys; the hash is now settled
		// but the data is not necessarily committed. If there's a crash,
		// the hash may revert to a hash prior to compaction completing
		// if the compaction resumes. Force the finished compaction to
		// commit so it won't resume following a crash.
		s.be.ForceCommit() // 提交压缩操作使用的事务
		trace.Step("physically apply compaction")
	}
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	resp := result.resp.(*pb.CompactionResponse)
	if resp == nil {
		resp = &pb.CompactionResponse{}
	}
	if resp.Header == nil {
		resp.Header = &pb.ResponseHeader{}
	}
	resp.Header.Revision = s.kv.Rev()
	trace.AddField(traceutil.Field{Key: "response_revision", Value: resp.Header.Revision})
	return resp, nil
}

// LeaseGrant 创建一个指定 TTL 秒数的 Lease
func (s *EtcdServer) LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	// no id given? choose one
	//
	// 如果客户端没有指定将要创建的 Lease 的 ID, 则自动生成一个
	for r.ID == int64(lease.NoLease) {
		// only use positive int64 id's
		r.ID = int64(s.reqIDGen.Next() & ((1 << 63) - 1))
	}
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{LeaseGrant: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.LeaseGrantResponse), nil
}

func (s *EtcdServer) LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	// 先将 LeaseRevokeRequest 封装成 InternalRaftRequest, 并调用 raftRequestOnce() 方法
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{LeaseRevoke: r})
	if err != nil {
		return nil, err
	}
	// TODO: 待 LeaseRevokeRequest 请求对应的 Entry 记录成功经过 Raft 协议处理之后, 会经过 applyEntryNormal() 方法
	//  的处理, 其中会调用 applierV3.LeaseRevoke() 方法, 该方法实际上是调用 EtcdServer.lessor.LeaseRevoke() 方法,
	//  该方法会删除对应的 Lease 实例及其绑定的键值对.
	return resp.(*pb.LeaseRevokeResponse), nil
}

func (s *EtcdServer) LeaseRenew(ctx context.Context, id lease.LeaseID) (int64, error) {
	ttl, err := s.lessor.Renew(id)
	if err == nil { // already requested to primary lessor(leader)
		return ttl, nil
	}
	if err != lease.ErrNotPrimary {
		return -1, err
	}

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()

	// renewals don't go through raft; forward to leader manually
	for cctx.Err() == nil && err != nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return -1, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + leasehttp.LeasePrefix
			ttl, err = leasehttp.RenewHTTP(cctx, id, lurl, s.peerRt)
			if err == nil || err == lease.ErrLeaseNotFound {
				return ttl, err
			}
		}
	}

	if cctx.Err() == context.DeadlineExceeded {
		return -1, ErrTimeout
	}
	return -1, ErrCanceled
}

func (s *EtcdServer) LeaseTimeToLive(ctx context.Context, r *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	if s.Leader() == s.ID() {
		// primary; timetolive directly from leader
		le := s.lessor.Lookup(lease.LeaseID(r.ID))
		if le == nil {
			return nil, lease.ErrLeaseNotFound
		}
		// TODO: fill out ResponseHeader
		resp := &pb.LeaseTimeToLiveResponse{Header: &pb.ResponseHeader{}, ID: r.ID, TTL: int64(le.Remaining().Seconds()), GrantedTTL: le.TTL()}
		if r.Keys {
			ks := le.Keys()
			kbs := make([][]byte, len(ks))
			for i := range ks {
				kbs[i] = []byte(ks[i])
			}
			resp.Keys = kbs
		}
		return resp, nil
	}

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()

	// forward to leader
	for cctx.Err() == nil {
		leader, err := s.waitLeader(cctx)
		if err != nil {
			return nil, err
		}
		for _, url := range leader.PeerURLs {
			lurl := url + leasehttp.LeaseInternalPrefix
			resp, err := leasehttp.TimeToLiveHTTP(cctx, lease.LeaseID(r.ID), r.Keys, lurl, s.peerRt)
			if err == nil {
				return resp.LeaseTimeToLiveResponse, nil
			}
			if err == lease.ErrLeaseNotFound {
				return nil, err
			}
		}
	}

	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

func (s *EtcdServer) LeaseLeases(ctx context.Context, r *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error) {
	ls := s.lessor.Leases()
	lss := make([]*pb.LeaseStatus, len(ls))
	for i := range ls {
		lss[i] = &pb.LeaseStatus{ID: int64(ls[i].ID)}
	}
	return &pb.LeaseLeasesResponse{Header: newHeader(s), Leases: lss}, nil
}

func (s *EtcdServer) waitLeader(ctx context.Context) (*membership.Member, error) {
	leader := s.cluster.Member(s.Leader())
	for leader == nil {
		// wait an election
		dur := time.Duration(s.Cfg.ElectionTicks) * time.Duration(s.Cfg.TickMs) * time.Millisecond
		select {
		case <-time.After(dur):
			leader = s.cluster.Member(s.Leader())
		case <-s.stopping:
			return nil, ErrStopped
		case <-ctx.Done():
			return nil, ErrNoLeader
		}
	}
	if leader == nil || len(leader.PeerURLs) == 0 {
		return nil, ErrNoLeader
	}
	return leader, nil
}

func (s *EtcdServer) Alarm(ctx context.Context, r *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{Alarm: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AlarmResponse), nil
}

func (s *EtcdServer) AuthEnable(ctx context.Context, r *pb.AuthEnableRequest) (*pb.AuthEnableResponse, error) {
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{AuthEnable: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthEnableResponse), nil
}

func (s *EtcdServer) AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthDisable: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthDisableResponse), nil
}

func (s *EtcdServer) Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error) {
	if err := s.linearizableReadNotify(ctx); err != nil {
		return nil, err
	}

	lg := s.getLogger()

	var resp proto.Message
	for {
		checkedRevision, err := s.AuthStore().CheckPassword(r.Name, r.Password)
		if err != nil {
			if err != auth.ErrAuthNotEnabled {
				if lg != nil {
					lg.Warn(
						"invalid authentication was requested",
						zap.String("user", r.Name),
						zap.Error(err),
					)
				} else {
					plog.Errorf("invalid authentication request to user %s was issued", r.Name)
				}
			}
			return nil, err
		}

		st, err := s.AuthStore().GenTokenPrefix()
		if err != nil {
			return nil, err
		}

		// internalReq doesn't need to have Password because the above s.AuthStore().CheckPassword() already did it.
		// In addition, it will let a WAL entry not record password as a plain text.
		internalReq := &pb.InternalAuthenticateRequest{
			Name:        r.Name,
			SimpleToken: st,
		}

		resp, err = s.raftRequestOnce(ctx, pb.InternalRaftRequest{Authenticate: internalReq})
		if err != nil {
			return nil, err
		}
		if checkedRevision == s.AuthStore().Revision() {
			break
		}

		if lg != nil {
			lg.Info("revision when password checked became stale; retrying")
		} else {
			plog.Infof("revision when password checked is obsolete, retrying")
		}
	}

	return resp.(*pb.AuthenticateResponse), nil
}

// UserAdd 添加用户
func (s *EtcdServer) UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserAdd: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserAddResponse), nil
}

func (s *EtcdServer) UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserDeleteResponse), nil
}

func (s *EtcdServer) UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserChangePassword: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserChangePasswordResponse), nil
}

func (s *EtcdServer) UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserGrantRole: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserGrantRoleResponse), nil
}

func (s *EtcdServer) UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserGet: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserGetResponse), nil
}

func (s *EtcdServer) UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserList: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserListResponse), nil
}

func (s *EtcdServer) UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserRevokeRole: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserRevokeRoleResponse), nil
}

func (s *EtcdServer) RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleAdd: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleAddResponse), nil
}

func (s *EtcdServer) RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleGrantPermission: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleGrantPermissionResponse), nil
}

func (s *EtcdServer) RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleGet: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleGetResponse), nil
}

func (s *EtcdServer) RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleList: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleListResponse), nil
}

func (s *EtcdServer) RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleRevokePermission: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleRevokePermissionResponse), nil
}

func (s *EtcdServer) RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleDeleteResponse), nil
}

func (s *EtcdServer) raftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
	result, err := s.processInternalRaftRequestOnce(ctx, r)
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	if startTime, ok := ctx.Value(traceutil.StartTimeKey).(time.Time); ok && result.trace != nil {
		applyStart := result.trace.GetStartTime()
		// The trace object is created in apply. Here reset the start time to trace
		// the raft request time by the difference between the request start time
		// and apply start time
		result.trace.SetStartTime(startTime)
		result.trace.InsertStep(0, applyStart, "process raft request")
		result.trace.LogIfLong(traceThreshold)
	}
	return result.resp, nil
}

func (s *EtcdServer) raftRequest(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
	return s.raftRequestOnce(ctx, r)
}

// doSerialize handles the auth logic, with permissions checked by "chk", for a serialized request "get". Returns a non-nil error on authentication failure.
func (s *EtcdServer) doSerialize(ctx context.Context, chk func(*auth.AuthInfo) error, get func()) error {
	trace := traceutil.Get(ctx)
	// 获取权限相关的信息
	ai, err := s.AuthInfoFromCtx(ctx)
	if err != nil {
		return err
	}
	if ai == nil {
		// chk expects non-nil AuthInfo; use empty credentials
		ai = &auth.AuthInfo{}
	}
	// 回调 chk() 回调函数, 通过 authStore 完全权限检测
	if err = chk(ai); err != nil {
		return err
	}
	trace.Step("get authentication metadata")
	// fetch response for serialized request
	// 回调 get() 函数, 其中直接通过 applyV3Base 从底层存储中读取键值对数据
	get()
	// check for stale token revision in case the auth store was updated while
	// the request has been handled.
	//
	// 若读取完成后发生了权限修改, 则返回错误
	if ai.Revision != 0 && ai.Revision != s.authStore.Revision() {
		return auth.ErrAuthOldRevision
	}
	return nil
}

func (s *EtcdServer) processInternalRaftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (*applyResult, error) {
	// 获取当前节点已应用的 Entry 记录的最大索引值
	ai := s.getAppliedIndex()
	// 获取当前节点已提交的 Entry 记录的最大索引值
	ci := s.getCommittedIndex()
	// Preflight Check 检查, 如果 Raft 模块已提交的日志索引（committed index）比已应用的状态机的
	// 日志索引（applied index）超过了 5000, 那么它就会返回一个 "etcdserver: too many requests" 错误给 client.
	if ci > ai+maxGapBetweenApplyAndCommitIndex {
		return nil, ErrTooManyRequests
	}

	// 为请求生成 id
	r.Header = &pb.RequestHeader{
		ID: s.reqIDGen.Next(),
	}

	// 获取请求中的鉴权信息, 记录到请求头中
	authInfo, err := s.AuthInfoFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	if authInfo != nil {
		r.Header.Username = authInfo.Username
		r.Header.AuthRevision = authInfo.Revision
	}

	// 将 InternalRaftRequest 请求序列化
	data, err := r.Marshal()
	if err != nil {
		return nil, err
	}

	// 检测序列化后的长度, 默认大小不可以超过 1.5M
	if len(data) > int(s.Cfg.MaxRequestBytes) {
		return nil, ErrRequestTooLarge
	}

	// 获取当前请求的 ID
	id := r.ID
	if id == 0 {
		id = r.Header.ID
	}
	// 在 EtcdServer.wait 中为该请求注册相应的 ch 通道, 后续可通过该通道获取 Raft 执行结果
	ch := s.w.Register(id)

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()

	start := time.Now()
	// 调用 node.Propose() 函数将 InternalRaftRequest 序列化后的数据封装为 Entry, 之后生成 MsgProp 消息并发送出去.
	err = s.r.Propose(cctx, data)
	if err != nil {
		proposalsFailed.Inc()
		s.w.Trigger(id, nil) // GC wait
		return nil, err
	}
	proposalsPending.Inc()
	defer proposalsPending.Dec()

	select {
	// 阻塞等待上面发送的 Entry 被应用
	case x := <-ch:
		return x.(*applyResult), nil
	case <-cctx.Done():
		proposalsFailed.Inc()
		s.w.Trigger(id, nil) // GC wait
		return nil, s.parseProposeCtxErr(cctx.Err(), start)
	case <-s.done:
		return nil, ErrStopped
	}
}

// Watchable returns a watchable interface attached to the etcdserver.
func (s *EtcdServer) Watchable() mvcc.WatchableKV { return s.KV() }

func (s *EtcdServer) linearizableReadLoop() {
	var rs raft.ReadState

	for {
		ctxToSend := make([]byte, 8)
		id1 := s.reqIDGen.Next() // 创建唯一 ID
		binary.BigEndian.PutUint64(ctxToSend, id1)
		leaderChangedNotifier := s.leaderChangedNotify()
		select {
		case <-leaderChangedNotifier:
			continue
		// 在 Client 发起一次 Linearizable Read 时, 会向 readwaitc 通道中写入一个空结构体作为信号.
		// （是通过调用 EtcdServer.linearizableReadNotify() 方法写入信号到 readwaitc 从而通知当前 goroutine）
		case <-s.readwaitc:
		// 监听到 stopping 通道关闭, 则表示当前 EtcdServer 实例正在关闭, 会结束该 goroutine
		case <-s.stopping:
			return
		}

		// 新建 notifier 实例
		nextnr := newNotifier()

		s.readMu.Lock() // 加锁
		// 记录当前 notifier 实例
		nr := s.readNotifier
		// 使用新建的 notifier 实例更新 EtcdServer.readNotifier 字段
		s.readNotifier = nextnr
		s.readMu.Unlock()

		lg := s.getLogger()
		cctx, cancel := context.WithTimeout(context.Background(), s.Cfg.ReqTimeout())
		// 创建并处理 MsgReadIndex 请求. 实际调用 node.ReadIndex() 函数
		if err := s.r.ReadIndex(cctx, ctxToSend); err != nil {
			cancel()
			if err == raft.ErrStopped {
				return
			}
			if lg != nil {
				lg.Warn("failed to get read index from Raft", zap.Error(err))
			} else {
				plog.Errorf("failed to get read index from raft: %v", err)
			}
			readIndexFailed.Inc()
			nr.notify(err)
			continue
		}
		cancel()

		var (
			timeout bool
			done    bool
		)
		// 检测 MsgReadIndex 请求是否超时, 是否已被处理完
		for !timeout && !done {
			select {
			// 在 raftNode 处理 Ready 实例时, 会将 Ready.ReadStates 中最后一项写入该通道中
			case rs = <-s.r.readStateC:
				// 在 ReadState.RequestCtx 中携带了请求 id
				done = bytes.Equal(rs.RequestCtx, ctxToSend)
				// 忽略乱序的响应, 输出日志并继续当前 for 循环等待请求对应的响应
				if !done {
					// a previous request might time out. now we should ignore the response of it and
					// continue waiting for the response of the current requests.
					id2 := uint64(0)
					if len(rs.RequestCtx) == 8 {
						id2 = binary.BigEndian.Uint64(rs.RequestCtx)
					}
					if lg != nil {
						lg.Warn(
							"ignored out-of-date read index response; local node read indexes queueing up and waiting to be in sync with leader",
							zap.Uint64("sent-request-id", id1),
							zap.Uint64("received-request-id", id2),
						)
					} else {
						plog.Warningf("ignored out-of-date read index response; local node read indexes queueing up and waiting to be in sync with leader (request ID want %d, got %d)", id1, id2)
					}
					slowReadIndex.Inc()
				}
			case <-leaderChangedNotifier:
				timeout = true
				readIndexFailed.Inc()
				// return a retryable error.
				nr.notify(ErrLeaderChanged)
			case <-time.After(s.Cfg.ReqTimeout()):
				if lg != nil {
					lg.Warn("timed out waiting for read index response (local node might have slow network)", zap.Duration("timeout", s.Cfg.ReqTimeout()))
				} else {
					plog.Warningf("timed out waiting for read index response (local node might have slow network)")
				}
				// 在 notifier.err 字段中设置错误信息, 同时关闭 notifier.c 通道
				nr.notify(ErrTimeout)
				timeout = true
				slowReadIndex.Inc()
			case <-s.stopping:
				return
			}
		}
		if !done {
			continue
		}

		// ReadState.Index 记录的是 commit Index
		if ai := s.getAppliedIndex(); ai < rs.Index {
			select {
			// 如果当前节点已应用的 Entry 记录未到指定的 committed Index, 则需要阻塞等待.
			// 后面 EtcdServer.applyAll() 方法处理完待应用的 Entry 记录之后, 会将已应用 Entry 记录
			// 在 WaitTime 中的通道关闭, 则 linearizableReadLoop goroutine 可以得到此信号, 继续执行.
			case <-s.applyWait.Wait(rs.Index):
			case <-s.stopping:
				return
			}
		}
		// unblock all l-reads requested at indices before rs.Index
		nr.notify(nil) // 关闭 notifier.c 通道
	}
}

// linearizableReadNotify 执行线性读, 会在该函数中阻塞, 直到获取到当前集群最新的已提交的日志索引（committed index）
func (s *EtcdServer) linearizableReadNotify(ctx context.Context) error {
	s.readMu.RLock()
	// 获取 EtcdServer.readNotifier 实例
	nc := s.readNotifier
	s.readMu.RUnlock()

	// signal linearizable loop for current notify if it hasn't been already
	select {
	// 在 linearizableReadLoop goroutine 中会阻塞监听 readwaitc 通道, 在这里会向 readwaitc
	// 通道中写入一个空结构体作为信号, 通过 linearizableReadLoop goroutine 开始工作.
	case s.readwaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	// 当 linearizableReadLoop goroutine 发现已应用 Entry 的索引值超过了请求时的 committedIndex,
	// 则关闭 notifier.c 通道, 通知执行读取操作的 goroutine 继续后续读取操作.
	case <-nc.c:
		return nc.err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrStopped
	}
}

func (s *EtcdServer) AuthInfoFromCtx(ctx context.Context) (*auth.AuthInfo, error) {
	authInfo, err := s.AuthStore().AuthInfoFromCtx(ctx)
	if authInfo != nil || err != nil {
		return authInfo, err
	}
	if !s.Cfg.ClientCertAuthEnabled {
		return nil, nil
	}
	authInfo = s.AuthStore().AuthInfoFromTLS(ctx)
	return authInfo, nil

}
