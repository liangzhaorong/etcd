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

package etcdserver

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"go.etcd.io/etcd/auth"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/traceutil"
	"go.etcd.io/etcd/pkg/types"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

const (
	warnApplyDuration = 100 * time.Millisecond
)

type applyResult struct {
	resp proto.Message
	err  error
	// physc signals the physical effect of the request has completed in addition
	// to being logically reflected by the node. Currently only used for
	// Compaction requests.
	//
	// 如果待应用的 Entry 记录中封装了 CompactionRequest, 会调用 applierV3backend.Compact()
	// 方法处理, 在其中会将压缩操作放入 FIFO 调度器中执行. 该方法还会返回一个通道（即这里的 physc）,
	// 当压缩操作真正完成之后, 会关闭该通道以实现通知的效果.
	physc <-chan struct{}
	trace *traceutil.Trace
}

// applierV3 is the interface for processing V3 raft messages
//
// applierV3 接口定义了在 v3 存储之上应用 Entry 记录的功能.
type applierV3 interface {
	// 根据 InternalRaftRequest 的类型调用下面不同的方法进行处理.
	Apply(r *pb.InternalRaftRequest) *applyResult

	// 下面这些方法分别对应于 v3 存储中的同名方法
	Put(txn mvcc.TxnWrite, p *pb.PutRequest) (*pb.PutResponse, *traceutil.Trace, error)
	Range(ctx context.Context, txn mvcc.TxnRead, r *pb.RangeRequest) (*pb.RangeResponse, error)
	DeleteRange(txn mvcc.TxnWrite, dr *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error)
	Txn(rt *pb.TxnRequest) (*pb.TxnResponse, error)
	Compaction(compaction *pb.CompactionRequest) (*pb.CompactionResponse, <-chan struct{}, *traceutil.Trace, error)

	LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error)
	LeaseRevoke(lc *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error)

	LeaseCheckpoint(lc *pb.LeaseCheckpointRequest) (*pb.LeaseCheckpointResponse, error)

	Alarm(*pb.AlarmRequest) (*pb.AlarmResponse, error)

	Authenticate(r *pb.InternalAuthenticateRequest) (*pb.AuthenticateResponse, error)

	AuthEnable() (*pb.AuthEnableResponse, error)
	AuthDisable() (*pb.AuthDisableResponse, error)

	UserAdd(ua *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error)
	UserDelete(ua *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error)
	UserChangePassword(ua *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error)
	UserGrantRole(ua *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error)
	UserGet(ua *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error)
	UserRevokeRole(ua *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error)
	RoleAdd(ua *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error)
	RoleGrantPermission(ua *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error)
	RoleGet(ua *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error)
	RoleRevokePermission(ua *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error)
	RoleDelete(ua *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error)
	UserList(ua *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error)
	RoleList(ua *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error)
}

type checkReqFunc func(mvcc.ReadView, *pb.RequestOp) error

// applierV3backend 结构体是 applierV3 接口的核心实现.
type applierV3backend struct {
	s *EtcdServer

	checkPut   checkReqFunc
	checkRange checkReqFunc
}

func (s *EtcdServer) newApplierV3Backend() applierV3 {
	base := &applierV3backend{s: s}
	base.checkPut = func(rv mvcc.ReadView, req *pb.RequestOp) error {
		return base.checkRequestPut(rv, req)
	}
	base.checkRange = func(rv mvcc.ReadView, req *pb.RequestOp) error {
		return base.checkRequestRange(rv, req)
	}
	return base
}

func (s *EtcdServer) newApplierV3() applierV3 {
	return newAuthApplierV3(
		s.AuthStore(),
		newQuotaApplierV3(s, s.newApplierV3Backend()),
		s.lessor,
	)
}

// Apply 根据请求的类型进行分类处理
func (a *applierV3backend) Apply(r *pb.InternalRaftRequest) *applyResult {
	ar := &applyResult{}
	defer func(start time.Time) {
		warnOfExpensiveRequest(a.s.getLogger(), start, &pb.InternalRaftStringer{Request: r}, ar.resp, ar.err)
		if ar.err != nil {
			warnOfFailedRequest(a.s.getLogger(), start, &pb.InternalRaftStringer{Request: r}, ar.resp, ar.err)
		}
	}(time.Now())

	// call into a.s.applyV3.F instead of a.F so upper appliers can check individual calls
	//
	// 注意, 这里调用的是 EtcdServer.applyV3 对应的方法, 而不是直接调用 applierV3backend 实例的对应方法.
	// 在 EtcdServer 中, applyV3Base 字段指向了 applierV3backend 实例, 而 applyV3 字段则是在 applyV3Base
	// 基础之上的扩展.
	switch {
	case r.Range != nil:
		ar.resp, ar.err = a.s.applyV3.Range(context.TODO(), nil, r.Range)

	// 写入一个 kv 数据
	case r.Put != nil:
		// 调用 applierV3backend.Put() 方法
		ar.resp, ar.trace, ar.err = a.s.applyV3.Put(nil, r.Put)
	case r.DeleteRange != nil:
		ar.resp, ar.err = a.s.applyV3.DeleteRange(nil, r.DeleteRange)

	// 事务操作
	case r.Txn != nil:
		// 调用 applierV3backend.Txn() 方法
		ar.resp, ar.err = a.s.applyV3.Txn(r.Txn)

	// 压缩操作
	case r.Compaction != nil:
		ar.resp, ar.physc, ar.trace, ar.err = a.s.applyV3.Compaction(r.Compaction)

	// 创建一个指定 TTL 秒数的 Lease
	case r.LeaseGrant != nil:
		// 调用 applierV3backend.LeaseGrant() 方法
		ar.resp, ar.err = a.s.applyV3.LeaseGrant(r.LeaseGrant)
	case r.LeaseRevoke != nil:
		ar.resp, ar.err = a.s.applyV3.LeaseRevoke(r.LeaseRevoke)
	case r.LeaseCheckpoint != nil:
		ar.resp, ar.err = a.s.applyV3.LeaseCheckpoint(r.LeaseCheckpoint)
	case r.Alarm != nil:
		ar.resp, ar.err = a.s.applyV3.Alarm(r.Alarm)
	case r.Authenticate != nil:
		ar.resp, ar.err = a.s.applyV3.Authenticate(r.Authenticate)

	// 启用鉴权
	case r.AuthEnable != nil:
		// 调用 applierV3backend.AuthEnable() 方法进行处理
		ar.resp, ar.err = a.s.applyV3.AuthEnable()
	case r.AuthDisable != nil:
		ar.resp, ar.err = a.s.applyV3.AuthDisable()

	// 添加用户
	case r.AuthUserAdd != nil:
		// 调用 applierV3backend.UserAdd() 方法进行处理
		ar.resp, ar.err = a.s.applyV3.UserAdd(r.AuthUserAdd)

	// 删除指定用户
	case r.AuthUserDelete != nil:
		ar.resp, ar.err = a.s.applyV3.UserDelete(r.AuthUserDelete)
	case r.AuthUserChangePassword != nil:
		ar.resp, ar.err = a.s.applyV3.UserChangePassword(r.AuthUserChangePassword)
	case r.AuthUserGrantRole != nil:
		ar.resp, ar.err = a.s.applyV3.UserGrantRole(r.AuthUserGrantRole)
	case r.AuthUserGet != nil:
		ar.resp, ar.err = a.s.applyV3.UserGet(r.AuthUserGet)
	case r.AuthUserRevokeRole != nil:
		ar.resp, ar.err = a.s.applyV3.UserRevokeRole(r.AuthUserRevokeRole)
	case r.AuthRoleAdd != nil:
		ar.resp, ar.err = a.s.applyV3.RoleAdd(r.AuthRoleAdd)
	case r.AuthRoleGrantPermission != nil:
		ar.resp, ar.err = a.s.applyV3.RoleGrantPermission(r.AuthRoleGrantPermission)
	case r.AuthRoleGet != nil:
		ar.resp, ar.err = a.s.applyV3.RoleGet(r.AuthRoleGet)
	case r.AuthRoleRevokePermission != nil:
		ar.resp, ar.err = a.s.applyV3.RoleRevokePermission(r.AuthRoleRevokePermission)
	case r.AuthRoleDelete != nil:
		ar.resp, ar.err = a.s.applyV3.RoleDelete(r.AuthRoleDelete)
	case r.AuthUserList != nil:
		ar.resp, ar.err = a.s.applyV3.UserList(r.AuthUserList)
	case r.AuthRoleList != nil:
		ar.resp, ar.err = a.s.applyV3.RoleList(r.AuthRoleList)
	default:
		panic("not implemented")
	}
	return ar
}

func (a *applierV3backend) Put(txn mvcc.TxnWrite, p *pb.PutRequest) (resp *pb.PutResponse, trace *traceutil.Trace, err error) {
	resp = &pb.PutResponse{} // 该方法的最终返回值
	resp.Header = &pb.ResponseHeader{}
	trace = traceutil.New("put",
		a.s.getLogger(),
		traceutil.Field{Key: "key", Value: string(p.Key)},
		traceutil.Field{Key: "req_size", Value: proto.Size(p)},
	)
	// 获取将要写入的 value 和将要绑定的 Lease 的 id
	val, leaseID := p.Value, lease.LeaseID(p.Lease)
	// 如果传入的 txn 事务为空, 则开启新事务
	if txn == nil {
		if leaseID != lease.NoLease {
			// 通过 lessor.Lookup() 方法查找 PutRequest 携带的 LeaseID 是否存在, 如果不存在, 则返回错误
			if l := a.s.lessor.Lookup(leaseID); l == nil {
				return nil, nil, lease.ErrLeaseNotFound
			}
		}
		// 调用 watchableStore.Write() 方法创建读写事务, 该方法将返回 watchableStoreTxnWrite 实例
		txn = a.s.KV().Write(trace)
		// 当前方法结束时调用 watchableStoreTxnWrite.End() 方法关闭该方法中开启的事务
		defer txn.End()
	}

	var rr *mvcc.RangeResult
	// 如果设置了 IgnoreValue、IgnoreLease 或是 PrevKv
	if p.IgnoreValue || p.IgnoreLease || p.PrevKv {
		trace.DisableStep()
		// 调用 metricsTxnWrite.Range() 方法查询当前的键值对信息
		rr, err = txn.Range(p.Key, nil, mvcc.RangeOptions{})
		if err != nil {
			return nil, nil, err
		}
		trace.EnableStep()
		trace.Step("get previous kv pair")
	}
	// 如果设置了 IgnoreValue 或 IgnoreLease, 则检查当前键值对是否存在， 如果不存在则返回错误
	if p.IgnoreValue || p.IgnoreLease {
		if rr == nil || len(rr.KVs) == 0 {
			// ignore_{lease,value} flag expects previous key-value pair
			return nil, nil, ErrKeyNotFound
		}
	}
	// 如果设置了 IgnoreValue, 则使用当前的 Value 值
	if p.IgnoreValue {
		val = rr.KVs[0].Value
	}
	// 同理, 如果设置了 IgnoreLease, 则不会更新键值对绑定的 Lease
	if p.IgnoreLease {
		leaseID = lease.LeaseID(rr.KVs[0].Lease)
	}
	// 如果设置了 PrevKv, 则在 PutResponse 中返回更新前的键值对信息
	if p.PrevKv {
		if rr != nil && len(rr.KVs) != 0 {
			resp.PrevKv = &rr.KVs[0]
		}
	}

	// 调用 metricsTxnWrite.Put() 方法完成更新操作, 在 PutResponse.Header 中记录该 Put 操作后最新的 revision 信息
	resp.Header.Revision = txn.Put(p.Key, val, leaseID)
	trace.AddField(traceutil.Field{Key: "response_revision", Value: resp.Header.Revision})
	return resp, trace, nil
}

func (a *applierV3backend) DeleteRange(txn mvcc.TxnWrite, dr *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp := &pb.DeleteRangeResponse{} // 该方法的返回值
	resp.Header = &pb.ResponseHeader{}
	// 修正 DeleteRangeRequest.RangeEnd 字段值
	end := mkGteRange(dr.RangeEnd)

	// 如果当前读写事务为空, 则开启新的读写事务, 并在该 DeleteRange() 方法结束后关闭此事务.
	if txn == nil {
		txn = a.s.kv.Write(traceutil.TODO())
		defer txn.End()
	}

	// 如果 PrevKv 设置为 true, 则先查询指定范围的键值对, 并封装到返回值中.
	if dr.PrevKv {
		rr, err := txn.Range(dr.Key, end, mvcc.RangeOptions{})
		if err != nil {
			return nil, err
		}
		if rr != nil {
			resp.PrevKvs = make([]*mvccpb.KeyValue, len(rr.KVs))
			for i := range rr.KVs {
				resp.PrevKvs[i] = &rr.KVs[i]
			}
		}
	}

	// 调用 DeleteRange() 方法删除指定范围的键值对.
	resp.Deleted, resp.Header.Revision = txn.DeleteRange(dr.Key, end)
	return resp, nil
}

// Range 根据 RangeRequest 中指定的条件进行查询, 并且对结果集进行过滤.
func (a *applierV3backend) Range(ctx context.Context, txn mvcc.TxnRead, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	trace := traceutil.Get(ctx)

	resp := &pb.RangeResponse{} // 该方法的返回值
	resp.Header = &pb.ResponseHeader{}

	// 如果当前只读事务为空, 则开启新的只读事务, 并在该 Range() 方法查询结束后关闭此事务.
	if txn == nil {
		txn = a.s.kv.Read(trace)
		defer txn.End()
	}

	limit := r.Limit
	// 如果指定了这些查询条件之一, 则先查询全部符合条件的 KV, 然后进行 limit 的过滤
	if r.SortOrder != pb.RangeRequest_NONE ||
		r.MinModRevision != 0 || r.MaxModRevision != 0 ||
		r.MinCreateRevision != 0 || r.MaxCreateRevision != 0 {
		// fetch everything; sort and truncate afterwards
		limit = 0
	}
	// 修正当前的 limit 值
	if limit > 0 {
		// fetch one extra for 'more' flag
		limit = limit + 1
	}

	// 创建 RangeOptions, 其中封装了 limit、revision 等查询条件
	ro := mvcc.RangeOptions{
		Limit: limit,
		Rev:   r.Revision,
		Count: r.CountOnly,
	}

	// 调用 TxnRead.Range() 方法进行查询
	rr, err := txn.Range(r.Key, mkGteRange(r.RangeEnd), ro)
	if err != nil {
		return nil, err
	}

	// 根据 MaxModRevision 对查询结果进行过滤
	if r.MaxModRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.ModRevision > r.MaxModRevision }
		// pruneKVs() 方法会根据定义的回调函数 f 的返回值进行过滤
		pruneKVs(rr, f)
	}
	// 根据 MinModRevision 对查询结果进行过滤
	if r.MinModRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.ModRevision < r.MinModRevision }
		pruneKVs(rr, f)
	}
	// 根据 MaxCreateRevision 对查询结果进行过滤
	if r.MaxCreateRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.CreateRevision > r.MaxCreateRevision }
		pruneKVs(rr, f)
	}
	// 根据 MinCreateRevision 对查询结果进行过滤
	if r.MinCreateRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.CreateRevision < r.MinCreateRevision }
		pruneKVs(rr, f)
	}

	// 根据 SortOrder 和 SortTarget 对结果集进行排序
	sortOrder := r.SortOrder
	if r.SortTarget != pb.RangeRequest_KEY && sortOrder == pb.RangeRequest_NONE {
		// Since current mvcc.Range implementation returns results
		// sorted by keys in lexiographically ascending order,
		// sort ASCEND by default only when target is not 'KEY'
		sortOrder = pb.RangeRequest_ASCEND
	}
	if sortOrder != pb.RangeRequest_NONE {
		var sorter sort.Interface
		switch {
		case r.SortTarget == pb.RangeRequest_KEY:
			sorter = &kvSortByKey{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_VERSION:
			sorter = &kvSortByVersion{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_CREATE:
			sorter = &kvSortByCreate{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_MOD:
			sorter = &kvSortByMod{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_VALUE:
			sorter = &kvSortByValue{&kvSort{rr.KVs}}
		}
		switch {
		case sortOrder == pb.RangeRequest_ASCEND:
			sort.Sort(sorter)
		case sortOrder == pb.RangeRequest_DESCEND:
			sort.Sort(sort.Reverse(sorter))
		}
	}

	// 根据 limit 对结果集进行过滤
	if r.Limit > 0 && len(rr.KVs) > int(r.Limit) {
		rr.KVs = rr.KVs[:r.Limit]
		resp.More = true
	}
	trace.Step("filter and sort the key-value pairs")
	// 将查询结果集中的键值对数据填充到 RangeResponse 中, 并返回
	resp.Header.Revision = rr.Rev
	resp.Count = int64(rr.Count) // 此次返回的键值对个数
	resp.Kvs = make([]*mvccpb.KeyValue, len(rr.KVs))
	for i := range rr.KVs {
		// 值返回 Key 值
		if r.KeysOnly {
			rr.KVs[i].Value = nil
		}
		resp.Kvs[i] = &rr.KVs[i]
	}
	trace.Step("assemble the response")
	return resp, nil
}

// Txn 方法提供了批量操作的功能.
func (a *applierV3backend) Txn(rt *pb.TxnRequest) (*pb.TxnResponse, error) {
	// 同时检测 TxnRequest.Success 和 Failure 中封装的操作, 如果两者的全部操作都是读操作, 则返回 false.
	isWrite := !isTxnReadonly(rt)
	// 开启只读事务. NewReadOnlyTxnWrite() 函数返回一个 txnReadWrite 实例, 该实例虽然实现了 TxnWrite 接口,
	// 但是其底层依赖只读事务完成读操作, 其写操作（Put、DeleteRange 等）都是不可用的.
	txn := mvcc.NewReadOnlyTxnWrite(a.s.KV().Read(traceutil.TODO()))

	// 执行 TxnRequest.Compare 中指定的比较操作
	txnPath := compareToPath(txn, rt)
	// 检测 TxnRequest 中封装的操作是否合法
	if isWrite {
		if _, err := checkRequests(txn, rt, txnPath, a.checkPut); err != nil {
			txn.End()
			return nil, err
		}
	}
	if _, err := checkRequests(txn, rt, txnPath, a.checkRange); err != nil {
		txn.End()
		return nil, err
	}

	txnResp, _ := newTxnResp(rt, txnPath)

	// When executing mutable txn ops, etcd must hold the txn lock so
	// readers do not see any intermediate results. Since writes are
	// serialized on the raft loop, the revision in the read view will
	// be the revision of the write txn.
	//
	// 如果 TxnRequest 中包含写操作, 则结束当前只读事务, 开启读写事务
	if isWrite {
		txn.End()
		txn = a.s.KV().Write(traceutil.TODO())
	}
	// 执行真正的批量操作, 并将返回值记录到 txnResp 中
	a.applyTxn(txn, rt, txnPath, txnResp)
	rev := txn.Rev() // 获取当前的 revision
	if len(txn.Changes()) != 0 {
		// 如果此次事务中有更新操作, 则递增 rev
		rev++
	}
	// 提交事务
	txn.End()

	// 在返回值中记录最新的 revision
	txnResp.Header.Revision = rev
	return txnResp, nil
}

// newTxnResp allocates a txn response for a txn request given a path.
func newTxnResp(rt *pb.TxnRequest, txnPath []bool) (txnResp *pb.TxnResponse, txnCount int) {
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}
	resps := make([]*pb.ResponseOp, len(reqs))
	txnResp = &pb.TxnResponse{
		Responses: resps,
		Succeeded: txnPath[0],
		Header:    &pb.ResponseHeader{},
	}
	for i, req := range reqs {
		switch tv := req.Request.(type) {
		case *pb.RequestOp_RequestRange:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseRange{}}
		case *pb.RequestOp_RequestPut:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponsePut{}}
		case *pb.RequestOp_RequestDeleteRange:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseDeleteRange{}}
		case *pb.RequestOp_RequestTxn:
			resp, txns := newTxnResp(tv.RequestTxn, txnPath[1:])
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: resp}}
			txnPath = txnPath[1+txns:]
			txnCount += txns + 1
		default:
		}
	}
	return txnResp, txnCount
}

// compareToPath 遍历 TxnRequest.Compare 字段并逐个调用 applyCompares() 函数完成检测,
// 如果其中任意一次比较返回 false, ...
func compareToPath(rv mvcc.ReadView, rt *pb.TxnRequest) []bool {
	txnPath := make([]bool, 1)
	ops := rt.Success
	if txnPath[0] = applyCompares(rv, rt.Compare); !txnPath[0] {
		ops = rt.Failure
	}
	for _, op := range ops {
		tv, ok := op.Request.(*pb.RequestOp_RequestTxn)
		if !ok || tv.RequestTxn == nil {
			continue
		}
		txnPath = append(txnPath, compareToPath(rv, tv.RequestTxn)...)
	}
	return txnPath
}

func applyCompares(rv mvcc.ReadView, cmps []*pb.Compare) bool {
	// 遍历所有的 Compare 操作, 任意一次比较返回 false, 则该方法立即返回 false
	for _, c := range cmps {
		if !applyCompare(rv, c) {
			return false
		}
	}
	return true
}

// applyCompare applies the compare request.
// If the comparison succeeds, it returns true. Otherwise, returns false.
func applyCompare(rv mvcc.ReadView, c *pb.Compare) bool {
	// TODO: possible optimizations
	// * chunk reads for large ranges to conserve memory
	// * rewrite rules for common patterns:
	//	ex. "[a, b) createrev > 0" => "limit 1 /\ kvs > 0"
	// * caching
	//
	// 查询指定键值对
	rr, err := rv.Range(c.Key, mkGteRange(c.RangeEnd), mvcc.RangeOptions{})
	if err != nil {
		return false
	}
	// 未查询到键值对的处理
	if len(rr.KVs) == 0 {
		// 若当前比较操作需要比较的是 value, 则返回 false, 比较失败
		if c.Target == pb.Compare_VALUE {
			// Always fail if comparing a value on a key/keys that doesn't exist;
			// nil == empty string in grpc; no way to represent missing value
			return false
		}
		return compareKV(c, mvccpb.KeyValue{})
	}
	// 遍历所有的键值对, 依次对每个键值对都执行比较
	for _, kv := range rr.KVs {
		if !compareKV(c, kv) {
			return false
		}
	}
	return true
}

func compareKV(c *pb.Compare, ckv mvccpb.KeyValue) bool {
	var result int
	rev := int64(0)
	switch c.Target {
	case pb.Compare_VALUE:
		v := []byte{}
		if tv, _ := c.TargetUnion.(*pb.Compare_Value); tv != nil {
			v = tv.Value
		}
		// 比较 value 值
		result = bytes.Compare(ckv.Value, v)
	case pb.Compare_CREATE:
		if tv, _ := c.TargetUnion.(*pb.Compare_CreateRevision); tv != nil {
			rev = tv.CreateRevision
		}
		result = compareInt64(ckv.CreateRevision, rev)
	case pb.Compare_MOD:
		if tv, _ := c.TargetUnion.(*pb.Compare_ModRevision); tv != nil {
			rev = tv.ModRevision
		}
		result = compareInt64(ckv.ModRevision, rev)
	case pb.Compare_VERSION:
		if tv, _ := c.TargetUnion.(*pb.Compare_Version); tv != nil {
			rev = tv.Version
		}
		result = compareInt64(ckv.Version, rev)
	case pb.Compare_LEASE:
		if tv, _ := c.TargetUnion.(*pb.Compare_Lease); tv != nil {
			rev = tv.Lease
		}
		result = compareInt64(ckv.Lease, rev)
	}
	switch c.Result {
	case pb.Compare_EQUAL:
		return result == 0
	case pb.Compare_NOT_EQUAL:
		return result != 0
	case pb.Compare_GREATER:
		return result > 0
	case pb.Compare_LESS:
		return result < 0
	}
	return true
}

func (a *applierV3backend) applyTxn(txn mvcc.TxnWrite, rt *pb.TxnRequest, txnPath []bool, tresp *pb.TxnResponse) (txns int) {
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}

	lg := a.s.getLogger()
	// 遍历 TxnRequest.Success 中封装的所有操作并执行
	for i, req := range reqs {
		respi := tresp.Responses[i].Response
		switch tv := req.Request.(type) {
		case *pb.RequestOp_RequestRange:
			resp, err := a.Range(context.TODO(), txn, tv.RequestRange)
			if err != nil {
				if lg != nil {
					lg.Panic("unexpected error during txn", zap.Error(err))
				} else {
					plog.Panicf("unexpected error during txn: %v", err)
				}
			}
			respi.(*pb.ResponseOp_ResponseRange).ResponseRange = resp
		case *pb.RequestOp_RequestPut:
			resp, _, err := a.Put(txn, tv.RequestPut)
			if err != nil {
				if lg != nil {
					lg.Panic("unexpected error during txn", zap.Error(err))
				} else {
					plog.Panicf("unexpected error during txn: %v", err)
				}
			}
			respi.(*pb.ResponseOp_ResponsePut).ResponsePut = resp
		case *pb.RequestOp_RequestDeleteRange:
			resp, err := a.DeleteRange(txn, tv.RequestDeleteRange)
			if err != nil {
				if lg != nil {
					lg.Panic("unexpected error during txn", zap.Error(err))
				} else {
					plog.Panicf("unexpected error during txn: %v", err)
				}
			}
			respi.(*pb.ResponseOp_ResponseDeleteRange).ResponseDeleteRange = resp
		case *pb.RequestOp_RequestTxn:
			resp := respi.(*pb.ResponseOp_ResponseTxn).ResponseTxn
			applyTxns := a.applyTxn(txn, tv.RequestTxn, txnPath[1:], resp)
			txns += applyTxns + 1
			txnPath = txnPath[applyTxns+1:]
		default:
			// empty union
		}
	}
	return txns
}

func (a *applierV3backend) Compaction(compaction *pb.CompactionRequest) (*pb.CompactionResponse, <-chan struct{}, *traceutil.Trace, error) {
	resp := &pb.CompactionResponse{}
	resp.Header = &pb.ResponseHeader{}
	trace := traceutil.New("compact",
		a.s.getLogger(),
		traceutil.Field{Key: "revision", Value: compaction.Revision},
	)

	// 调用 store.Compact() 方法完成压缩操作
	ch, err := a.s.KV().Compact(trace, compaction.Revision)
	if err != nil {
		return nil, ch, nil, err
	}
	// get the current revision. which key to get is not important.
	//
	// 获取最新的 revision
	rr, _ := a.s.KV().Range([]byte("compaction"), nil, mvcc.RangeOptions{})
	resp.Header.Revision = rr.Rev
	return resp, ch, trace, err
}

func (a *applierV3backend) LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	l, err := a.s.lessor.Grant(lease.LeaseID(lc.ID), lc.TTL)
	resp := &pb.LeaseGrantResponse{}
	if err == nil {
		resp.ID = int64(l.ID)
		resp.TTL = l.TTL()
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) LeaseRevoke(lc *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	err := a.s.lessor.Revoke(lease.LeaseID(lc.ID))
	return &pb.LeaseRevokeResponse{Header: newHeader(a.s)}, err
}

func (a *applierV3backend) LeaseCheckpoint(lc *pb.LeaseCheckpointRequest) (*pb.LeaseCheckpointResponse, error) {
	for _, c := range lc.Checkpoints {
		err := a.s.lessor.Checkpoint(lease.LeaseID(c.ID), c.Remaining_TTL)
		if err != nil {
			return &pb.LeaseCheckpointResponse{Header: newHeader(a.s)}, err
		}
	}
	return &pb.LeaseCheckpointResponse{Header: newHeader(a.s)}, nil
}

func (a *applierV3backend) Alarm(ar *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	resp := &pb.AlarmResponse{}
	oldCount := len(a.s.alarmStore.Get(ar.Alarm))

	lg := a.s.getLogger()
	switch ar.Action {
	case pb.AlarmRequest_GET:
		resp.Alarms = a.s.alarmStore.Get(ar.Alarm)
	case pb.AlarmRequest_ACTIVATE:
		m := a.s.alarmStore.Activate(types.ID(ar.MemberID), ar.Alarm)
		if m == nil {
			break
		}
		resp.Alarms = append(resp.Alarms, m)
		activated := oldCount == 0 && len(a.s.alarmStore.Get(m.Alarm)) == 1
		if !activated {
			break
		}

		if lg != nil {
			lg.Warn("alarm raised", zap.String("alarm", m.Alarm.String()), zap.String("from", types.ID(m.MemberID).String()))
		} else {
			plog.Warningf("alarm %v raised by peer %s", m.Alarm, types.ID(m.MemberID))
		}
		switch m.Alarm {
		case pb.AlarmType_CORRUPT:
			a.s.applyV3 = newApplierV3Corrupt(a)
		case pb.AlarmType_NOSPACE:
			a.s.applyV3 = newApplierV3Capped(a)
		default:
			if lg != nil {
				lg.Warn("unimplemented alarm activation", zap.String("alarm", fmt.Sprintf("%+v", m)))
			} else {
				plog.Errorf("unimplemented alarm activation (%+v)", m)
			}
		}
	case pb.AlarmRequest_DEACTIVATE:
		m := a.s.alarmStore.Deactivate(types.ID(ar.MemberID), ar.Alarm)
		if m == nil {
			break
		}
		resp.Alarms = append(resp.Alarms, m)
		deactivated := oldCount > 0 && len(a.s.alarmStore.Get(ar.Alarm)) == 0
		if !deactivated {
			break
		}

		switch m.Alarm {
		case pb.AlarmType_NOSPACE, pb.AlarmType_CORRUPT:
			// TODO: check kv hash before deactivating CORRUPT?
			if lg != nil {
				lg.Warn("alarm disarmed", zap.String("alarm", m.Alarm.String()), zap.String("from", types.ID(m.MemberID).String()))
			} else {
				plog.Infof("alarm disarmed %+v", ar)
			}
			a.s.applyV3 = a.s.newApplierV3()
		default:
			if lg != nil {
				lg.Warn("unimplemented alarm deactivation", zap.String("alarm", fmt.Sprintf("%+v", m)))
			} else {
				plog.Errorf("unimplemented alarm deactivation (%+v)", m)
			}
		}
	default:
		return nil, nil
	}
	return resp, nil
}

// applierV3Capped 在 quotaApplierV3 触发限流操作之后, 就会创建 applierV3Capped 实例替换
// EtcdServer 当前使用的 applierV3 接口实现. 通过 applierV3Capped 实例执行任何写入操作都会
// 失败（如 Put() 方法等）, 这样就可以保证底层存储中的数据量不再增加.
type applierV3Capped struct {
	applierV3
	q backendQuota
}

// newApplierV3Capped creates an applyV3 that will reject Puts and transactions
// with Puts so that the number of keys in the store is capped.
func newApplierV3Capped(base applierV3) applierV3 { return &applierV3Capped{applierV3: base} }

func (a *applierV3Capped) Put(txn mvcc.TxnWrite, p *pb.PutRequest) (*pb.PutResponse, *traceutil.Trace, error) {
	return nil, nil, ErrNoSpace
}

func (a *applierV3Capped) Txn(r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if a.q.Cost(r) > 0 {
		return nil, ErrNoSpace
	}
	return a.applierV3.Txn(r)
}

func (a *applierV3Capped) LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	return nil, ErrNoSpace
}

// AuthEnable 启用鉴权功能
func (a *applierV3backend) AuthEnable() (*pb.AuthEnableResponse, error) {
	// 调用 auth.authStore.AuthEnable() 方法进行处理
	err := a.s.AuthStore().AuthEnable()
	if err != nil {
		return nil, err
	}
	// 返回启用鉴权成功的响应
	return &pb.AuthEnableResponse{Header: newHeader(a.s)}, nil
}

func (a *applierV3backend) AuthDisable() (*pb.AuthDisableResponse, error) {
	a.s.AuthStore().AuthDisable()
	return &pb.AuthDisableResponse{Header: newHeader(a.s)}, nil
}

func (a *applierV3backend) Authenticate(r *pb.InternalAuthenticateRequest) (*pb.AuthenticateResponse, error) {
	ctx := context.WithValue(context.WithValue(a.s.ctx, auth.AuthenticateParamIndex{}, a.s.consistIndex.ConsistentIndex()), auth.AuthenticateParamSimpleTokenPrefix{}, r.SimpleToken)
	resp, err := a.s.AuthStore().Authenticate(ctx, r.Name, r.Password)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

// UserAdd 添加用户
func (a *applierV3backend) UserAdd(r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	// 调用 auth.authStore.UserAdd() 方法进行处理
	resp, err := a.s.AuthStore().UserAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserDelete(r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	resp, err := a.s.AuthStore().UserDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserChangePassword(r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	resp, err := a.s.AuthStore().UserChangePassword(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserGrantRole(r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	resp, err := a.s.AuthStore().UserGrantRole(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserGet(r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	resp, err := a.s.AuthStore().UserGet(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserRevokeRole(r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	resp, err := a.s.AuthStore().UserRevokeRole(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleAdd(r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	resp, err := a.s.AuthStore().RoleAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleGrantPermission(r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	resp, err := a.s.AuthStore().RoleGrantPermission(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleGet(r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	resp, err := a.s.AuthStore().RoleGet(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleRevokePermission(r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	resp, err := a.s.AuthStore().RoleRevokePermission(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleDelete(r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	resp, err := a.s.AuthStore().RoleDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserList(r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	resp, err := a.s.AuthStore().UserList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleList(r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	resp, err := a.s.AuthStore().RoleList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

// quotaApplierV3 在 applierV3backend 的基础上提供了限流功能, 即底层的 BoltDB
// 数据库文件的大小增大到上限之后, 就会触发限流操作.
type quotaApplierV3 struct {
	// 内嵌 applierV3, 实际指向 applierV3backend 实例
	applierV3
	q Quota
}

func newQuotaApplierV3(s *EtcdServer, app applierV3) applierV3 {
	return &quotaApplierV3{app, NewBackendQuota(s, "v3-applier")}
}

func (a *quotaApplierV3) Put(txn mvcc.TxnWrite, p *pb.PutRequest) (*pb.PutResponse, *traceutil.Trace, error) {
	// 检测是否触发限流
	ok := a.q.Available(p)
	// 完成真正的 Put 操作
	resp, trace, err := a.applierV3.Put(txn, p)
	// 触发限流之后会返回 ErrNoSpace 信息.
	// 延伸: EtcdServer.applyEntryNormal() 方法中当收到 applierV3.Apply() 方法返回的 ErrNoSpace 错误之后,
	//       会向集群其他节点发送 AlarmRequest 请求. 当该请求经过 Raft 协议提交之后, 集群中各个节点在应用该
	//       请求时, 就会新建一个 applierV3Capped 实例, 并将当前 EtcdServer.applyV3 字段指向该实例.
	if err == nil && !ok {
		err = ErrNoSpace // 注意返回的错误信息
	}
	return resp, trace, err
}

func (a *quotaApplierV3) Txn(rt *pb.TxnRequest) (*pb.TxnResponse, error) {
	ok := a.q.Available(rt)
	resp, err := a.applierV3.Txn(rt)
	if err == nil && !ok {
		err = ErrNoSpace
	}
	return resp, err
}

func (a *quotaApplierV3) LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	ok := a.q.Available(lc)
	resp, err := a.applierV3.LeaseGrant(lc)
	if err == nil && !ok {
		err = ErrNoSpace
	}
	return resp, err
}

type kvSort struct{ kvs []mvccpb.KeyValue }

func (s *kvSort) Swap(i, j int) {
	t := s.kvs[i]
	s.kvs[i] = s.kvs[j]
	s.kvs[j] = t
}
func (s *kvSort) Len() int { return len(s.kvs) }

type kvSortByKey struct{ *kvSort }

func (s *kvSortByKey) Less(i, j int) bool {
	return bytes.Compare(s.kvs[i].Key, s.kvs[j].Key) < 0
}

type kvSortByVersion struct{ *kvSort }

func (s *kvSortByVersion) Less(i, j int) bool {
	return (s.kvs[i].Version - s.kvs[j].Version) < 0
}

type kvSortByCreate struct{ *kvSort }

func (s *kvSortByCreate) Less(i, j int) bool {
	return (s.kvs[i].CreateRevision - s.kvs[j].CreateRevision) < 0
}

type kvSortByMod struct{ *kvSort }

func (s *kvSortByMod) Less(i, j int) bool {
	return (s.kvs[i].ModRevision - s.kvs[j].ModRevision) < 0
}

type kvSortByValue struct{ *kvSort }

func (s *kvSortByValue) Less(i, j int) bool {
	return bytes.Compare(s.kvs[i].Value, s.kvs[j].Value) < 0
}

func checkRequests(rv mvcc.ReadView, rt *pb.TxnRequest, txnPath []bool, f checkReqFunc) (int, error) {
	txnCount := 0
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}
	for _, req := range reqs {
		if tv, ok := req.Request.(*pb.RequestOp_RequestTxn); ok && tv.RequestTxn != nil {
			txns, err := checkRequests(rv, tv.RequestTxn, txnPath[1:], f)
			if err != nil {
				return 0, err
			}
			txnCount += txns + 1
			txnPath = txnPath[txns+1:]
			continue
		}
		if err := f(rv, req); err != nil {
			return 0, err
		}
	}
	return txnCount, nil
}

func (a *applierV3backend) checkRequestPut(rv mvcc.ReadView, reqOp *pb.RequestOp) error {
	tv, ok := reqOp.Request.(*pb.RequestOp_RequestPut)
	if !ok || tv.RequestPut == nil {
		return nil
	}
	req := tv.RequestPut
	if req.IgnoreValue || req.IgnoreLease {
		// expects previous key-value, error if not exist
		rr, err := rv.Range(req.Key, nil, mvcc.RangeOptions{})
		if err != nil {
			return err
		}
		if rr == nil || len(rr.KVs) == 0 {
			return ErrKeyNotFound
		}
	}
	if lease.LeaseID(req.Lease) != lease.NoLease {
		if l := a.s.lessor.Lookup(lease.LeaseID(req.Lease)); l == nil {
			return lease.ErrLeaseNotFound
		}
	}
	return nil
}

func (a *applierV3backend) checkRequestRange(rv mvcc.ReadView, reqOp *pb.RequestOp) error {
	tv, ok := reqOp.Request.(*pb.RequestOp_RequestRange)
	if !ok || tv.RequestRange == nil {
		return nil
	}
	req := tv.RequestRange
	switch {
	case req.Revision == 0:
		return nil
	case req.Revision > rv.Rev():
		return mvcc.ErrFutureRev
	case req.Revision < rv.FirstRev():
		return mvcc.ErrCompacted
	}
	return nil
}

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// mkGteRange determines if the range end is a >= range. This works around grpc
// sending empty byte strings as nil; >= is encoded in the range end as '\0'.
// If it is a GTE range, then []byte{} is returned to indicate the empty byte
// string (vs nil being no byte string).
func mkGteRange(rangeEnd []byte) []byte {
	if len(rangeEnd) == 1 && rangeEnd[0] == 0 {
		return []byte{}
	}
	return rangeEnd
}

func noSideEffect(r *pb.InternalRaftRequest) bool {
	return r.Range != nil || r.AuthUserGet != nil || r.AuthRoleGet != nil
}

func removeNeedlessRangeReqs(txn *pb.TxnRequest) {
	f := func(ops []*pb.RequestOp) []*pb.RequestOp {
		j := 0
		for i := 0; i < len(ops); i++ {
			if _, ok := ops[i].Request.(*pb.RequestOp_RequestRange); ok {
				continue
			}
			ops[j] = ops[i]
			j++
		}

		return ops[:j]
	}

	txn.Success = f(txn.Success)
	txn.Failure = f(txn.Failure)
}

func pruneKVs(rr *mvcc.RangeResult, isPrunable func(*mvccpb.KeyValue) bool) {
	j := 0
	for i := range rr.KVs {
		rr.KVs[j] = rr.KVs[i]
		if !isPrunable(&rr.KVs[i]) {
			j++
		}
	}
	rr.KVs = rr.KVs[:j]
}

func newHeader(s *EtcdServer) *pb.ResponseHeader {
	return &pb.ResponseHeader{
		ClusterId: uint64(s.Cluster().ID()),
		MemberId:  uint64(s.ID()),
		Revision:  s.KV().Rev(),
		RaftTerm:  s.Term(),
	}
}
