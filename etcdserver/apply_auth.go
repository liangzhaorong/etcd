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
	"context"
	"sync"

	"go.etcd.io/etcd/auth"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/pkg/traceutil"
)

// authApplierV3 在 applierV3backend 的基础上扩展出了权限控制的功能.
type authApplierV3 struct {
	// 内嵌 applierV3
	applierV3
	// AuthStore 接口中定义与权限管理相关操作
	as     auth.AuthStore
	lessor lease.Lessor

	// mu serializes Apply so that user isn't corrupted and so that
	// serialized requests don't leak data from TOCTOU errors
	mu sync.Mutex

	// 在处理每个请求时, 都会使用该字段记录该请求头中的权限信息
	authInfo auth.AuthInfo
}

func newAuthApplierV3(as auth.AuthStore, base applierV3, lessor lease.Lessor) *authApplierV3 {
	return &authApplierV3{applierV3: base, as: as, lessor: lessor}
}

// authApplierV3 重写了 Apply 方法, 首先记录请求头中携带的权限信息, 然后对此次请求是否需要 Admin 权限
// 进行检测, 最后调用 applierV3 实现的 Apply() 方法完成请求的分发.
func (aa *authApplierV3) Apply(r *pb.InternalRaftRequest) *applyResult {
	aa.mu.Lock()
	defer aa.mu.Unlock()
	// 将请求头中的 Username 和 AuthRevision 记录到 authApplierV3.authInfo 中
	if r.Header != nil {
		// backward-compatible with pre-3.0 releases when internalRaftRequest
		// does not have header field
		aa.authInfo.Username = r.Header.Username
		aa.authInfo.Revision = r.Header.AuthRevision
	}
	// 检测该请求是否需要 Admin 权限, 其中 AuthEnable、AuthDisable、AuthUser*、AuthRole* 等请求,
	// 都是需要 Admin 权限.
	if needAdminPermission(r) {
		// 检测 Admin 权限
		if err := aa.as.IsAdminPermitted(&aa.authInfo); err != nil {
			aa.authInfo.Username = ""
			aa.authInfo.Revision = 0
			return &applyResult{err: err}
		}
	}
	// 调用底层 applierV3 实现的 Apply() 方法完成请求分发
	ret := aa.applierV3.Apply(r)
	// 清空 authApplierV3.authInfo
	aa.authInfo.Username = ""
	aa.authInfo.Revision = 0
	return ret
}

func (aa *authApplierV3) Put(txn mvcc.TxnWrite, r *pb.PutRequest) (*pb.PutResponse, *traceutil.Trace, error) {
	// 检测 Put 权限
	if err := aa.as.IsPutPermitted(&aa.authInfo, r.Key); err != nil {
		return nil, nil, err
	}

	if err := aa.checkLeasePuts(lease.LeaseID(r.Lease)); err != nil {
		// The specified lease is already attached with a key that cannot
		// be written by this user. It means the user cannot revoke the
		// lease so attaching the lease to the newly written key should
		// be forbidden.
		return nil, nil, err
	}

	// 如果需要返回更新前的键值对信息, 则需要用 Range 权限
	if r.PrevKv {
		err := aa.as.IsRangePermitted(&aa.authInfo, r.Key, nil)
		if err != nil {
			return nil, nil, err
		}
	}
	// 调用底层的 applierV3 实现, 完成 Put 操作
	return aa.applierV3.Put(txn, r)
}

func (aa *authApplierV3) Range(ctx context.Context, txn mvcc.TxnRead, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	if err := aa.as.IsRangePermitted(&aa.authInfo, r.Key, r.RangeEnd); err != nil {
		return nil, err
	}
	return aa.applierV3.Range(ctx, txn, r)
}

func (aa *authApplierV3) DeleteRange(txn mvcc.TxnWrite, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	if err := aa.as.IsDeleteRangePermitted(&aa.authInfo, r.Key, r.RangeEnd); err != nil {
		return nil, err
	}
	if r.PrevKv {
		err := aa.as.IsRangePermitted(&aa.authInfo, r.Key, r.RangeEnd)
		if err != nil {
			return nil, err
		}
	}

	return aa.applierV3.DeleteRange(txn, r)
}

func checkTxnReqsPermission(as auth.AuthStore, ai *auth.AuthInfo, reqs []*pb.RequestOp) error {
	for _, requ := range reqs {
		switch tv := requ.Request.(type) {
		case *pb.RequestOp_RequestRange:
			if tv.RequestRange == nil {
				continue
			}

			if err := as.IsRangePermitted(ai, tv.RequestRange.Key, tv.RequestRange.RangeEnd); err != nil {
				return err
			}

		case *pb.RequestOp_RequestPut:
			if tv.RequestPut == nil {
				continue
			}

			if err := as.IsPutPermitted(ai, tv.RequestPut.Key); err != nil {
				return err
			}

		case *pb.RequestOp_RequestDeleteRange:
			if tv.RequestDeleteRange == nil {
				continue
			}

			if tv.RequestDeleteRange.PrevKv {
				err := as.IsRangePermitted(ai, tv.RequestDeleteRange.Key, tv.RequestDeleteRange.RangeEnd)
				if err != nil {
					return err
				}
			}

			err := as.IsDeleteRangePermitted(ai, tv.RequestDeleteRange.Key, tv.RequestDeleteRange.RangeEnd)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func checkTxnAuth(as auth.AuthStore, ai *auth.AuthInfo, rt *pb.TxnRequest) error {
	for _, c := range rt.Compare {
		if err := as.IsRangePermitted(ai, c.Key, c.RangeEnd); err != nil {
			return err
		}
	}
	if err := checkTxnReqsPermission(as, ai, rt.Success); err != nil {
		return err
	}
	return checkTxnReqsPermission(as, ai, rt.Failure)
}

func (aa *authApplierV3) Txn(rt *pb.TxnRequest) (*pb.TxnResponse, error) {
	if err := checkTxnAuth(aa.as, &aa.authInfo, rt); err != nil {
		return nil, err
	}
	return aa.applierV3.Txn(rt)
}

func (aa *authApplierV3) LeaseRevoke(lc *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	if err := aa.checkLeasePuts(lease.LeaseID(lc.ID)); err != nil {
		return nil, err
	}
	return aa.applierV3.LeaseRevoke(lc)
}

func (aa *authApplierV3) checkLeasePuts(leaseID lease.LeaseID) error {
	lease := aa.lessor.Lookup(leaseID)
	if lease != nil {
		for _, key := range lease.Keys() {
			if err := aa.as.IsPutPermitted(&aa.authInfo, []byte(key)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (aa *authApplierV3) UserGet(r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	err := aa.as.IsAdminPermitted(&aa.authInfo)
	if err != nil && r.Name != aa.authInfo.Username {
		aa.authInfo.Username = ""
		aa.authInfo.Revision = 0
		return &pb.AuthUserGetResponse{}, err
	}

	return aa.applierV3.UserGet(r)
}

func (aa *authApplierV3) RoleGet(r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	err := aa.as.IsAdminPermitted(&aa.authInfo)
	if err != nil && !aa.as.HasRole(aa.authInfo.Username, r.Role) {
		aa.authInfo.Username = ""
		aa.authInfo.Revision = 0
		return &pb.AuthRoleGetResponse{}, err
	}

	return aa.applierV3.RoleGet(r)
}

func needAdminPermission(r *pb.InternalRaftRequest) bool {
	switch {
	case r.AuthEnable != nil:
		return true
	case r.AuthDisable != nil:
		return true
	case r.AuthUserAdd != nil:
		return true
	case r.AuthUserDelete != nil:
		return true
	case r.AuthUserChangePassword != nil:
		return true
	case r.AuthUserGrantRole != nil:
		return true
	case r.AuthUserRevokeRole != nil:
		return true
	case r.AuthRoleAdd != nil:
		return true
	case r.AuthRoleGrantPermission != nil:
		return true
	case r.AuthRoleRevokePermission != nil:
		return true
	case r.AuthRoleDelete != nil:
		return true
	case r.AuthUserList != nil:
		return true
	case r.AuthRoleList != nil:
		return true
	default:
		return false
	}
}
