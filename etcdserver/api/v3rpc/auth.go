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

package v3rpc

import (
	"context"

	"go.etcd.io/etcd/etcdserver"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
)

// AuthServer 认证服务器
type AuthServer struct {
	// 指向 etcdserver.EtcdServer 实现
	authenticator etcdserver.Authenticator
}

func NewAuthServer(s *etcdserver.EtcdServer) *AuthServer {
	return &AuthServer{authenticator: s}
}

// AuthEnable 开启鉴权
func (as *AuthServer) AuthEnable(ctx context.Context, r *pb.AuthEnableRequest) (*pb.AuthEnableResponse, error) {
	resp, err := as.authenticator.AuthEnable(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

// AuthDisable 关闭鉴权
func (as *AuthServer) AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error) {
	resp, err := as.authenticator.AuthDisable(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error) {
	resp, err := as.authenticator.Authenticate(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	resp, err := as.authenticator.RoleAdd(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	resp, err := as.authenticator.RoleDelete(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	resp, err := as.authenticator.RoleGet(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	resp, err := as.authenticator.RoleList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	resp, err := as.authenticator.RoleRevokePermission(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	resp, err := as.authenticator.RoleGrantPermission(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

// UserAdd 添加一个用户
func (as *AuthServer) UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	// 调用 etcdserver.EtcdServer.UserAdd() 函数
	resp, err := as.authenticator.UserAdd(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

// UserDelete 删除指定用户
func (as *AuthServer) UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	resp, err := as.authenticator.UserDelete(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	resp, err := as.authenticator.UserGet(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	resp, err := as.authenticator.UserList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	resp, err := as.authenticator.UserGrantRole(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *AuthServer) UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	resp, err := as.authenticator.UserRevokeRole(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

// UserChangePassword 更改指定用户密码
func (as *AuthServer) UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	resp, err := as.authenticator.UserChangePassword(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}
