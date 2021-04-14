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
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/types"
)

// quotaKVServer 内嵌 pb.KVServer, 在 pb.KVServer 的基础上扩展了写入数据限额功能, 即会先检查当前 etcd db
// 大小加上请求的 key-value 大小之和是否超过了配额（quota-backend-bytes）, 若超过了配额, 则产生 NO SPACE
// 告警, 并通过 Raft 日志同步给其他节点, 告知 db 无可用空间, 并将告警持久化存储到 db 中, 后续将无法写入.
type quotaKVServer struct {
	// 指向 kvServer 实例
	pb.KVServer
	qa quotaAlarmer
}

type quotaAlarmer struct {
	// 开启了配额限制后, 指向 etcdserver.backendQuota 实例;
	// 没有开启配额限制, 则指向 etcdserver.passthroughQuota 实例.
	q  etcdserver.Quota
	// 指向 etcdserver.EtcdServer 实例
	a  Alarmer
	// 当前节点 ID
	id types.ID
}

// check whether request satisfies the quota. If there is not enough space,
// ignore request and raise the free space alarm.
//
// check 检查当前 etcd db 大小加上请求的 key-value 大小之和是否超过了配额（quota-backend-bytes）.
// 如果超过了配额, 它会产生一个告警（Alarm）请求, 告警类型是 NO SPACE, 并通过 Raft 日志同步给其他节点,
// 告知 db 无空间了, 并将告警持久化存储到 db 中.
//
// 最终, 无论是 API 层 gRPC 模块还是负责将 Raft 侧已提交的日志条目应用到状态机的 Apply 模块, 都拒绝写入,
// 集群只读.
func (qa *quotaAlarmer) check(ctx context.Context, r interface{}) error {
	// 检测当前请求的数据量加上 db 大小后是否超过了配额
	// 若开启了配额限制, 则调用 backendQuota.Available() 方法
	if qa.q.Available(r) {
		// 没有超过配额, 则直接返回
		return nil
	}
	// 超过了配额, 则创建一个 AlarmType_NOSPACE 类型的 AlarmRequest 请求
	req := &pb.AlarmRequest{
		MemberID: uint64(qa.id),
		Action:   pb.AlarmRequest_ACTIVATE,
		Alarm:    pb.AlarmType_NOSPACE,
	}
	qa.a.Alarm(ctx, req)
	return rpctypes.ErrGRPCNoSpace
}

// NewQuotaKVServer 创建 quotaKVServer 实例
func NewQuotaKVServer(s *etcdserver.EtcdServer) pb.KVServer {
	return &quotaKVServer{
		NewKVServer(s),
		quotaAlarmer{etcdserver.NewBackendQuota(s, "kv"), s, s.ID()},
	}
}

func (s *quotaKVServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	// 检测当前 etcd db 大小加上请求的 key-value 大小之和是否超过了配额限制
	if err := s.qa.check(ctx, r); err != nil {
		return nil, err
	}
	return s.KVServer.Put(ctx, r)
}

func (s *quotaKVServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	// 检测当前 etcd db 大小加上请求的 key-value 大小之和是否超过了配额限制
	if err := s.qa.check(ctx, r); err != nil {
		return nil, err
	}
	// 调用 kvServer.Txn() 方法
	return s.KVServer.Txn(ctx, r)
}

type quotaLeaseServer struct {
	pb.LeaseServer
	qa quotaAlarmer
}

// LeaseGrant 创建一个 TTL 为指定秒数的 Lease, 底层的 Lessor 会将 Lease 信息持久化存储在 BoltDB 中.
func (s *quotaLeaseServer) LeaseGrant(ctx context.Context, cr *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	// 检测加上传入的数据量后 BoltDB 是否超过了配额限制
	if err := s.qa.check(ctx, cr); err != nil {
		return nil, err
	}
	// 调用 LeaseServer.LeaseGrant() 进行处理
	return s.LeaseServer.LeaseGrant(ctx, cr)
}

func NewQuotaLeaseServer(s *etcdserver.EtcdServer) pb.LeaseServer {
	return &quotaLeaseServer{
		NewLeaseServer(s),
		quotaAlarmer{etcdserver.NewBackendQuota(s, "lease"), s, s.ID()},
	}
}
