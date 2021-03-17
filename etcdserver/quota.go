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
	"sync"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"

	humanize "github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

const (
	// DefaultQuotaBytes is the number of bytes the backend Size may
	// consume before exceeding the space quota.
	DefaultQuotaBytes = int64(2 * 1024 * 1024 * 1024) // 2GB
	// MaxQuotaBytes is the maximum number of bytes suggested for a backend
	// quota. A larger quota may lead to degraded performance.
	MaxQuotaBytes = int64(8 * 1024 * 1024 * 1024) // 8GB
)

// Quota represents an arbitrary quota against arbitrary requests. Each request
// costs some charge; if there is not enough remaining charge, then there are
// too few resources available within the quota to apply the request.
//
// Quota 接口定义了实现限流功能的核心.
type Quota interface {
	// Available judges whether the given request fits within the quota.
	//
	// 检测此次请求是否能通过限流, 即当前请求执行后, 未达到系统负载的上限, 也就未触发限流.
	Available(req interface{}) bool
	// Cost computes the charge against the quota for a given request.
	//
	// 计算此次请求所产生的负载, 该方法主要在 Available() 方法中调用
	Cost(req interface{}) int
	// Remaining is the amount of charge left for the quota.
	//
	// 当前系统所能支持的剩余负载量.
	Remaining() int64
}

// passthroughQuota 也是 Quota 接口的实现之一, 但它并没有实现限流的功能,
// 其 Available() 方法始终返回 true, Cose() 方法始终返回 0.
type passthroughQuota struct{}

func (*passthroughQuota) Available(interface{}) bool { return true }
func (*passthroughQuota) Cost(interface{}) int       { return 0 }
func (*passthroughQuota) Remaining() int64           { return 1 }

// backendQuota 结构体是 Quota 接口的实现之一, 它主要用于限制底层 BoltDB 中的数据量, 其中
// 封装了当前节点的 EtcdServer 实例（即 s 字段）和 BoltDB 数据量的上限值（maxBackendBytes 字段）.
type backendQuota struct {
	s               *EtcdServer // 指向当前节点的 EtcdServer 实例
	maxBackendBytes int64       // BoltDB 数据量的上限值
}

const (
	// leaseOverhead is an estimate for the cost of storing a lease
	leaseOverhead = 64
	// kvOverhead is an estimate for the cost of storing a key's metadata
	kvOverhead = 256
)

var (
	// only log once
	quotaLogOnce sync.Once

	DefaultQuotaSize = humanize.Bytes(uint64(DefaultQuotaBytes))
	maxQuotaSize     = humanize.Bytes(uint64(MaxQuotaBytes))
)

// NewBackendQuota creates a quota layer with the given storage limit.
//
// NewBackendQuota 根据传入的参数初始化相应的 Quota 实例
func NewBackendQuota(s *EtcdServer, name string) Quota {
	lg := s.getLogger()
	quotaBackendBytes.Set(float64(s.Cfg.QuotaBackendBytes))

	if s.Cfg.QuotaBackendBytes < 0 {
		// disable quotas if negative
		quotaLogOnce.Do(func() {
			if lg != nil {
				lg.Info(
					"disabled backend quota",
					zap.String("quota-name", name),
					zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes),
				)
			} else {
				plog.Warningf("disabling backend quota")
			}
		})
		// 创建 passthroughQuota 实例, 它也是 Quota 接口的实现之一, 但它并没有实现限流的功能,
		// 其 Available() 方法始终返回 true, Cost() 方法始终返回 0.
		return &passthroughQuota{}
	}

	// 没有指定 QuotaBackendBytes 值, 则使用默认的 2GB
	if s.Cfg.QuotaBackendBytes == 0 {
		// use default size if no quota size given
		quotaLogOnce.Do(func() {
			if lg != nil {
				lg.Info(
					"enabled backend quota with default value",
					zap.String("quota-name", name),
					zap.Int64("quota-size-bytes", DefaultQuotaBytes),
					zap.String("quota-size", DefaultQuotaSize),
				)
			}
		})
		// 创建 backendQuota 实例
		quotaBackendBytes.Set(float64(DefaultQuotaBytes))
		return &backendQuota{s, DefaultQuotaBytes}
	}

	quotaLogOnce.Do(func() {
		if s.Cfg.QuotaBackendBytes > MaxQuotaBytes {
			if lg != nil {
				lg.Warn(
					"quota exceeds the maximum value",
					zap.String("quota-name", name),
					zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes),
					zap.String("quota-size", humanize.Bytes(uint64(s.Cfg.QuotaBackendBytes))),
					zap.Int64("quota-maximum-size-bytes", MaxQuotaBytes),
					zap.String("quota-maximum-size", maxQuotaSize),
				)
			} else {
				plog.Warningf("backend quota %v exceeds maximum recommended quota %v", s.Cfg.QuotaBackendBytes, MaxQuotaBytes)
			}
		}
		if lg != nil {
			lg.Info(
				"enabled backend quota",
				zap.String("quota-name", name),
				zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes),
				zap.String("quota-size", humanize.Bytes(uint64(s.Cfg.QuotaBackendBytes))),
			)
		}
	})
	return &backendQuota{s, s.Cfg.QuotaBackendBytes}
}

// Available 将当前 BoltDB 中的数据量、此次请求的数据量之和与上限阈值进行比较,
// 从而决定此次请求是否触发限流.
func (b *backendQuota) Available(v interface{}) bool {
	// TODO: maybe optimize backend.Size()
	return b.s.Backend().Size()+int64(b.Cost(v)) < b.maxBackendBytes
}

// Cost 根据请求的类型计算请求的数据量.
func (b *backendQuota) Cost(v interface{}) int {
	switch r := v.(type) {
	case *pb.PutRequest:
		return costPut(r)
	case *pb.TxnRequest:
		return costTxn(r)
	case *pb.LeaseGrantRequest:
		return leaseOverhead
	default:
		panic("unexpected cost")
	}
}

// costPut 计算请求的 Key 值、Value 值和相关元数据的字节数之和.
func costPut(r *pb.PutRequest) int { return kvOverhead + len(r.Key) + len(r.Value) }

func costTxnReq(u *pb.RequestOp) int {
	r := u.GetRequestPut()
	if r == nil {
		return 0
	}
	return costPut(r)
}

// costTxn 底层是调用 costPut() 方法计算 TxnRequest.Success 和 TxnRequest.Failure.
func costTxn(r *pb.TxnRequest) int {
	sizeSuccess := 0
	for _, u := range r.Success {
		sizeSuccess += costTxnReq(u)
	}
	sizeFailure := 0
	for _, u := range r.Failure {
		sizeFailure += costTxnReq(u)
	}
	if sizeFailure > sizeSuccess {
		return sizeFailure
	}
	return sizeSuccess
}

func (b *backendQuota) Remaining() int64 {
	return b.maxBackendBytes - b.s.Backend().Size()
}
