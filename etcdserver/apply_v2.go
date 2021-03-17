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
	"encoding/json"
	"fmt"
	"path"
	"time"

	"go.etcd.io/etcd/etcdserver/api"
	"go.etcd.io/etcd/etcdserver/api/membership"
	"go.etcd.io/etcd/etcdserver/api/v2store"
	"go.etcd.io/etcd/pkg/pbutil"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
)

// ApplierV2 is the interface for processing V2 raft messages
type ApplierV2 interface {
	Delete(r *RequestV2) Response
	Post(r *RequestV2) Response
	Put(r *RequestV2) Response
	QGet(r *RequestV2) Response
	Sync(r *RequestV2) Response
}

func NewApplierV2(lg *zap.Logger, s v2store.Store, c *membership.RaftCluster) ApplierV2 {
	return &applierV2store{lg: lg, store: s, cluster: c}
}

// applierV2store 结构体实现了 ApplierV2 接口.
type applierV2store struct {
	lg      *zap.Logger
	// 指向当前节点的 v2 存储
	store   v2store.Store
	cluster *membership.RaftCluster
}

func (a *applierV2store) Delete(r *RequestV2) Response {
	switch {
	case r.PrevIndex > 0 || r.PrevValue != "":
		return toResponse(a.store.CompareAndDelete(r.Path, r.PrevValue, r.PrevIndex))
	default:
		return toResponse(a.store.Delete(r.Path, r.Dir, r.Recursive))
	}
}

func (a *applierV2store) Post(r *RequestV2) Response {
	return toResponse(a.store.Create(r.Path, r.Dir, r.Val, true, r.TTLOptions()))
}

func (a *applierV2store) Put(r *RequestV2) Response {
	// 根据请求内容创建 TTLOptionSet 实例, 其中会设置节点的超时时间、此次请求是否为刷新操作
	ttlOptions := r.TTLOptions()
	// 修改节点之前是否存在
	exists, existsSet := pbutil.GetBool(r.PrevExist)
	switch {
	case existsSet:
		if exists {
			if r.PrevIndex == 0 && r.PrevValue == "" {
				// 未提供 PreIndex 和 PreValue 信息, 则直接调用 Update() 方法更新节点值
				return toResponse(a.store.Update(r.Path, r.Val, ttlOptions))
			}
			// 提供了 PreIndex 和 PreValue 信息, 则调用 CompareAndSwap() 方法更新节点值.
			// CompareAndSwap() 方法会比较当前节点的 PreIndex 和 PreValue 是否与此次操作
			// 提供的值相同, 然后决定是否修改.
			return toResponse(a.store.CompareAndSwap(r.Path, r.PrevValue, r.PrevIndex, r.Val, ttlOptions))
		}
		return toResponse(a.store.Create(r.Path, r.Dir, r.Val, false, ttlOptions))
	case r.PrevIndex > 0 || r.PrevValue != "":
		return toResponse(a.store.CompareAndSwap(r.Path, r.PrevValue, r.PrevIndex, r.Val, ttlOptions))
	default:
		// 操作的节点是 "/0/members" 下的节点集群成员信息节点, 注意, 不会修改 v2 存储中对应的节点
		if storeMemberAttributeRegexp.MatchString(r.Path) {
			// 从节点的路径信息中解析得到节点 id
			id := membership.MustParseMemberIDFromKey(path.Dir(r.Path))
			var attr membership.Attributes
			// 将节点值反序列化
			if err := json.Unmarshal([]byte(r.Val), &attr); err != nil {
				if a.lg != nil {
					a.lg.Panic("failed to unmarshal", zap.String("value", r.Val), zap.Error(err))
				} else {
					plog.Panicf("unmarshal %s should never fail: %v", r.Val, err)
				}
			}
			// 更新 RaftCluster 中对应节点的信息
			if a.cluster != nil {
				a.cluster.UpdateAttributes(id, attr)
			}
			// return an empty response since there is no consumer.
			return Response{}
		}
		// 操作的节点是 "/0/version"
		if r.Path == membership.StoreClusterVersionKey() {
			// 更新 RaftCluster 中的版本信息
			if a.cluster != nil {
				a.cluster.SetVersion(semver.Must(semver.NewVersion(r.Val)), api.UpdateCapability)
			}
			// return an empty response since there is no consumer.
			return Response{}
		}
		// 如果不是上述两种节点, 则直接调用 Set() 方法更新对应节点值
		return toResponse(a.store.Set(r.Path, r.Dir, r.Val, ttlOptions))
	}
}

func (a *applierV2store) QGet(r *RequestV2) Response {
	return toResponse(a.store.Get(r.Path, r.Recursive, r.Sorted))
}

func (a *applierV2store) Sync(r *RequestV2) Response {
	a.store.DeleteExpiredKeys(time.Unix(0, r.Time))
	return Response{}
}

// applyV2Request interprets r as a call to v2store.X
// and returns a Response interpreted from v2store.Event
func (s *EtcdServer) applyV2Request(r *RequestV2) Response {
	stringer := panicAlternativeStringer{
		stringer:    r,
		alternative: func() string { return fmt.Sprintf("id:%d,method:%s,path:%s", r.ID, r.Method, r.Path) },
	}
	defer warnOfExpensiveRequest(s.getLogger(), time.Now(), stringer, nil, nil)

	switch r.Method {
	case "POST":
		return s.applyV2.Post(r)
	case "PUT":
		return s.applyV2.Put(r)
	case "DELETE":
		return s.applyV2.Delete(r)
	case "QGET":
		return s.applyV2.QGet(r)
	case "SYNC":
		return s.applyV2.Sync(r)
	default:
		// This should never be reached, but just in case:
		return Response{Err: ErrUnknownMethod}
	}
}

func (r *RequestV2) TTLOptions() v2store.TTLOptionSet {
	refresh, _ := pbutil.GetBool(r.Refresh)
	ttlOptions := v2store.TTLOptionSet{Refresh: refresh}
	if r.Expiration != 0 {
		ttlOptions.ExpireTime = time.Unix(0, r.Expiration)
	}
	return ttlOptions
}

func toResponse(ev *v2store.Event, err error) Response {
	return Response{Event: ev, Err: err}
}
