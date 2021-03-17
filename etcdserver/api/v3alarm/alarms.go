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

// Package v3alarm manages health status alarms in etcd.
package v3alarm

import (
	"sync"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/pkg/types"

	"github.com/coreos/pkg/capnslog"
)

var (
	alarmBucketName = []byte("alarm")
	plog            = capnslog.NewPackageLogger("go.etcd.io/etcd", "alarm")
)

type BackendGetter interface {
	Backend() backend.Backend
}

// alarmSet 记录了节点 ID 与 AlarmMember 之间的映射关系.
type alarmSet map[types.ID]*pb.AlarmMember

// AlarmStore persists alarms to the backend.
//
// AlarmStore 与 quotaApplierV3 配合实现限流功能.
type AlarmStore struct {
	mu    sync.Mutex
	// 在该 map 字段中记录了每种 AlarmType 对应的 AlarmMember 实例. AlarmType 现在有 AlarmType_NONE、AlarmType_NOSPACE
	// 以及 AlarmType_CORRUPT 三种类型. alarmSet 类型实际上是 map[types.ID]*pb.AlarmMember 类型, 其中记录了节点 ID
	// 与 AlarmMember 之间的映射关系.
	types map[pb.AlarmType]alarmSet

	// BackendGetter 接口用于返回该 AlarmStore 实例使用的存储. EtcdServer 就是 BackendGetter 接口的实现之一,
	// 返回的就是其底层使用的 backend 实例.
	bg BackendGetter
}

func NewAlarmStore(bg BackendGetter) (*AlarmStore, error) {
	ret := &AlarmStore{types: make(map[pb.AlarmType]alarmSet), bg: bg}
	err := ret.restore()
	return ret, err
}

// Activate 负责新建 AlarmMember 实例, 并将其记录到 AlarmStore.types 字段和底层存储中.
func (a *AlarmStore) Activate(id types.ID, at pb.AlarmType) *pb.AlarmMember {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 新建 AlarmMember 实例
	newAlarm := &pb.AlarmMember{MemberID: uint64(id), Alarm: at}
	// 将 AlarmMember 实例添加到 types 字段中
	if m := a.addToMap(newAlarm); m != newAlarm {
		return m
	}

	// 将 AlarmMember 实例序列化
	v, err := newAlarm.Marshal()
	if err != nil {
		plog.Panicf("failed to marshal alarm member")
	}

	// 获取底层的存储接口
	b := a.bg.Backend()
	b.BatchTx().Lock()
	// 将 AlarmMember 持久化到底层存储中
	b.BatchTx().UnsafePut(alarmBucketName, v, nil)
	b.BatchTx().Unlock()

	return newAlarm
}

// Deactivate 负责从 types 字段和底层存储中删除指定的 AlarmMember 实例.
func (a *AlarmStore) Deactivate(id types.ID, at pb.AlarmType) *pb.AlarmMember {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 根据 AlarmType 类型查找 alarmSet
	t := a.types[at]
	if t == nil {
		t = make(alarmSet)
		a.types[at] = t
	}
	// 根据 id 查找 AlarmMember 实例
	m := t[id]
	if m == nil {
		return nil
	}

	// 从 types 字段中删除指定的 AlarmMember 实例
	delete(t, id)

	v, err := m.Marshal()
	if err != nil {
		plog.Panicf("failed to marshal alarm member")
	}

	// 从底层存储中删除指定的 AlarmMember 信息
	b := a.bg.Backend()
	b.BatchTx().Lock()
	b.BatchTx().UnsafeDelete(alarmBucketName, v)
	b.BatchTx().Unlock()

	return m
}

func (a *AlarmStore) Get(at pb.AlarmType) (ret []*pb.AlarmMember) {
	a.mu.Lock()
	defer a.mu.Unlock()
	// 针对 AlarmType_NONE 类型的 AlarmType 处理
	if at == pb.AlarmType_NONE {
		// 遍历获取 types 字段中全部的 AlarmMember 实例并返回
		for _, t := range a.types {
			for _, m := range t {
				ret = append(ret, m)
			}
		}
		return ret
	}
	// 只返回指定类型的 AlarmMember 实例
	for _, m := range a.types[at] {
		ret = append(ret, m)
	}
	return ret
}

func (a *AlarmStore) restore() error {
	b := a.bg.Backend()
	tx := b.BatchTx()

	tx.Lock()
	tx.UnsafeCreateBucket(alarmBucketName)
	err := tx.UnsafeForEach(alarmBucketName, func(k, v []byte) error {
		var m pb.AlarmMember
		if err := m.Unmarshal(k); err != nil {
			return err
		}
		a.addToMap(&m)
		return nil
	})
	tx.Unlock()

	b.ForceCommit()
	return err
}

func (a *AlarmStore) addToMap(newAlarm *pb.AlarmMember) *pb.AlarmMember {
	t := a.types[newAlarm.Alarm]
	if t == nil {
		t = make(alarmSet)
		a.types[newAlarm.Alarm] = t
	}
	m := t[types.ID(newAlarm.MemberID)]
	if m != nil {
		return m
	}
	t[types.ID(newAlarm.MemberID)] = newAlarm
	return newAlarm
}
