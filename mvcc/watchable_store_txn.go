// Copyright 2017 The etcd Authors
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

package mvcc

import (
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/traceutil"
)

// End 重写了内嵌的 TxnWrite 接口的 End() 方法, 在该方法中, 如果检测到当前读写事务中存在更新操作, 则会触发相应的 watcher 实例.
func (tw *watchableStoreTxnWrite) End() {
	changes := tw.Changes() // 当前读写事务中执行的更新操作
	// 如果当前读写事务中没有任何更新操作, 则直接结束当前事务即可
	if len(changes) == 0 {
		// 结束当前读写事务
		tw.TxnWrite.End()
		return
	}

	rev := tw.Rev() + 1
	evs := make([]mvccpb.Event, len(changes))
	// 将当前事务中的更新操作转换成对应的 Event 事件
	for i, change := range changes {
		evs[i].Kv = &changes[i]
		if change.CreateRevision == 0 {
			evs[i].Type = mvccpb.DELETE
			evs[i].Kv.ModRevision = rev
		} else {
			evs[i].Type = mvccpb.PUT
		}
	}

	// end write txn under watchable store lock so the updates are visible
	// when asynchronous event posting checks the current store revision
	tw.s.mu.Lock()
	// 调用 watchableStore.notify() 方法, 将上述 Event 事件发送出去
	tw.s.notify(rev, evs)
	// 结束当前读写事务
	tw.TxnWrite.End()
	tw.s.mu.Unlock()
}

type watchableStoreTxnWrite struct {
	TxnWrite          // 内嵌了 TxnWrite, 并且 watchableStoreTxnWrite 重写了 End() 方法
	s *watchableStore // 关联的 watchableStore 实例
}

// Write 重写了内嵌的 store 的 Write() 方法
func (s *watchableStore) Write(trace *traceutil.Trace) TxnWrite {
	return &watchableStoreTxnWrite{s.store.Write(trace), s}
}
