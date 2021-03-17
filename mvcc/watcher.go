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

package mvcc

import (
	"bytes"
	"errors"
	"sync"

	"go.etcd.io/etcd/mvcc/mvccpb"
)

// AutoWatchID is the watcher ID passed in WatchStream.Watch when no
// user-provided ID is available. If pass, an ID will automatically be assigned.
const AutoWatchID WatchID = 0

var (
	ErrWatcherNotExist    = errors.New("mvcc: watcher does not exist")
	ErrEmptyWatcherRange  = errors.New("mvcc: watcher range is empty")
	ErrWatcherDuplicateID = errors.New("mvcc: duplicate watch ID provided on the WatchStream")
)

type WatchID int64

// FilterFunc returns true if the given event should be filtered out.
type FilterFunc func(e mvccpb.Event) bool

type WatchStream interface {
	// Watch creates a watcher. The watcher watches the events happening or
	// happened on the given key or range [key, end) from the given startRev.
	//
	// The whole event history can be watched unless compacted.
	// If "startRev" <=0, watch observes events after currentRev.
	//
	// The returned "id" is the ID of this watcher. It appears as WatchID
	// in events that are sent to the created watcher through stream channel.
	// The watch ID is used when it's not equal to AutoWatchID. Otherwise,
	// an auto-generated watch ID is returned.
	Watch(id WatchID, key, end []byte, startRev int64, fcs ...FilterFunc) (WatchID, error)

	// Chan returns a chan. All watch response will be sent to the returned chan.
	Chan() <-chan WatchResponse

	// RequestProgress requests the progress of the watcher with given ID. The response
	// will only be sent if the watcher is currently synced.
	// The responses will be sent through the WatchRespone Chan attached
	// with this stream to ensure correct ordering.
	// The responses contains no events. The revision in the response is the progress
	// of the watchers since the watcher is currently synced.
	RequestProgress(id WatchID)

	// Cancel cancels a watcher by giving its ID. If watcher does not exist, an error will be
	// returned.
	Cancel(id WatchID) error

	// Close closes Chan and release all related resources.
	Close()

	// Rev returns the current revision of the KV the stream watches on.
	Rev() int64
}

// WatchResponse 表示一次响应, 其中封装了多个 Event 实例.
type WatchResponse struct {
	// WatchID is the WatchID of the watcher this response sent to.
	//
	// 被触发的 watcher 实例的唯一标识
	WatchID WatchID

	// Events contains all the events that needs to send.
	//
	// 触发对应 watcher 事件集合
	Events []mvccpb.Event

	// Revision is the revision of the KV when the watchResponse is created.
	// For a normal response, the revision should be the same as the last
	// modified revision inside Events. For a delayed response to a unsynced
	// watcher, the revision is greater than the last modified revision
	// inside Events.
	//
	// 当前 watchResponse 实例创建时对应的 revision 值.
	Revision int64

	// CompactRevision is set when the watcher is cancelled due to compaction.
	//
	// 如果因为压缩操作对应 watcher 实例被取消了, 则该字段设置为压缩操作对应的 revision.
	CompactRevision int64
}

// watchStream contains a collection of watchers that share
// one streaming chan to send out watched events and other control events.
type watchStream struct {
	// 用于记录关联的 watchableStore 实例
	watchable watchable
	// 通过该 watchStream 实例创建的 watcher 实例在被触发时, 都会将 Event 事件写入该通道中.
	ch        chan WatchResponse

	mu sync.Mutex // guards fields below it
	// nextID is the ID pre-allocated for next new watcher in this stream
	//
	// 在当前 watchStream 实例中添加 watcher 实例时, 会为其分配唯一标识, 该字段就用来生成该唯一标识.
	nextID   WatchID
	// 标识当前 watchStream 是否已关闭
	closed   bool
	// 该字段记录唯一标识与取消对应 watcher 的回调函数之间的映射关系
	cancels  map[WatchID]cancelFunc
	// 该字段记录唯一标识与对应 watcher 实例之间的映射关系
	watchers map[WatchID]*watcher
}

// Watch creates a new watcher in the stream and returns its WatchID.
//
// Watch 创建 watcher 实例监听指定的 Key（或是范围监听 [key, end]）, 其中 startRev 参数则指定了该 watcher 实例
// 监听的起始 revision, 如果 startRev 参数小于等于 0, 则表示从当前 revision 开始监听, fcs 参数则是 Event 事件
// 过滤器.
func (ws *watchStream) Watch(id WatchID, key, end []byte, startRev int64, fcs ...FilterFunc) (WatchID, error) {
	// prevent wrong range where key >= end lexicographically
	// watch request with 'WithFromKey' has empty-byte range end
	//
	// 检测参数的合法性
	if len(end) != 0 && bytes.Compare(key, end) != -1 {
		return -1, ErrEmptyWatcherRange
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()
	// 检测当前 watchStream 是否已关闭
	if ws.closed {
		return -1, ErrEmptyWatcherRange
	}

	// 若使用 watchStream 中的 nextID 字段来自动生成新建 watcher 的唯一标识
	if id == AutoWatchID {
		// 若 nextID 对应的 watcher 已存在, 则递增 nextID
		for ws.watchers[ws.nextID] != nil {
			ws.nextID++
		}
		id = ws.nextID // 新建 watcher 的唯一标识
		ws.nextID++

	// 若不使用自动生成, 而是使用传入的指定 id 作为新建 watcher 的唯一标识, 则检测该 id 是否唯一
	} else if _, ok := ws.watchers[id]; ok {
		return -1, ErrWatcherDuplicateID
	}

	// 调用 watchableStore.watch() 方法创建 watcher 实例, 新建的 watcher 实例共用了同一个通道
	w, c := ws.watchable.watch(key, end, startRev, id, ws.ch, fcs...)

	ws.cancels[id] = c  // 保存取消 watcher 实例时用到的回调函数
	ws.watchers[id] = w // 记录新建的 watcher 实例
	return id, nil
}

func (ws *watchStream) Chan() <-chan WatchResponse {
	return ws.ch
}

// Cancel 查找并调用指定 watcher 实例对应的取消回调函数
func (ws *watchStream) Cancel(id WatchID) error {
	ws.mu.Lock() // 加锁
	// 根据 watcher 唯一标识, 查找对应的取消回调函数
	cancel, ok := ws.cancels[id]
	w := ws.watchers[id]
	ok = ok && !ws.closed
	ws.mu.Unlock()

	// watcher 不存在
	if !ok {
		return ErrWatcherNotExist
	}
	// 调用回调函数, 取消 watcher 实例
	cancel()

	ws.mu.Lock()
	// The watch isn't removed until cancel so that if Close() is called,
	// it will wait for the cancel. Otherwise, Close() could close the
	// watch channel while the store is still posting events.
	if ww := ws.watchers[id]; ww == w {
		delete(ws.cancels, id)  // 从 watchStream.cancels 字段中清理对应的取消回调函数
		delete(ws.watchers, id) // 从 watchStream.watchers 字段中清理对应的 watcher 实例
	}
	ws.mu.Unlock()

	return nil
}

func (ws *watchStream) Close() {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for _, cancel := range ws.cancels {
		cancel()
	}
	ws.closed = true
	close(ws.ch)
	watchStreamGauge.Dec()
}

func (ws *watchStream) Rev() int64 {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.watchable.rev()
}

// RequestProgress 检测指定 watcher 的处理进度（progress, 即当前 watcher 正在处理哪个 revision 中的更新操作）.
// 只有当 watcher 完全同步时, 调用该方法才会创建一个空的 watchResponse, 并写入 watcher.ch 通道中.
func (ws *watchStream) RequestProgress(id WatchID) {
	ws.mu.Lock()
	// 从 watchStream.watchers 中查询指定的 watcher 实例
	w, ok := ws.watchers[id]
	ws.mu.Unlock()
	if !ok {
		return
	}
	// 调用 watchableStore.progress() 方法
	ws.watchable.progress(w)
}
