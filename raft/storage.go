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

package raft

import (
	"errors"
	"sync"

	pb "go.etcd.io/etcd/raft/raftpb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
//
// Storage 接口主要用于存储当前节点接收到的 Entry 记录.
type Storage interface {
	// TODO(tbg): split this into two interfaces, LogStorage and StateStorage.

	// InitialState returns the saved HardState and ConfState information.
	//
	// 返回 Storage 中记录的状态信息, 返回的是 HardState 实例和 ConfState 实例.
	//
	// 集群中每个节点都需要保存一些必需的基本信息, 在 etcd 中将其封装成 HardState, 其中主要封装了当前
	// 任期号（Term 字段）、当前节点在该任期中将选票投给了哪个节点（Vote 字段）、已提交 Entry 记录的
	// 位置（Commit 字段, 即最后一条已提交记录的索引值）
	//
	// ConfState 中封装了当前集群中所有节点的 ID（Nodes 字段）
	InitialState() (pb.HardState, pb.ConfState, error)
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	//
	// 在 Storage 中记录了当前节点的所有 Entry 记录, Entries 方法返回指定范围的 Entry 记录（[lo, hi)）,
	// 第三个参数 maxSize 限定了返回的 Entry 集合的字节数上限.
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	//
	// 查询指定 Index 对应的 Entry 的 Term 值.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	//
	// 该方法返回 Storage 中记录的最后一条 Entry 的索引值.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	//
	// 该方法返回 Storage 中记录的第一条 Entry 的索引值（Index）, 在该 Entry 之前的所有 Entry 都
	// 已经被包含进了最近的一次 SnapShot 中.
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	//
	// 返回最近一次生成的快照数据.
	Snapshot() (pb.Snapshot, error)
}

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
//
// MemoryStorage 是 etcd-raft 模块为 Storage 接口提供的一个实现. MemoryStorage 在内存中维护 hardState、snapshot
// 及所有的 Entry 记录（ents 字段）等信息
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	//
	// MemoryStorage 中的大部分操作是需要加锁同步的.
	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	//
	// 该字段维护了快照数据之后的所有 Entry 记录.
	ents []pb.Entry
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		// 最开始创建一个 Storage 实例时, 会在 ents 列表的首位填充一个空的 Entry 实例.
		ents: make([]pb.Entry, 1),
	}
}

// InitialState implements the Storage interface.
//
// InitialState 直接返回 hardState 字段中记录的 HardState 实例并使用快照的元数据中记录信息创建 ConfState 实例返回.
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
// SetHardState 将当前的 HardState 信息缓存到 MemoryStorage 的 hardState 字段中
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
//
// Entries 返回指定范围的 Entry 记录（[lo, hi)）, maxSize 限定了返回的 Entry 集合的字节数上限.
// Storage 中记录了自最近一次快照后所有接收到的 Entry 记录.
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	// 如果待查询的最小 Index 值（参数 lo）小于 FirstIndex, 则直接抛出异常
	if lo <= offset {
		return nil, ErrCompacted
	}
	// 如果待查询的最大 Index 值（参数 hi）大于 LastIndex, 则直接抛出异常
	if hi > ms.lastIndex()+1 {
		raftLogger.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	// 当前 Storage 是空的, 还未保存新的 Entry 记录
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	// 获取 Storage 中指定 [lo, hi) 范围的 Entry 记录
	ents := ms.ents[lo-offset : hi-offset]
	// 限定了返回的 Entry 集合的字节数上限. 超过 maxSize 大小的将被截断.
	return limitSize(ents, maxSize), nil
}

// Term implements the Storage interface.
//
// Term 查询指定 Index 对应的 Entry 的 Term 值.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

// lastIndex 返回当前 Storage 中保存的最后一条 Entry 记录的索引值
func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
//
// Snapshot 返回 Storage 中保存的快照数据
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
//
// ApplySnapshot 使用指定的最新的 SnapShot 更新 MemoryStorage 中的快照数据.
// 在节点重启时, 会通过读取快照文件创建对应的 SnapShot 实例, 然后保存到 MemoryStorage 中.
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	//handle check for old snapshot being applied
	// 通过快照的元数据比较当前 MemoryStorage 中记录的 Snapshot 与待处理的 SnapShot 数据的新旧程度
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	// 比较两个 pb.SnapShot 所包含的最后一条记录的 Index 值, 如果待处理 SnapShot 数据比较旧, 则直接抛出异常.
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate
	}

	// 更新 MemoryStorage.snapshot 字段
	ms.snapshot = snap
	// 重置 MemorySnapShot.ents 字段, 此时在 ents 中只有一个空的 Entry 实例
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
//
// i: 新建 SnapShot 包含的最大的索引值; cs: 当前集群的状态; data: 新建 SnapShot 的具体数据
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	// i 必须大于当前 SnapShot 包含的最大 Index 值
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	// i 必须小于 MemoryStorage 的 LastIndex 值, 否则抛出异常
	if i > ms.lastIndex() {
		raftLogger.Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	// 更新 MemoryStorage.snapshot 的元数据
	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	// 更新具体的快照数据
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
//
// Compact 新建 SnapShot 后, 一般会调用 Compact 函数将 MemoryStorage.ents 中指定索引之前的 Entry 记录全部抛弃,
// 从而实现压缩 MemoryStorage.ents 的目的.
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		raftLogger.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset
	// 创建新的切片, 用来存储 compactIndex 之后的 Entry.
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	// 将 compactIndex 之后的 Entry 拷贝到 ents 中, 并更新 MemoryStorage.ents 字段
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
//
// Append 向 MemoryStorage 中追加 Entry 记录.
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	// 获取当前 MemoryStorage 中保存的第一条 Entry 记录的索引值
	first := ms.firstIndex()
	// 获取待添加 Entry 记录数组中最后一条 Entry 记录的索引值
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	// 若待添加的 Entry 记录是旧的, 则无须再添加
	if last < first {
		return nil
	}
	// truncate compacted entries
	// first 之前的 Entry 记录已经记入 SnapShot 中, 不应该再记录到 ents 中, 所以将这部分 Entry 截掉.
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	// 计算 entries 切片中第一条可用的 Entry 与 first 之间的差距
	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		// 保留 MemoryStorage.ents 中 firs~offset 的部分, offset 之后的抛弃
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		// 然后将待追加的 Entry 追加到 MemoryStorage.ents 中
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		// 若待追加的 Entry 记录刚好与 MemoryStorage.ents 中保存的 Entry 记录索引上连续,
		// 则直接将待追加的日志记录 entries 追加到 MemoryStorage.ents 中.
		ms.ents = append(ms.ents, entries...)
	default:
		raftLogger.Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
