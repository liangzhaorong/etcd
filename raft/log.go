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
	"fmt"
	"log"

	pb "go.etcd.io/etcd/raft/raftpb"
)

// raftLog Raft 协议中日志复制部分的核心就是在集群中各个节点之间完成日志的复制, etcd-raft 模块的实现中使用
// raftLog 结构体来管理节点上的日志.
type raftLog struct {
	// storage contains all stable entries since the last snapshot.
	//
	// 指向 MemoryStorage 实例, 其中存储了快照数据以及该快照之后的 Entry 记录. 别称 "stable storage".
	storage Storage

	// unstable contains all unstable entries and snapshot.
	// they will be saved into storage.
	//
	// 用于存储未写入 Storage 的快照数据以及 Entry 记录.
	unstable unstable

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	//
	// 已提交的位置, 即已提交的 Entry 记录中最大的索引值. (已追加到 raftLog.unstable 中的 Entry 记录即算已提交)
	committed uint64
	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	//
	// 已应用的位置, 即已应用的 Entry 记录中最大的索引值. 其中 committed 和 applied 之间始终满足
	// applied <= committed 这个不等式关系.
	applied uint64

	logger Logger

	// maxNextEntsSize is the maximum number aggregate byte size of the messages
	// returned from calls to nextEnts.
	//
	// maxNextEntsSize 是从对 nextEnts 函数的调用返回的消息的最大总字节数大小.
	maxNextEntsSize uint64
}

// newLog returns log using the given storage and default options. It
// recovers the log to the state that it just commits and applies the
// latest snapshot.
//
// newLog 创建一个使用指定 Storage 和带有默认选项的 raftLog 实例, 并返回该实例.
func newLog(storage Storage, logger Logger) *raftLog {
	return newLogWithSize(storage, logger, noLimit)
}

// newLogWithSize returns a log using the given storage and max
// message size.
func newLogWithSize(storage Storage, logger Logger, maxNextEntsSize uint64) *raftLog {
	// 必须指定要使用的 Storage 实例
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	// 创建 raftLog 实例, 并初始化 storage 字段
	log := &raftLog{
		storage:         storage,
		logger:          logger,
		maxNextEntsSize: maxNextEntsSize,
	}
	// 获取 Storage 中的第一条 Entry 记录的索引值
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	// 获取 Storage 中最后一条 Entry 记录的索引值
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	// 初始化 unstable.offset 的值为 Storage 的 lastIndex+1
	log.unstable.offset = lastIndex + 1
	log.unstable.logger = logger
	// Initialize our committed and applied pointers to the time of the last compaction.
	// 初始化 committed、applied 字段
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
//
// maybeAppend 当 Follower 节点或 Candidate 节点需要向 raftLog 中追加 Entry 记录时, 会通过 raft.handleAppendEntries()
// 方法调用 raftLog.maybeAppend 方法完成追加 Entry 记录的功能.
//
// index: 表示 Leader 发送给 Follower 的上一条日志的索引值;
// logTerm: 是 MsgApp 消息的 LogTerm 字段, 通过消息中携带的该 Term 值与当前节点记录的 Term 进行比较, 就可以判断
// 消息是否为过时的消息;
// committed: 是 MsgApp 消息的 Commit 字段, Leader 节点通过该字段通知 Follower 节点当前已提交 Entry 的位置;
// ents: 是 MsgApp 消息中携带的 Entry 记录, 即待追加到 raftLog 中的 Entry 记录.
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	// 检测待追加消息（如 MsgApp）的 Index 字段及 logTerm 字段是否合法
	if l.matchTerm(index, logTerm) {
		// 待追加 Entry 集合中最后一个 Entry 记录的索引值
		lastnewi = index + uint64(len(ents))
		// findConflict 的返回值: 当待追加的 Entry 记录在 raftLog 中不存在时, 会返回第一条不存在 Entry 记录的索引值;
		// raftLog 包含全部的待追加记录且没有发生冲突, 则返回 0.
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
			// findConflict 返回 0 时, 表示 raftLog 中已经包含了所有待追加的 Entry 记录, 不必进行任何追加操作.
		case ci <= l.committed:
			// 如果出现冲突的位置是已提交的记录, 则输出异常日志并终止整个程序.
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			// 如果冲突位置是未提交的部分
			offset := index + 1
			// 则将 ents 中未发生冲突的部分追加到 raftLog 中（这里先追加到 unstable 中）
			l.append(ents[ci-offset:]...)
		}
		// 更新 raftLog 的 committed 字段
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

// append 将 ents 中的 Entry 记录追加到 raftLog 中
func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	// 调用 unstable.truncateAndAppend 方法将 Entry 记录追加到 unstable 中
	l.unstable.truncateAndAppend(ents)
	// 返回 raftLog 最后一条日志记录的索引
	return l.lastIndex()
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The first entry MUST have an index equal to the argument 'from'.
// The index of the given entries MUST be continuously increasing.
//
// findConflict 的返回值: 当待追加的 Entry 记录在 raftLog 中不存在时, 会返回第一条不存在 Entry 记录的索引值;
// raftLog 包含全部的待追加记录且没有发生冲突, 则返回 0.
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	// 遍历待追加的 Entry 集合, 查找是否与 raftLog 中已有的 Entry 发生冲突（Index 相同但 Term 不同）
	for _, ne := range ents {
		// 查找冲突的 Entry 记录（即不存在当前 raftLog 中的首个 Entry 记录）
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			// 返回冲突记录的索引值
			return ne.Index
		}
	}
	// 如果没有发生冲突 Entry, 则返回 0
	return 0
}

// unstableEntries 返回 raftLog.unstable 中所有的 Entry 记录
func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
//
// nextEnts 将已提交且未应用的 Entry 记录返回给上层模块处理.
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	// 获取当前已经应用记录的位置
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off {
		// 获取全部已提交且未应用的 Entry 记录并返回
		ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// hasNextEnts returns if there is any available entries for execution. This
// is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
//
// 当上层模块需要从 raftLog 获取 Entry 记录进行处理时, 会先调用 hasNextEnts 方法检测是否有待应用的记录,
// 然后调用 nextEnts 方法将已提交且未应用的 Entry 记录返回给上层模块处理.
func (l *raftLog) hasNextEnts() bool {
	// 获取当前已经应用记录的位置
	off := max(l.applied+1, l.firstIndex())
	// 是否存在已提交且未应用的 Entry 记录
	return l.committed+1 > off
}

// snapshot 返回 raftLog 中保存的快照数据:
// - 若 unstable 中保存有, 则返回 unstable 中的快照数据; 否则返回 storage 中的快照数据
func (l *raftLog) snapshot() (pb.Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

// firstIndex 获取 raftLog 中记录的第一条 Entry 记录的索引值.
// 1. 先从 unstable 中获取其记录的第一条 Entry 记录的索引值;
// 2. unstable 中没有记录 Entry, 则从 sotrage 中获取其记录的第一条 Entry 记录.
func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}

func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit { // raftLog.committed 字段只能后移, 不能前移
		if l.lastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		// 更新 committed 字段
		l.committed = tocommit
	}
}

// appliedTo 更新已应用位置 raftLog.applied 字段
func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	// 已应用位置（applied）必须 <= 已提交位置（committed）
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

// stableTo 当 unstable.entries 中的 Entry 记录已经被写入 Storage 之后, 会调用该方法清除 unstable.entries 中对应的 Entry 记录.
func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }

// stableSnapTo 当 unstable.snapshot 字段指向的快照数据被写入 Storage 后, 会调用该方法将 unstable.snapshot 字段清空.
func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

// lastTerm 返回 raftLog 中记录的最后一个 Entry 的 Term 值
func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// term 查询指定索引值 i 对应的 Entry 记录的 Term 值
func (l *raftLog) term(i uint64) (uint64, error) {
	// the valid term range is [index of dummy entry, last index]
	//
	// 检测指定的索引值 i 是否合法
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.lastIndex() {
		// TODO: return an error instead?
		return 0, nil
	}

	// 尝试从 unstable 中获取对应的 Entry 记录并返回其 Term 值.
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}

	// 尝试从 storage 中获取对应的 Entry 记录并返回其 Term 值
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err) // TODO(bdarnell)
}

// entries 在 Leader 节点向 Follower 节点发送 MsgApp 消息时, 需要根据 Follower 节点的日志复制（NextIndex 和 MatchIndex）
// 情况决定发送的 Entry 记录, 此时需要调用 entries() 方法获取指定的 Entry 记录.
func (l *raftLog) entries(i, maxsize uint64) ([]pb.Entry, error) {
	// 检测指定的 Entry 记录索引值是否超出了 raftLog 的范围
	if i > l.lastIndex() {
		return nil, nil
	}
	// 获取指定范围 [i, l.lastIndex()+1] 的 Entry 记录, 限制返回最大总字节数为 maxsize, 超过则将截断.
	return l.slice(i, l.lastIndex()+1, maxsize)
}

// allEntries returns all entries in the log.
//
// allEntries 返回 raftLog 中所有的 Entry 记录
func (l *raftLog) allEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted { // try again if there was a racing compaction
		return l.allEntries()
	}
	// TODO (xiangli): handle error?
	panic(err)
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
//
// isUpToDate Follower 节点在接收到 Candidate 节点的选举请求后, 会通过比较 Candidate 节点的本地日志与自身本地日志的新旧程度,
// 从而决定是否投票. isUpToDate() 方法即用于比较日志的新旧程度.
//
// lasti 和 term 分别是 Candidate 节点的最大记录索引值和最大任期号（即 MsgVote 请求（Candidate 发送的选举请求）携带的 Index 和 LogTerm）.
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	// 先比较任期号, 任期号相同再比较索引值
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

// matchTerm 检测指定索引 i 对应的 Entry 记录的 Term 值是否与 term 相等
func (l *raftLog) matchTerm(i, term uint64) bool {
	// 查询指定索引值对应的 Entry 记录的 Term 值
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	// maxIndex 必须大于当前 raftLog 中已提交的 Entry 记录中最大的索引值 且该 maxIndex 对应的 Entry 记录的任期号与 term 相等,
	// 才能更新当前 raftLog 的已提交位置 committed 字段.
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

// restore 根据传入的快照数据 s 重建 raftLog.unstable
func (l *raftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	// 使用快照数据的元数据 Index 值重置当前 raftLog 的已提交位置 committed 字段
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
//
// slice 返回 [lo, hi-1] 之间的 Entry 记录, 返回的最大总字节数不得超过 maxSize.
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	// 边界检测, 确保获取的范围合法
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	// 如果 lo 小于 unstable.offset（unstable 中第一条记录的索引）, 则需要从 raftLog.storage 中获取记录
	if lo < l.unstable.offset {
		// 从 Storage 中获取, 可能只能从 Storage 中取前半部分（也就是到 lo~l.unstable.offset 部分）.
		// 注意, maxSize 会限制获取的记录切片的总字节数.
		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.unstable.offset))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}

		// check if ents has reached the size limitation
		//
		// 从 Storage 中取出的记录已经达到了 maxSize 的上限, 则直接返回
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}
	// 从 unstable 中取出后半部分记录, 即 l.unstable.offset~hi 的日志记录
	if hi > l.unstable.offset {
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		// 将 Storage 中获取的记录与 unstable 中获取的记录进行合并
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	// 限制获取的记录切片的总字节数
	return limitSize(ents, maxSize), nil
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.lastIndex() + 1 - fi
	if lo < fi || hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}
