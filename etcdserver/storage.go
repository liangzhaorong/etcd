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

package etcdserver

import (
	"io"

	"go.etcd.io/etcd/etcdserver/api/snap"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

// Storage 该接口用于保存待持久化的日志条目以及快照数据
type Storage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	//
	// 负责将 Entry 记录和 HardState 状态信息保存到底层的持久化存储上, 该方法可能会阻塞,
	// Storage 接口的实现是通过 WAL 模块将上述数据持久化到 WAL 日志文件中的.
	Save(st raftpb.HardState, ents []raftpb.Entry) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	//
	// 负责将快照数据持久化到底层的持久化存储上, 该方法也可能会阻塞, Storage 接口的实现
	// 是使用 Snapshotter 将快照数据保存到快照文件中的.
	SaveSnap(snap raftpb.Snapshot) error
	// Close closes the Storage and performs finalization.
	Close() error
	// Release releases the locked wal files older than the provided snapshot.
	Release(snap raftpb.Snapshot) error
	// Sync WAL
	Sync() error
}

// storage 结构体是 EtcdServer.Storage 接口的实现, 其中内嵌了 WAL 和 Snapshotter.
type storage struct {
	*wal.WAL
	*snap.Snapshotter
}

func NewStorage(w *wal.WAL, s *snap.Snapshotter) Storage {
	return &storage{w, s}
}

// SaveSnap saves the snapshot file to disk and writes the WAL snapshot entry.
//
// SaveSnap 将快照数据写入到磁盘和 WAL 中.
func (st *storage) SaveSnap(snap raftpb.Snapshot) error {
	// 根据快照的元数据创建对应的 walpb.Snapshot 实例
	walsnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	//
	// 通过 Snapshotter 将快照数据写入到磁盘中
	err := st.Snapshotter.SaveSnap(snap)
	if err != nil {
		return err
	}
	// gofail: var raftBeforeWALSaveSnaphot struct{}

	// 将 walpb.Snapshot 实例封装成 Record 记录写入 WAL 日志文件中
	return st.WAL.SaveSnapshot(walsnap)
}

// Release releases resources older than the given snap and are no longer needed:
// - releases the locks to the wal files that are older than the provided wal for the given snap.
// - deletes any .snap.db files that are older than the given snap.
func (st *storage) Release(snap raftpb.Snapshot) error {
	// 根据 WAL 日志文件中的名称及快照的元数据, 释放快照之前的 WAL 日志文件句柄
	if err := st.WAL.ReleaseLockTo(snap.Metadata.Index); err != nil {
		return err
	}
	return st.Snapshotter.ReleaseSnapDBs(snap)
}

// readWAL reads the WAL at the given snap and returns the wal, its latest HardState and cluster ID, and all entries that appear
// after the position of the given snap in the WAL.
// The snap must have been previously saved to the WAL, or this call will panic.
func readWAL(lg *zap.Logger, waldir string, snap walpb.Snapshot, unsafeNoFsync bool) (w *wal.WAL, id, cid types.ID, st raftpb.HardState, ents []raftpb.Entry) {
	var (
		err       error
		wmetadata []byte
	)

	repaired := false
	for {
		if w, err = wal.Open(lg, waldir, snap); err != nil {
			if lg != nil {
				lg.Fatal("failed to open WAL", zap.Error(err))
			} else {
				plog.Fatalf("open wal error: %v", err)
			}
		}
		if unsafeNoFsync {
			w.SetUnsafeNoFsync()
		}
		if wmetadata, st, ents, err = w.ReadAll(); err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				if lg != nil {
					lg.Fatal("failed to read WAL, cannot be repaired", zap.Error(err))
				} else {
					plog.Fatalf("read wal error (%v) and cannot be repaired", err)
				}
			}
			if !wal.Repair(lg, waldir) {
				if lg != nil {
					lg.Fatal("failed to repair WAL", zap.Error(err))
				} else {
					plog.Fatalf("WAL error (%v) cannot be repaired", err)
				}
			} else {
				if lg != nil {
					lg.Info("repaired WAL", zap.Error(err))
				} else {
					plog.Infof("repaired WAL error (%v)", err)
				}
				repaired = true
			}
			continue
		}
		break
	}
	var metadata pb.Metadata
	pbutil.MustUnmarshal(&metadata, wmetadata)
	id = types.ID(metadata.NodeID)
	cid = types.ID(metadata.ClusterID)
	return w, id, cid, st, ents
}
