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

package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/pkg/capnslog"
	"go.etcd.io/etcd/etcdserver/api/snap/snappb"
	pioutil "go.etcd.io/etcd/pkg/ioutil"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

const snapSuffix = ".snap"

var (
	plog = capnslog.NewPackageLogger("go.etcd.io/etcd/v3", "snap")

	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")
	ErrCRCMismatch   = errors.New("snap: crc mismatch")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

type Snapshotter struct {
	lg  *zap.Logger
	// 指定存储快照文件的目录位置
	dir string
}

func New(lg *zap.Logger, dir string) *Snapshotter {
	return &Snapshotter{
		lg:  lg,
		dir: dir,
	}
}

// SaveSnap 将快照数据保存到快照文件中
func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.save(&snapshot)
}

func (s *Snapshotter) save(snapshot *raftpb.Snapshot) error {
	start := time.Now()

	// 创建快照文件名, 快照文件的名称由三部分组成, 分别是快照所涵盖的最后一条 Entry 记录的 Term、Index 和 ".snap" 后缀
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)
	// 将快照数据进行序列化
	b := pbutil.MustMarshal(snapshot)
	// 计算 crc 校验码
	crc := crc32.Update(0, crcTable, b)
	// 将序列化后的数据和校验码封装成 snappb.Snapshot 实例.
	// raftpb.Snapshot 和 snappb.Snapshot 的区别：前者包含了 Snapshot 数据及一些元数据（如, 该快照数据所涵盖的最后一条
	// Entry 记录的 Term 和 Index 值）; 后者则是在前者序列化之后的封装, 其中还记录了相应的校验码等信息.
	snap := snappb.Snapshot{Crc: crc, Data: b}
	// 将 snappb.Snapshot 实例序列化
	d, err := snap.Marshal()
	if err != nil {
		return err
	}
	snapMarshallingSec.Observe(time.Since(start).Seconds())

	spath := filepath.Join(s.dir, fname)

	fsyncStart := time.Now()
	// 将 snappb.Snapshot 序列化后的数据写入文件, 并不同刷新到磁盘
	err = pioutil.WriteAndSyncFile(spath, d, 0666)
	snapFsyncSec.Observe(time.Since(fsyncStart).Seconds())

	if err != nil {
		if s.lg != nil {
			s.lg.Warn("failed to write a snap file", zap.String("path", spath), zap.Error(err))
		}
		rerr := os.Remove(spath)
		if rerr != nil {
			if s.lg != nil {
				s.lg.Warn("failed to remove a broken snap file", zap.String("path", spath), zap.Error(err))
			} else {
				plog.Errorf("failed to remove broken snapshot file %s", spath)
			}
		}
		return err
	}

	snapSaveSec.Observe(time.Since(start).Seconds())
	return nil
}

// Load returns the newest snapshot.
//
// Load 读取指定目录下的全部快照文件, 并查找其中最近的可用快照文件, 然后通过 Snapshotter.loadSnap()
// 加法加载其中的快照数据.
func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	return s.loadMatching(func(*raftpb.Snapshot) bool { return true })
}

// LoadNewestAvailable loads the newest snapshot available that is in walSnaps.
func (s *Snapshotter) LoadNewestAvailable(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return s.loadMatching(func(snapshot *raftpb.Snapshot) bool {
		m := snapshot.Metadata
		for i := len(walSnaps) - 1; i >= 0; i-- {
			if m.Term == walSnaps[i].Term && m.Index == walSnaps[i].Index {
				return true
			}
		}
		return false
	})
}

// loadMatching returns the newest snapshot where matchFn returns true.
func (s *Snapshotter) loadMatching(matchFn func(*raftpb.Snapshot) bool) (*raftpb.Snapshot, error) {
	// 获取全部快照文件的名称, 其中会检查文件的后缀名, 过滤不合法的文件.
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}
	var snap *raftpb.Snapshot
	// 遍历所有的快照文件
	for _, name := range names {
		// 按序读取快照文件, 直到读取到第一个合法的快照文件, 对于读取失败的快照文件则添加 ".broken" 后缀,
		// 这样在下次调用 snapNames() 方法获取可用快照文件时, 就会将其过滤掉.
		if snap, err = loadSnap(s.lg, s.dir, name); err == nil && matchFn(snap) {
			return snap, nil
		}
	}
	return nil, ErrNoSnapshot
}

// loadSnap 真正加载快照数据
func loadSnap(lg *zap.Logger, dir, name string) (*raftpb.Snapshot, error) {
	// 获取快照文件的绝对路径
	fpath := filepath.Join(dir, name)
	// 读取快照文件的内容
	snap, err := Read(lg, fpath)
	// 如果失败, 则将该快照文件的后缀名修改成 ".broken"
	if err != nil {
		brokenPath := fpath + ".broken"
		if lg != nil {
			lg.Warn("failed to read a snap file", zap.String("path", fpath), zap.Error(err))
		}
		if rerr := os.Rename(fpath, brokenPath); rerr != nil {
			if lg != nil {
				lg.Warn("failed to rename a broken snap file", zap.String("path", fpath), zap.String("broken-path", brokenPath), zap.Error(rerr))
			} else {
				plog.Warningf("cannot rename broken snapshot file %v to %v: %v", fpath, brokenPath, rerr)
			}
		} else {
			if lg != nil {
				lg.Warn("renamed to a broken snap file", zap.String("path", fpath), zap.String("broken-path", brokenPath))
			}
		}
	}
	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
func Read(lg *zap.Logger, snapname string) (*raftpb.Snapshot, error) {
	// 读取文件
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		if lg != nil {
			lg.Warn("failed to read a snap file", zap.String("path", snapname), zap.Error(err))
		} else {
			plog.Errorf("cannot read file %v: %v", snapname, err)
		}
		return nil, err
	}

	if len(b) == 0 {
		if lg != nil {
			lg.Warn("failed to read empty snapshot file", zap.String("path", snapname))
		} else {
			plog.Errorf("unexpected empty snapshot")
		}
		return nil, ErrEmptySnapshot
	}

	var serializedSnap snappb.Snapshot
	// 将读取到的数据进行反序列化
	if err = serializedSnap.Unmarshal(b); err != nil {
		if lg != nil {
			lg.Warn("failed to unmarshal snappb.Snapshot", zap.String("path", snapname), zap.Error(err))
		} else {
			plog.Errorf("corrupted snapshot file %v: %v", snapname, err)
		}
		return nil, err
	}

	// 数据为空, 或 crc 校验码为 0, 则报错返回
	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		if lg != nil {
			lg.Warn("failed to read empty snapshot data", zap.String("path", snapname))
		} else {
			plog.Errorf("unexpected empty snapshot")
		}
		return nil, ErrEmptySnapshot
	}

	// 计算新的 crc 校验码
	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		if lg != nil {
			lg.Warn("snap file is corrupt",
				zap.String("path", snapname),
				zap.Uint32("prev-crc", serializedSnap.Crc),
				zap.Uint32("new-crc", crc),
			)
		} else {
			plog.Errorf("corrupted snapshot file %v: crc mismatch", snapname)
		}
		return nil, ErrCRCMismatch
	}

	// 使用 crc32 校验码验证数据是否正确
	var snap raftpb.Snapshot
	// 反序列化 snappb.Snapshot  实例的 Data 字段, 得到 raftpb.Snapshot 实例.
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		if lg != nil {
			lg.Warn("failed to unmarshal raftpb.Snapshot", zap.String("path", snapname), zap.Error(err))
		} else {
			plog.Errorf("corrupted snapshot file %v: %v", snapname, err)
		}
		return nil, err
	}
	return &snap, nil
}

// snapNames returns the filename of the snapshots in logical time order (from newest to oldest).
// If there is no available snapshots, an ErrNoSnapshot will be returned.
func (s *Snapshotter) snapNames() ([]string, error) {
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	names, err = s.cleanupSnapdir(names)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(s.lg, names)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(lg *zap.Logger, names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				if lg != nil {
					lg.Warn("found unexpected non-snap file; skipping", zap.String("path", names[i]))
				} else {
					plog.Warningf("skipped unexpected non snapshot file %v", names[i])
				}
			}
		}
	}
	return snaps
}

// cleanupSnapdir removes any files that should not be in the snapshot directory:
// - db.tmp prefixed files that can be orphaned by defragmentation
func (s *Snapshotter) cleanupSnapdir(filenames []string) (names []string, err error) {
	for _, filename := range filenames {
		if strings.HasPrefix(filename, "db.tmp") {
			if s.lg != nil {
				s.lg.Info("found orphaned defragmentation file; deleting", zap.String("path", filename))
			} else {
				plog.Infof("found orphaned defragmentation file; deleting: %s", filename)
			}
			if rmErr := os.Remove(filepath.Join(s.dir, filename)); rmErr != nil && !os.IsNotExist(rmErr) {
				return nil, fmt.Errorf("failed to remove orphaned .snap.db file %s: %v", filename, rmErr)
			}
			continue
		}
		names = append(names, filename)
	}
	return names, nil
}

func (s *Snapshotter) ReleaseSnapDBs(snap raftpb.Snapshot) error {
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, filename := range filenames {
		if strings.HasSuffix(filename, ".snap.db") {
			hexIndex := strings.TrimSuffix(filepath.Base(filename), ".snap.db")
			index, err := strconv.ParseUint(hexIndex, 16, 64)
			if err != nil {
				if s.lg != nil {
					s.lg.Warn("failed to parse index from filename", zap.String("path", filename), zap.String("error", err.Error()))
				} else {
					plog.Warningf("failed to parse index from filename: %s (%v)", filename, err)
				}
				continue
			}
			if index < snap.Metadata.Index {
				if s.lg != nil {
					s.lg.Warn("found orphaned .snap.db file; deleting", zap.String("path", filename))
				} else {
					plog.Warningf("found orphaned .snap.db file; deleting: %s", filename)
				}
				if rmErr := os.Remove(filepath.Join(s.dir, filename)); rmErr != nil && !os.IsNotExist(rmErr) {
					if s.lg != nil {
						s.lg.Warn("failed to remove orphaned .snap.db file", zap.String("path", filename), zap.Error(rmErr))
					} else {
						plog.Warningf("failed to remove orphaned .snap.db file: %s (%v)", filename, rmErr)
					}
				}
			}
		}
	}
	return nil
}
