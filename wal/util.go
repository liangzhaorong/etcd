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

package wal

import (
	"errors"
	"fmt"
	"strings"

	"go.etcd.io/etcd/pkg/fileutil"

	"go.uber.org/zap"
)

var errBadWALName = errors.New("bad wal name")

// Exist returns true if there are any files in a given directory.
func Exist(dir string) bool {
	names, err := fileutil.ReadDir(dir, fileutil.WithExt(".wal"))
	if err != nil {
		return false
	}
	return len(names) != 0
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
//
// searchIndex 根据 WAL 日志文件名的规则, 查找上面得到的所有文件名, 找到 index 最大且 index 小于 snap.Index
// 的 WAL 日志文件, 并返回该文件在 names 数组中的索引（nameIndex）.
func searchIndex(lg *zap.Logger, names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex, err := parseWALName(name)
		if err != nil {
			if lg != nil {
				lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
			} else {
				plog.Panicf("parse correct name should never fail: %v", err)
			}
		}
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}

// names should have been sorted based on sequence number.
// isValidSeq checks whether seq increases continuously.
func isValidSeq(lg *zap.Logger, names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _, err := parseWALName(name)
		if err != nil {
			if lg != nil {
				lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
			} else {
				plog.Panicf("parse correct name should never fail: %v", err)
			}
		}
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}

// readWALNames 获取全部的 WAL 日志文件名, 且返回的 wal 日志文件名都是符合 wal 命名规范的
func readWALNames(lg *zap.Logger, dirpath string) ([]string, error) {
	// 读取目录中所有的文件名
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}
	// 检测这些文件命名格式是否正确, 并返回符合 wal 命名规范的文件名
	wnames := checkWalNames(lg, names)
	if len(wnames) == 0 {
		return nil, ErrFileNotFound
	}
	return wnames, nil
}

// checkWalNames 检测并返回 names 中命名格式正确的 wal 文件名
func checkWalNames(lg *zap.Logger, names []string) []string {
	wnames := make([]string, 0)
	// 遍历所有文件名, 返回命名正确的 wal 文件名
	for _, name := range names {
		// 若解析该 wal 文件名失败, 则忽略该文件
		if _, _, err := parseWALName(name); err != nil {
			// don't complain about left over tmp files
			if !strings.HasSuffix(name, ".tmp") {
				if lg != nil {
					lg.Warn(
						"ignored file in WAL directory",
						zap.String("path", name),
					)
				} else {
					plog.Warningf("ignored file %v in wal", name)
				}
			}
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

// parseWALName 根据 wal 日志文件名解析出 seq（单调递增） 和 index（该 wal 中记录的第一条日志记录的索引）
func parseWALName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, errBadWALName
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

// walName WAL 日志文件名由两部分组成, 一部分是 seq（单调递增）,
// 另一部分是该日志文件中的第一条日志记录的索引值.
func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}
