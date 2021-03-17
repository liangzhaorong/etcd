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

package fileutil

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
)

func PurgeFile(lg *zap.Logger, dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}) <-chan error {
	return purgeFile(lg, dirname, suffix, max, interval, stop, nil, nil)
}

func PurgeFileWithDoneNotify(lg *zap.Logger, dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}) (<-chan struct{}, <-chan error) {
	doneC := make(chan struct{}) // 用于监听 purgeFile() 函数启动的后台 goroutine 的退出
	errC := purgeFile(lg, dirname, suffix, max, interval, stop, nil, doneC)
	return doneC, errC
}

// purgeFile is the internal implementation for PurgeFile which can post purged files to purgec if non-nil.
// if donec is non-nil, the function closes it to notify its exit.
//
// purgeFile 定期查询指定目录, 并统计指定后缀的文件个数, 如果文件个数超过指定的上限, 则进行清理操作.
func purgeFile(lg *zap.Logger, dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}, purgec chan<- string, donec chan<- struct{}) <-chan error {
	errC := make(chan error, 1)
	// 启动一个后台 goroutine
	go func() {
		if donec != nil {
			defer close(donec)
		}
		for {
			// 查找目标目录下的子目录, 其中还会根据文件名称进行排序
			fnames, err := ReadDir(dirname)
			if err != nil {
				errC <- err
				return
			}
			newfnames := make([]string, 0)
			// 根据指定的后缀对文件名称进行过滤
			for _, fname := range fnames {
				if strings.HasSuffix(fname, suffix) {
					newfnames = append(newfnames, fname)
				}
			}
			sort.Strings(newfnames) // 对过滤后的文件名称进行排序
			fnames = newfnames
			// 指定后缀的文件个数是否超过上限
			for len(newfnames) > int(max) {
				f := filepath.Join(dirname, newfnames[0])
				// 获取文件锁
				l, err := TryLockFile(f, os.O_WRONLY, PrivateFileMode)
				if err != nil {
					break
				}
				// 删除文件
				if err = os.Remove(f); err != nil {
					errC <- err
					return
				}
				// 关闭对应的文件句柄
				if err = l.Close(); err != nil {
					if lg != nil {
						lg.Warn("failed to unlock/close", zap.String("path", l.Name()), zap.Error(err))
					} else {
						plog.Errorf("error unlocking %s when purging file (%v)", l.Name(), err)
					}
					errC <- err
					return
				}
				if lg != nil {
					lg.Info("purged", zap.String("path", f))
				} else {
					plog.Infof("purged file %s successfully", f)
				}
				// 从 newfnames 中清除指定的文件
				newfnames = newfnames[1:]
			}
			if purgec != nil {
				for i := 0; i < len(fnames)-len(newfnames); i++ {
					purgec <- fnames[i]
				}
			}
			select {
			// 阻塞指定的时间间隔
			case <-time.After(interval):
			case <-stop: // 当前节点正在关闭, 则退出 goroutine
				return
			}
		}
	}()
	return errC
}
