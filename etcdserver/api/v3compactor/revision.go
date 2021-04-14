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

package v3compactor

import (
	"context"
	"sync"
	"time"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/mvcc"

	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"
)

// Revision compacts the log by purging revisions older than
// the configured reivison number. Compaction happens every 5 minutes.
type Revision struct {
	lg *zap.Logger

	clock     clockwork.Clock
	retention int64

	rg RevGetter
	c  Compactable

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	paused bool
}

// newRevision creates a new instance of Revisonal compactor that purges
// the log older than retention revisions from the current revision.
func newRevision(lg *zap.Logger, clock clockwork.Clock, retention int64, rg RevGetter, c Compactable) *Revision {
	rc := &Revision{
		lg:        lg,
		clock:     clock,
		retention: retention,
		rg:        rg,
		c:         c,
	}
	rc.ctx, rc.cancel = context.WithCancel(context.Background())
	return rc
}

const revInterval = 5 * time.Minute

// Run runs revision-based compactor.
//
// Run 执行基于 revision 的压缩机制, 创建一个 goroutine 单独执行 revision Compactor,
// revision Compactor 会根据设置的保留版本号数, 每隔 5 分钟定时获取当前 server 的最大
// 版本号, 减去想保留的历史版本数, 然后通过 etcd server 的 Compact 接口发起压缩操作.
func (rc *Revision) Run() {
	prev := int64(0)
	go func() {
		for {
			select {
			case <-rc.ctx.Done():
				return
			case <-rc.clock.After(revInterval):
				rc.mu.Lock()
				p := rc.paused
				rc.mu.Unlock()
				if p {
					continue
				}
			}

			// 获取当前版本号, 减去保留的版本号数
			rev := rc.rg.Rev() - rc.retention
			if rev <= 0 || rev == prev {
				continue
			}

			now := time.Now()
			if rc.lg != nil {
				rc.lg.Info(
					"starting auto revision compaction",
					zap.Int64("revision", rev),
					zap.Int64("revision-compaction-retention", rc.retention),
				)
			} else {
				plog.Noticef("Starting auto-compaction at revision %d (retention: %d revisions)", rev, rc.retention)
			}
			// 调用 server 的 Compact 接口发起压缩
			_, err := rc.c.Compact(rc.ctx, &pb.CompactionRequest{Revision: rev})
			if err == nil || err == mvcc.ErrCompacted {
				prev = rev
				if rc.lg != nil {
					rc.lg.Info(
						"completed auto revision compaction",
						zap.Int64("revision", rev),
						zap.Int64("revision-compaction-retention", rc.retention),
						zap.Duration("took", time.Since(now)),
					)
				} else {
					plog.Noticef("Finished auto-compaction at revision %d", rev)
				}
			} else {
				if rc.lg != nil {
					rc.lg.Warn(
						"failed auto revision compaction",
						zap.Int64("revision", rev),
						zap.Int64("revision-compaction-retention", rc.retention),
						zap.Duration("retry-interval", revInterval),
						zap.Error(err),
					)
				} else {
					plog.Noticef("Failed auto-compaction at revision %d (%v)", rev, err)
					plog.Noticef("Retry after %v", revInterval)
				}
			}
		}
	}()
}

// Stop stops revision-based compactor.
func (rc *Revision) Stop() {
	rc.cancel()
}

// Pause pauses revision-based compactor.
func (rc *Revision) Pause() {
	rc.mu.Lock()
	rc.paused = true
	rc.mu.Unlock()
}

// Resume resumes revision-based compactor.
func (rc *Revision) Resume() {
	rc.mu.Lock()
	rc.paused = false
	rc.mu.Unlock()
}
