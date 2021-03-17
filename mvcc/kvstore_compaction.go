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
	"encoding/binary"
	"time"

	"go.uber.org/zap"
)

// scheduleCompaction 完成对 BoltDB 中存储的键值对的压缩, 在该方法中会通过 UnsafeRange() 方法批量查询待删除的 Key（revision）,
// 然后逐个调用 UnsafeDelete() 方法进行删除, 全部的待压缩 Key 被处理完成后, 会向 "meta" Bucket 中写入此次压缩的相关信息, 其中
// Key 为 "finishedCompactRev".
func (s *store) scheduleCompaction(compactMainRev int64, keep map[revision]struct{}) bool {
	totalStart := time.Now()
	defer func() { dbCompactionTotalMs.Observe(float64(time.Since(totalStart) / time.Millisecond)) }()
	keyCompactions := 0
	defer func() { dbCompactionKeysCounter.Add(float64(keyCompactions)) }()

	end := make([]byte, 8)
	// 将 compactMainRev+1 写入 end（[]byte 类型）中（store.Compact() 方法中已经更新过 compactMainRev）
	// 作为范围查询的结束 key（revision）.
	binary.BigEndian.PutUint64(end, uint64(compactMainRev+1))

	last := make([]byte, 8+1+8) // 范围查询的起始 key（revision）
	for {
		var rev revision

		start := time.Now()

		tx := s.b.BatchTx() // 获取读写事务
		tx.Lock() // 对读写事务加锁
		// 调用 UnsafeRange() 方法对 "key" Bucket 进行范围查询
		keys, _ := tx.UnsafeRange(keyBucketName, last, end, int64(s.cfg.CompactionBatchLimit))
		// 遍历查询到的 Key（revision）, 并逐个调用 UnsafeDelete() 方法进行删除
		for _, key := range keys {
			// 将 BoltDB 中的 key 转换成 revision 结构
			rev = bytesToRev(key)
			if _, ok := keep[rev]; !ok {
				tx.UnsafeDelete(keyBucketName, key)
				keyCompactions++
			}
		}

		// 最后一次批量操作
		if len(keys) < s.cfg.CompactionBatchLimit {
			// 将 compactMainRev 封装成 revision, 并写入 "meta" Bucket 中, 其 Key 为 "finishedCompactRev"
			rbytes := make([]byte, 8+1+8)
			revToBytes(revision{main: compactMainRev}, rbytes)
			tx.UnsafePut(metaBucketName, finishedCompactKeyName, rbytes)
			tx.Unlock() // 读写事务使用完毕, 释放锁
			if s.lg != nil {
				s.lg.Info(
					"finished scheduled compaction",
					zap.Int64("compact-revision", compactMainRev),
					zap.Duration("took", time.Since(totalStart)),
				)
			} else {
				plog.Infof("finished scheduled compaction at %d (took %v)", compactMainRev, time.Since(totalStart))
			}
			return true
		}

		// update last
		// 更新 last, 下次范围查询的起始 key 依然是 last
		revToBytes(revision{main: rev.main, sub: rev.sub + 1}, last)
		tx.Unlock()
		// Immediately commit the compaction deletes instead of letting them accumulate in the write buffer
		s.b.ForceCommit()
		dbCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))

		select {
		case <-time.After(10 * time.Millisecond):
		case <-s.stopc:
			return false
		}
	}
}
