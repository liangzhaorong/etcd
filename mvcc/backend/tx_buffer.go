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

package backend

import (
	"bytes"
	"sort"
)

// txBuffer handles functionality shared between txWriteBuffer and txReadBuffer.
type txBuffer struct {
	// 记录 Bucket 名称与对应 bucketBuffer 的映射关系. 在 bucketBuffer 中缓存了对应 Bucket 中的键值对数据
	buckets map[string]*bucketBuffer
}

// reset 负责清空 buckets 字段中的全部内容
func (txb *txBuffer) reset() {
	for k, v := range txb.buckets {
		// 删除未使用的 bucketBuffer
		if v.used == 0 {
			// demote
			delete(txb.buckets, k)
		}
		// 清空使用过的 bucketBuffer
		v.used = 0
	}
}

// txWriteBuffer buffers writes of pending updates that have not yet committed.
type txWriteBuffer struct {
	txBuffer
	seq bool // 用于标记写入当前 txWriteBuffer 的键值对是否为顺序的
}

// put 以非顺序写入的方式向 bucketBuffer 中添加键值对
func (txw *txWriteBuffer) put(bucket, k, v []byte) {
	txw.seq = false
	txw.putSeq(bucket, k, v)
}

// putSeq 向指定 bucketBuffer 添加键值对
func (txw *txWriteBuffer) putSeq(bucket, k, v []byte) {
	// 根据 bucket 名获取对应的 bucketBuffer 实例
	b, ok := txw.buckets[string(bucket)]
	if !ok {
		// 如果未找到, 则创建对应的 bucketBuffer 实例并保存到 buckets 中
		b = newBucketBuffer()
		txw.buckets[string(bucket)] = b
	}
	// 通过 bucketBuffer.add() 方法添加键值对
	b.add(k, v)
}

// writeback 回写 txWriteBuffer 中的 bucketBuffer 到传入的 txReadBuffer 参数中, 最后清空 txWriteBuffer 中的内容
func (txw *txWriteBuffer) writeback(txr *txReadBuffer) {
	// 遍历所有的 bucketBuffer
	for k, wb := range txw.buckets {
		// 从传入的 bucketBuffer 中查找指定的 bucketBuffer
		rb, ok := txr.buckets[k]
		if !ok {
			// 如果 txReadBuffer 中不存在对应的 bucketBuffer, 则直接使用 txWriteBuffer 中缓存的
			// bucketBuffer 实例.
			delete(txw.buckets, k)
			txr.buckets[k] = wb
			continue
		}
		// 若 txWriteBuffer 中的 bucketBuffer 是非顺序写入的, 则进行排序
		if !txw.seq && wb.used > 1 {
			// assume no duplicate keys
			sort.Sort(wb)
		}
		// 通过 bucketBuffer.merge 方法, 合并两个 bucketBuffer 实例并去重
		rb.merge(wb)
	}
	// 清空 txWriteBuffer
	txw.reset()
}

// txReadBuffer accesses buffered updates.
type txReadBuffer struct{ txBuffer }

// Range 查询指定 bucket 中 key~endKey 范围的内容
func (txr *txReadBuffer) Range(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	// 查询指定 txBuffer 实例
	if b := txr.buckets[string(bucketName)]; b != nil {
		return b.Range(key, endKey, limit)
	}
	return nil, nil
}

// ForEach 遍历指定 bucket 中的所有键值对
func (txr *txReadBuffer) ForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	if b := txr.buckets[string(bucketName)]; b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// unsafeCopy returns a copy of txReadBuffer, caller should acquire backend.readTx.RLock()
func (txr *txReadBuffer) unsafeCopy() txReadBuffer {
	txrCopy := txReadBuffer{
		txBuffer: txBuffer{
			buckets: make(map[string]*bucketBuffer, len(txr.txBuffer.buckets)),
		},
	}
	for bucketName, bucket := range txr.txBuffer.buckets {
		txrCopy.txBuffer.buckets[bucketName] = bucket.Copy()
	}
	return txrCopy
}

type kv struct {
	key []byte
	val []byte
}

// bucketBuffer buffers key-value pairs that are pending commit.
//
// bucketBuffer 中缓存了对应 Bucket 中的键值对数据
type bucketBuffer struct {
	// 每个元素都表示一个键值对, kv.key 和 kv.value 都是 []byte 类型. 在初始化时, 该切片的默认大小是 512.
	buf []kv
	// used tracks number of elements in use so buf can be reused without reallocation.
	//
	// 该字段记录 buf 中目前使用的下标位置
	used int
}

func newBucketBuffer() *bucketBuffer {
	return &bucketBuffer{buf: make([]kv, 512), used: 0}
}

// Range 查找当前缓存的 Bucket 中符合 key~endKye 之间的所有键值对, 上限为 limit
func (bb *bucketBuffer) Range(key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	// 定义 key 的比较方式
	f := func(i int) bool { return bytes.Compare(bb.buf[i].key, key) >= 0 }
	// 查询 0~used 之间是否有指定的 key
	idx := sort.Search(bb.used, f)
	if idx < 0 {
		return nil, nil
	}
	// 没有指定 endKye, 则只返回 key 对应的键值对
	if len(endKey) == 0 {
		if bytes.Equal(key, bb.buf[idx].key) {
			keys = append(keys, bb.buf[idx].key)
			vals = append(vals, bb.buf[idx].val)
		}
		return keys, vals
	}
	// 如果指定了 endKye, 则检测 endKey 的合法性
	if bytes.Compare(endKey, bb.buf[idx].key) <= 0 {
		return nil, nil
	}
	// 从前面查找到的 idx 位置开始遍历, 直到遍历到 endKey 或是遍历的键值对个数达到 limit 上限为止.
	for i := idx; i < bb.used && int64(len(keys)) < limit; i++ {
		if bytes.Compare(endKey, bb.buf[i].key) <= 0 {
			break
		}
		keys = append(keys, bb.buf[i].key)
		vals = append(vals, bb.buf[i].val)
	}
	// 返回全部符合条件的键值对
	return keys, vals
}

// ForEach 提供了遍历当前 bucketBuffer 实例缓存的所有键值对的功能, 其中会调用传入的 visitor() 函数处理每个键值对.
func (bb *bucketBuffer) ForEach(visitor func(k, v []byte) error) error {
	// 遍历 used 之前的所有元素
	for i := 0; i < bb.used; i++ {
		// 调用 visitor() 函数处理键值对
		if err := visitor(bb.buf[i].key, bb.buf[i].val); err != nil {
			return err
		}
	}
	return nil
}

// add 向当前 bucketBuffer.buf 中添加键值对
func (bb *bucketBuffer) add(k, v []byte) {
	// 添加键值对
	bb.buf[bb.used].key, bb.buf[bb.used].val = k, v
	// 递增 used
	bb.used++
	// 当 buf 空间被用尽时, 对其进行扩容
	if bb.used == len(bb.buf) {
		buf := make([]kv, (3*len(bb.buf))/2)
		copy(buf, bb.buf)
		bb.buf = buf
	}
}

// merge merges data from bb into bbsrc.
//
// merge 将传入的 bbsrc（bucketBuffer 实例）与当前的 bucketBuffer 进行合并, 之后会对合并结果进行排序和去重.
func (bb *bucketBuffer) merge(bbsrc *bucketBuffer) {
	// 将 bbsrc 中的键值对添加到 bucketBuffer 中
	for i := 0; i < bbsrc.used; i++ {
		bb.add(bbsrc.buf[i].key, bbsrc.buf[i].val)
	}
	// 复制之前, 如果当前 bucketBuffer 是空的, 则复制完键值对之后直接返回
	if bb.used == bbsrc.used {
		return
	}
	// 复制之前, 如果当前 bucketBuffer 不是空的, 则需要判断复制之后, 是否需要进行排序
	if bytes.Compare(bb.buf[(bb.used-bbsrc.used)-1].key, bbsrc.buf[0].key) < 0 {
		return
	}

	// 如果需要排序, 则调用 sort.Stable() 函数对 bucketBuffer 进行排序
	// 注意, Stable() 函数进行的是稳定排序, 即相等键值对的相对位置在排序之后不会改变.
	sort.Stable(bb)

	// remove duplicates, using only newest update
	widx := 0
	// 清除重复的 key, 使用 key 的最新值
	for ridx := 1; ridx < bb.used; ridx++ {
		if !bytes.Equal(bb.buf[ridx].key, bb.buf[widx].key) {
			widx++
		}
		// 新添加的键值对覆盖原有的键值对
		bb.buf[widx] = bb.buf[ridx]
	}
	bb.used = widx + 1
}

func (bb *bucketBuffer) Len() int { return bb.used }
func (bb *bucketBuffer) Less(i, j int) bool {
	return bytes.Compare(bb.buf[i].key, bb.buf[j].key) < 0
}
func (bb *bucketBuffer) Swap(i, j int) { bb.buf[i], bb.buf[j] = bb.buf[j], bb.buf[i] }

func (bb *bucketBuffer) Copy() *bucketBuffer {
	bbCopy := bucketBuffer{
		buf:  make([]kv, len(bb.buf)),
		used: bb.used,
	}
	copy(bbCopy.buf, bb.buf)
	return &bbCopy
}
