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

package mvcc

import "go.etcd.io/etcd/lease"

// metricsTxnWrite 结构体同时实现了 TxnRead 和 TxnWrite 接口, 并在原有的功能上记录监控信息
type metricsTxnWrite struct {
	// 指向 storeTxnWrite 实例
	TxnWrite
	ranges  uint
	// 记录当前事务执行过的 put 操作次数
	puts    uint
	deletes uint
	// 累积当前事务中执行 put 操作写入的数据量大小
	putSize int64
}

func newMetricsTxnRead(tr TxnRead) TxnRead {
	return &metricsTxnWrite{&txnReadWrite{tr}, 0, 0, 0, 0}
}

func newMetricsTxnWrite(tw TxnWrite) TxnWrite {
	return &metricsTxnWrite{tw, 0, 0, 0, 0}
}

func (tw *metricsTxnWrite) Range(key, end []byte, ro RangeOptions) (*RangeResult, error) {
	tw.ranges++
	return tw.TxnWrite.Range(key, end, ro)
}

func (tw *metricsTxnWrite) DeleteRange(key, end []byte) (n, rev int64) {
	tw.deletes++
	return tw.TxnWrite.DeleteRange(key, end)
}

func (tw *metricsTxnWrite) Put(key, value []byte, lease lease.LeaseID) (rev int64) {
	tw.puts++
	size := int64(len(key) + len(value))
	tw.putSize += size
	// 调用 storeTxnWrite.Put() 方法, 返回值是该事务操作后最新的 main revision 值
	return tw.TxnWrite.Put(key, value, lease)
}

// End 结束并提交事务
func (tw *metricsTxnWrite) End() {
	defer tw.TxnWrite.End()
	if sum := tw.ranges + tw.puts + tw.deletes; sum > 1 {
		txnCounter.Inc()
		txnCounterDebug.Inc() // TODO: remove in 3.5 release
	}

	ranges := float64(tw.ranges)
	rangeCounter.Add(ranges)
	rangeCounterDebug.Add(ranges) // TODO: remove in 3.5 release

	puts := float64(tw.puts)
	putCounter.Add(puts)
	putCounterDebug.Add(puts) // TODO: remove in 3.5 release
	totalPutSizeGauge.Add(float64(tw.putSize))

	deletes := float64(tw.deletes)
	deleteCounter.Add(deletes)
	deleteCounterDebug.Add(deletes) // TODO: remove in 3.5 release
}
