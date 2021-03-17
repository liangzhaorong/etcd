// Copyright 2016 The etcd Authors
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

package clientv3

import (
	"context"
	"sync"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"

	"google.golang.org/grpc"
)

// Txn is the interface that wraps mini-transactions.
//
//	 Txn(context.TODO()).If(
//	  Compare(Value(k1), ">", v1),
//	  Compare(Version(k1), "=", 2)
//	 ).Then(
//	  OpPut(k2,v2), OpPut(k3,v3)
//	 ).Else(
//	  OpPut(k4,v4), OpPut(k5,v5)
//	 ).Commit()
//
type Txn interface {
	// If takes a list of comparison. If all comparisons passed in succeed,
	// the operations passed into Then() will be executed. Or the operations
	// passed into Else() will be executed.
	//
	// 添加 Compare 条件
	If(cs ...Cmp) Txn

	// Then takes a list of operations. The Ops list will be executed, if the
	// comparisons passed in If() succeed.
	//
	// 如果 Compare 条件全部成立, 则执行 Then() 方法中添加的全部操作
	Then(ops ...Op) Txn

	// Else takes a list of operations. The Ops list will be executed, if the
	// comparisons passed in If() fail.
	//
	// 如果 Compare 条件不成立, 则执行 Else() 方法中添加的全部操作
	Else(ops ...Op) Txn

	// Commit tries to commit the transaction.
	//
	// 提交 Txn
	Commit() (*TxnResponse, error)
}

// txn 结构体实现了 Txn 接口.
type txn struct {
	// 关联的 kv 实例
	kv  *kv
	ctx context.Context

	mu    sync.Mutex
	// 是否已经设置 Compare 条件, 设置一次则该字段就会被设置为 true
	cif   bool
	// 是否已经向 sus 集合中添加了操作
	cthen bool
	// 是否已经向 fas 集合中添加了操作
	celse bool

	// 如果当前 txn 实例中记录的操作全部是读操作, 则该字段为 false
	isWrite bool

	// 用于记录 Compare 条件的集合
	cmps []*pb.Compare

	// 全部 Compare 条件通过之后执行的操作列表
	sus []*pb.RequestOp
	// 任一 Compare 条件检测失败之后执行的操作列表
	fas []*pb.RequestOp

	callOpts []grpc.CallOption
}

// If 将 Compare 条件添加到 cmps 集合中
func (txn *txn) If(cs ...Cmp) Txn {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	// 不可调用两次该函数
	if txn.cif {
		panic("cannot call If twice!")
	}

	if txn.cthen {
		panic("cannot call If after Then!")
	}

	if txn.celse {
		panic("cannot call If after Else!")
	}

	txn.cif = true

	for i := range cs {
		txn.cmps = append(txn.cmps, (*pb.Compare)(&cs[i]))
	}

	return txn
}

// Then 将操作添加到 sus 集合中并同时更新 isWrite 字段
func (txn *txn) Then(ops ...Op) Txn {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.cthen {
		panic("cannot call Then twice!")
	}
	if txn.celse {
		panic("cannot call Then after Else!")
	}

	txn.cthen = true

	for _, op := range ops {
		txn.isWrite = txn.isWrite || op.isWrite()
		txn.sus = append(txn.sus, op.toRequestOp())
	}

	return txn
}

// Else 将操作添加到 fas 集合中并同时更新 isWrite 字段
func (txn *txn) Else(ops ...Op) Txn {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.celse {
		panic("cannot call Else twice!")
	}

	txn.celse = true

	for _, op := range ops {
		txn.isWrite = txn.isWrite || op.isWrite()
		txn.fas = append(txn.fas, op.toRequestOp())
	}

	return txn
}

func (txn *txn) Commit() (*TxnResponse, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	// 创建 TxnRequest 实例
	r := &pb.TxnRequest{Compare: txn.cmps, Success: txn.sus, Failure: txn.fas}

	var resp *pb.TxnResponse
	var err error
	// 调用 KVClient.Txn() 方法与服务端交互
	resp, err = txn.kv.remote.Txn(txn.ctx, r, txn.callOpts...)
	if err != nil {
		return nil, toErr(txn.ctx, err)
	}
	return (*TxnResponse)(resp), nil
}
