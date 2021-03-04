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

package main

import (
	"errors"
	"net"
	"time"
)

// stoppableListener sets TCP keep-alive timeouts on accepted
// connections and waits on stopc message
type stoppableListener struct {
	// 内嵌了 net.TCPListener
	*net.TCPListener
	// 用来监听是否应该关闭当前的 Server, 也就是 raftNode 中的 httpstopc 通道
	stopc <-chan struct{}
}

func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

// Accept 重写了 net.TCPListener.Accept() 方法
func (ln stoppableListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	// 启动单独 goroutine 来接收新连接
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		// 将接收到的新连接传递到 connc 通道中
		connc <- tc
	}()
	select {
	case <-ln.stopc: // 从 stopc 通道中接收到通知时, 抛出异常
		return nil, errors.New("server stopped")
	case err := <-errc: // 如果在接收新连接时出现异常, 则抛出异常
		return nil, err
	case tc := <-connc: // 接收到新连接, 设置 KeepAlive 等连接属性, 并返回
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}
