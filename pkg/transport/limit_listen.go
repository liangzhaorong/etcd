// Copyright 2013 The etcd Authors
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

// Package transport provides network utility functions, complementing the more
// common ones in the net package.
package transport

import (
	"errors"
	"net"
	"sync"
	"time"
)

var (
	ErrNotTCP = errors.New("only tcp connections have keepalive")
)

// LimitListener returns a Listener that accepts at most n simultaneous
// connections from the provided Listener.
func LimitListener(l net.Listener, n int) net.Listener {
	return &limitListener{l, make(chan struct{}, n)}
}

type limitListener struct {
	net.Listener
	sem chan struct{}
}

func (l *limitListener) acquire() { l.sem <- struct{}{} }
func (l *limitListener) release() { <-l.sem }

func (l *limitListener) Accept() (net.Conn, error) {
	// 向 l.sem 通道写入一个信号, 若该通道已满, 则阻塞
	l.acquire()
	// 调用底层的 net.Listener.Accept() 方法接收客户端的连接请求
	c, err := l.Listener.Accept()
	if err != nil {
		l.release()
		return nil, err
	}
	return &limitListenerConn{Conn: c, release: l.release}, nil
}

type limitListenerConn struct {
	net.Conn
	// 用于当连接关闭时仅调用一次 release 回调函数
	releaseOnce sync.Once
	// 当前连接结束时的回调函数, 默认从 l.sem 通道中取出一个信号
	release     func()
}

func (l *limitListenerConn) Close() error {
	err := l.Conn.Close()
	l.releaseOnce.Do(l.release)
	return err
}

func (l *limitListenerConn) SetKeepAlive(doKeepAlive bool) error {
	tcpc, ok := l.Conn.(*net.TCPConn)
	if !ok {
		return ErrNotTCP
	}
	return tcpc.SetKeepAlive(doKeepAlive)
}

func (l *limitListenerConn) SetKeepAlivePeriod(d time.Duration) error {
	tcpc, ok := l.Conn.(*net.TCPConn)
	if !ok {
		return ErrNotTCP
	}
	return tcpc.SetKeepAlivePeriod(d)
}
