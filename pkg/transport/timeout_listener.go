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

package transport

import (
	"net"
	"time"
)

// NewTimeoutListener returns a listener that listens on the given address.
// If read/write on the accepted connection blocks longer than its time limit,
// it will return timeout error.
func NewTimeoutListener(addr string, scheme string, tlsinfo *TLSInfo, rdtimeoutd, wtimeoutd time.Duration) (net.Listener, error) {
	// 根据 scheme（unix 或 tcp）的类型创建 net.Listener 实例
	ln, err := newListener(addr, scheme)
	if err != nil {
		return nil, err
	}
	// 将 net.Listener 实例重新封装为 rwTimeoutListener 实例
	ln = &rwTimeoutListener{
		Listener:   ln,
		rdtimeoutd: rdtimeoutd,
		wtimeoutd:  wtimeoutd,
	}
	if ln, err = wrapTLS(scheme, tlsinfo, ln); err != nil {
		return nil, err
	}
	return ln, nil
}

// rwTimeoutListener 结构体在 net.Listener 的基础上扩展了读写超时
type rwTimeoutListener struct {
	net.Listener
	wtimeoutd  time.Duration // 写超时
	rdtimeoutd time.Duration // 读超时
}

// Accept 等待接收客户端的连接
func (rwln *rwTimeoutListener) Accept() (net.Conn, error) {
	// 调用底层的 net.Listener.Accept() 方法阻塞等待客户端的连接到来
	c, err := rwln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	// 将接收到的 net.Conn 连接实例封装成 timeoutConn 实例, 该实例重写了 Read()、Write() 方法
	return timeoutConn{
		Conn:       c,
		wtimeoutd:  rwln.wtimeoutd,
		rdtimeoutd: rwln.rdtimeoutd,
	}, nil
}
