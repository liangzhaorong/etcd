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

// rwTimeoutDialer 封装了带读写超时的 Dialer 实例
type rwTimeoutDialer struct {
	wtimeoutd  time.Duration // 写请求超时时间
	rdtimeoutd time.Duration // 读请求超时时间
	net.Dialer               // 内嵌 net.Dialer
}

func (d *rwTimeoutDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := d.Dialer.Dial(network, address)
	tconn := &timeoutConn{
		rdtimeoutd: d.rdtimeoutd,
		wtimeoutd:  d.wtimeoutd,
		Conn:       conn,
	}
	return tconn, err
}
