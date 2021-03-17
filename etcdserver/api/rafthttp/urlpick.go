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

package rafthttp

import (
	"net/url"
	"sync"

	"go.etcd.io/etcd/pkg/types"
)

// urlPicker 每个节点可能提供多个 URL 供其他节点方法, 当其中一个访问失败时, 应可以尝试使用另一个.
// 而 urlPicker 提供的主要功能就是在这些 URL 之间进行切换.
type urlPicker struct {
	mu     sync.Mutex // guards urls and picked
	urls   types.URLs
	picked int
}

func newURLPicker(urls types.URLs) *urlPicker {
	return &urlPicker{
		urls: urls,
	}
}

func (p *urlPicker) update(urls types.URLs) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.urls = urls
	p.picked = 0
}

func (p *urlPicker) pick() url.URL {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.urls[p.picked]
}

// unreachable notices the picker that the given url is unreachable,
// and it should use other possible urls.
func (p *urlPicker) unreachable(u url.URL) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if u == p.urls[p.picked] {
		p.picked = (p.picked + 1) % len(p.urls)
	}
}
