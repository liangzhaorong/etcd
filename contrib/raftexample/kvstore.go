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
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"go.etcd.io/etcd/etcdserver/api/snap"
)

// a key-value store backed by raft
//
// kvstore 用于存储键值对信息, kvstore 扮演了持久化存储和状态机的角色, etcd-raft 模块通过 Ready 实例返回的
// 待应用 Entry 记录最终都会存储到 kvstore 中.
type kvstore struct {
	// httpKVAPI 处理 HTTP PUT 请求时, 会调用 kvstore.Propose() 方法将用户请求的数据写入 proposeC 通道中,
	// 之后 raftNode 会从该通道中读取数据并进行处理.
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	// 用来存储键值对的 map
	kvStore     map[string]string // current committed key-value pairs
	// 负责读取快照文件
	snapshotter *snap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *kvstore {
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]string), snapshotter: snapshotter}
	// replay log into key-value map
	s.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore[key]
	return v, ok
}

func (s *kvstore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

// readCommits raftNode 会将待应用的 Entry 记录写入 commitC 通道中, 另外, 当需要加载快照数据时, raftNode 会向 commitC
// 通道写入 nil 作为信号. readCommits 方法就会读取 commitC 通道, 然后将读取到的键值对写入 kvstore 中.
func (s *kvstore) readCommits(commitC <-chan *string, errorC <-chan error) {
	// 循环读取 commitC 通道
	for data := range commitC {
		// 读取到 nil 时, 则表示需要读取快照数据
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			//
			// 通过 snapshotter 读取快照文件
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			// 加载快照数据
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		// 将读取到的数据进行反序列化得到 kv 实例
		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.mu.Lock()
		// 将读取到的键值对保存到 kvStore 中
		s.kvStore[dataKv.Key] = dataKv.Val
		s.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

// recoverFromSnapshot 加载快照数据
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	// 将快照数据反序列化得到一个 map 实例
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// 替换 kvStore 字段
	s.kvStore = store
	return nil
}
