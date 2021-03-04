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
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"go.etcd.io/etcd/raft/raftpb"
)

// Handler for a http based key-value store backed by raft
//
// httpKVAPI 向外提供 HTTP 接口, 用户可通过调用 HTTP 接口来模拟客户端的行为.
type httpKVAPI struct {
	// 在 raftexample 中扮演了持久化存储的角色, 用于保存用户提交的键值对信息
	store       *kvstore
	// 在 raftexample 中, 当用户发送 POST（或 DELETE）请求时, 会被认为是发送了一个集群节点增加（或删除）
	// 的请求, httpKVAPI 会将该请求的信息写入 confChangeC 通道, 而 raftNode 实例会读取 confChangeC 通道
	// 并进行相应的处理.
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 获取请求的 URI 作为 key
	key := r.RequestURI
	defer r.Body.Close()
	switch {
	// PUT 请求表示向 kvstore 实例中添加（或更新）指定的键值对数据
	case r.Method == "PUT":
		// 读取 HTTP 请求体
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		// 在 kvstore.Propose 方法中会对键值对进行序列化, 之后将结果写入 proposeC 通道,
		// 后续 raftNode 会读取其中数据进行处理.
		h.store.Propose(key, string(v))

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)

	// GET 请求表示从 kvstore 实例中读取指定的键值对数据
	case r.Method == "GET":
		// 直接从 kvstore 中读取指定的键值对数据, 并返回给用户
		if v, ok := h.store.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}

	// POST 请求表示向集群中新增指定的节点
	case r.Method == "POST":
		// 读取请求体, 获取新加节点的 URL
		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		// 解析 URI 得到新增节点的 id
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		// 创建 ConfChange 消息
		cc := raftpb.ConfChange{
			// ConfChangeAddNode 即表示新增节点
			Type:    raftpb.ConfChangeAddNode,
			// 指定新增节点的 ID
			NodeID:  nodeId,
			// 指定新增节点的 URL
			Context: url,
		}
		// 将 ConfChange 实例写入 confChangeC 通道中
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)

	// DELETE 请求表示从集群中删除指定的节点
	case r.Method == "DELETE":
		// 解析 URI 得到新增节点的 id
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode, // 删除指定节点
			NodeID: nodeId, // 指定待删除的节点 ID
		}
		// 将 ConfChange 实例发送到 confChangeC 通道中
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	// 创建 http.Server 用于接收 HTTP 请求
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{
			store:       kv,
			confChangeC: confChangeC,
		},
	}
	// 启动单独的 goroutine 来监听 Addr 指定的地址, 当有 HTTP 请求时, http.Server 会创建对应的
	// goroutine, 并调用 httpKVAPI.ServeHTTP() 方法进行处理.
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
