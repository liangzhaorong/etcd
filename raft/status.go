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

package raft

import (
	"fmt"

	pb "go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/raft/tracker"
)

// Status contains information about this Raft peer and its view of the system.
// The Progress is only populated on the leader.
type Status struct {
	BasicStatus
	Config   tracker.Config
	// 记录集群中每个节点对应的 Progress 实例
	Progress map[uint64]tracker.Progress
}

// BasicStatus contains basic information about the Raft peer. It does not allocate.
type BasicStatus struct {
	// 当前节点的 ID
	ID uint64

	// 内嵌 HardState 和 SoftState
	pb.HardState
	SoftState

	// 已应用的 Entry 记录的最大索引值
	Applied uint64

	// 当前正在执行 Leader 转移的目标节点 ID
	LeadTransferee uint64
}

func getProgressCopy(r *raft) map[uint64]tracker.Progress {
	m := make(map[uint64]tracker.Progress)
	r.prs.Visit(func(id uint64, pr *tracker.Progress) {
		var p tracker.Progress
		p = *pr
		p.Inflights = pr.Inflights.Clone()
		pr = nil

		m[id] = p
	})
	return m
}

func getBasicStatus(r *raft) BasicStatus {
	s := BasicStatus{
		// 当前节点的 ID
		ID:             r.id,
		// 当前正在进行 Leader 节点转移的目标节点 ID（若有的话）
		LeadTransferee: r.leadTransferee,
	}
	// 从底层的 raft 实例获取 HardState 实例
	s.HardState = r.hardState()
	// 从底层的 raft 实例获取 SoftState 实例
	s.SoftState = *r.softState()
	// 获取底层的 raftLog 中记录已应用的位置
	s.Applied = r.raftLog.applied
	return s
}

// getStatus gets a copy of the current raft status.
//
// getStatus 获取当前 raft 状态的副本
func getStatus(r *raft) Status {
	// 创建 Status 实例
	var s Status
	// 创建 BasicStatus 实例, 并根据底层 raft 信息初始化该实例
	s.BasicStatus = getBasicStatus(r)
	// 如果当前节点是 Leader 状态, 则将集群中每个节点对应的 Progress 实例封装到 Status 实例中
	if s.RaftState == StateLeader {
		s.Progress = getProgressCopy(r)
	}
	s.Config = r.prs.Config.Clone()
	return s
}

// MarshalJSON translates the raft status into JSON.
// TODO: try to simplify this by introducing ID type into raft
func (s Status) MarshalJSON() ([]byte, error) {
	j := fmt.Sprintf(`{"id":"%x","term":%d,"vote":"%x","commit":%d,"lead":"%x","raftState":%q,"applied":%d,"progress":{`,
		s.ID, s.Term, s.Vote, s.Commit, s.Lead, s.RaftState, s.Applied)

	if len(s.Progress) == 0 {
		j += "},"
	} else {
		for k, v := range s.Progress {
			subj := fmt.Sprintf(`"%x":{"match":%d,"next":%d,"state":%q},`, k, v.Match, v.Next, v.State)
			j += subj
		}
		// remove the trailing ","
		j = j[:len(j)-1] + "},"
	}

	j += fmt.Sprintf(`"leadtransferee":"%x"}`, s.LeadTransferee)
	return []byte(j), nil
}

func (s Status) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		raftLogger.Panicf("unexpected error: %v", err)
	}
	return string(b)
}
