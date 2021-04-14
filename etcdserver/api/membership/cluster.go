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

package membership

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/etcdserver/api/v2store"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/pkg/netutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/version"

	"github.com/coreos/go-semver/semver"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const maxLearners = 1

// RaftCluster is a list of Members that belong to the same raft cluster
type RaftCluster struct {
	lg *zap.Logger

	// 当前节点的 ID
	localID types.ID
	// 当前集群的 ID
	cid     types.ID
	// 当前集群的 token, 默认 "etcd-cluster", 可通过 --initial-cluster-token 启动参数配置
	token   string

	// etcd v2 版本的持久化存储
	v2store v2store.Store
	// etcd v3 版本的持久化存储
	be      backend.Backend

	// 内嵌 sync.Mutex, 用于保护下面的字段
	sync.Mutex // guards the fields below
	version    *semver.Version
	// 集群中每个节点都会有一个唯一 ID, 同时对应一个 Member 实例, 该 map
	// 中记录了节点 ID 与其 Member 实例的对应关系. 在 Member 中记录了对
	// 应节点暴露给集群其他节点的 URL 地址（RaftAttributes.PeerURLs）及
	// 与客户端交互的 URL 地址（Attributes.ClientURLs）.
	members    map[types.ID]*Member
	// removed contains the ids of removed members in the cluster.
	// removed id cannot be reused.
	//
	// 从当前集群中移除的节点的 ID, 在后续添加新节点时, 这些 ID 不能被再次使用.
	removed map[types.ID]bool
}

// ConfigChangeContext represents a context for confChange.
type ConfigChangeContext struct {
	Member
	// IsPromote indicates if the config change is for promoting a learner member.
	// This flag is needed because both adding a new member and promoting a learner member
	// uses the same config change type 'ConfChangeAddNode'.
	IsPromote bool `json:"isPromote"`
}

// NewClusterFromURLsMap creates a new raft cluster using provided urls map. Currently, it does not support creating
// cluster with raft learner member.
//
// NewClusterFromURLsMap urlsmap 参数中封装了集群中每个节点的名称与其提供的 URL 之间的映射, 在 NewClusterFromURLsMap()
// 函数中会根据该映射关系创建相应的 Member 实例和 RaftCluster 实例.
func NewClusterFromURLsMap(lg *zap.Logger, token string, urlsmap types.URLsMap) (*RaftCluster, error) {
	// 创建 RaftCluster 实例
	c := NewCluster(lg, token)
	// 遍历集群中节点的地址, 并创建对应的 Member 实例
	for name, urls := range urlsmap {
		// 创建一个 Member 实例
		m := NewMember(name, urls, token, nil)
		// 若该 Member.ID 对应的 Member 实例已存在, 则报错退出
		if _, ok := c.members[m.ID]; ok {
			return nil, fmt.Errorf("member exists with identical ID %v", m)
		}
		if uint64(m.ID) == raft.None {
			return nil, fmt.Errorf("cannot use %x as member id", raft.None)
		}
		// 将新创建的 Member 实例与该 ID 的映射关系保存到 members 字段中
		c.members[m.ID] = m
	}
	// 通过所有节点的 ID, 为集群生成一个 ID
	c.genID()
	return c, nil
}

func NewClusterFromMembers(lg *zap.Logger, token string, id types.ID, membs []*Member) *RaftCluster {
	c := NewCluster(lg, token)
	c.cid = id
	for _, m := range membs {
		c.members[m.ID] = m
	}
	return c
}

func NewCluster(lg *zap.Logger, token string) *RaftCluster {
	return &RaftCluster{
		lg:      lg,
		token:   token,
		members: make(map[types.ID]*Member),
		removed: make(map[types.ID]bool),
	}
}

func (c *RaftCluster) ID() types.ID { return c.cid }

// Members 返回当前集群中经过排序的所有 Member 实例的副本
func (c *RaftCluster) Members() []*Member {
	c.Lock() // 加互斥锁
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.members {
		ms = append(ms, m.Clone())
	}
	sort.Sort(ms)
	return []*Member(ms)
}

// Member 返回指定节点 ID 对应的 Member 实例的副本
func (c *RaftCluster) Member(id types.ID) *Member {
	c.Lock()
	defer c.Unlock()
	return c.members[id].Clone()
}

func (c *RaftCluster) VotingMembers() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.members {
		if !m.IsLearner {
			ms = append(ms, m.Clone())
		}
	}
	sort.Sort(ms)
	return []*Member(ms)
}

// MemberByName returns a Member with the given name if exists.
// If more than one member has the given name, it will panic.
//
// MemberByName 根据节点名称从 members 字段中获取对应的 Member 实例的副本
func (c *RaftCluster) MemberByName(name string) *Member {
	c.Lock()
	defer c.Unlock()
	var memb *Member
	// 遍历所有的 Member 实例
	for _, m := range c.members {
		if m.Name == name {
			if memb != nil {
				if c.lg != nil {
					c.lg.Panic("two member with same name found", zap.String("name", name))
				} else {
					plog.Panicf("two members with the given name %q exist", name)
				}
			}
			memb = m
		}
	}
	// 获取 Member 实例的副本
	return memb.Clone()
}

// MemberIDs 返回当前集群中经过排序后的所有节点的 ID 列表
func (c *RaftCluster) MemberIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for _, m := range c.members {
		ids = append(ids, m.ID)
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}

func (c *RaftCluster) IsIDRemoved(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	return c.removed[id]
}

// PeerURLs returns a list of all peer addresses.
// The returned list is sorted in ascending lexicographical order.
//
// PeerURLs 将所有 Member 实例中的 PeerURLs 保存到一个集群中后返回（经过排序）
func (c *RaftCluster) PeerURLs() []string {
	c.Lock()
	defer c.Unlock()
	urls := make([]string, 0)
	for _, p := range c.members {
		urls = append(urls, p.PeerURLs...)
	}
	sort.Strings(urls)
	return urls
}

// ClientURLs returns a list of all client addresses.
// The returned list is sorted in ascending lexicographical order.
func (c *RaftCluster) ClientURLs() []string {
	c.Lock()
	defer c.Unlock()
	urls := make([]string, 0)
	for _, p := range c.members {
		urls = append(urls, p.ClientURLs...)
	}
	sort.Strings(urls)
	return urls
}

func (c *RaftCluster) String() string {
	c.Lock()
	defer c.Unlock()
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "{ClusterID:%s ", c.cid)
	var ms []string
	for _, m := range c.members {
		ms = append(ms, fmt.Sprintf("%+v", m))
	}
	fmt.Fprintf(b, "Members:[%s] ", strings.Join(ms, " "))
	var ids []string
	for id := range c.removed {
		ids = append(ids, id.String())
	}
	fmt.Fprintf(b, "RemovedMemberIDs:[%s]}", strings.Join(ids, " "))
	return b.String()
}

// genID 通过所有节点的 ID, 为集群生成一个 ID
func (c *RaftCluster) genID() {
	// 获取当前集群中经过排序后的所有节点 ID 列表
	mIDs := c.MemberIDs()
	b := make([]byte, 8*len(mIDs))
	for i, id := range mIDs {
		binary.BigEndian.PutUint64(b[8*i:], uint64(id))
	}
	hash := sha1.Sum(b)
	c.cid = types.ID(binary.BigEndian.Uint64(hash[:8]))
}

func (c *RaftCluster) SetID(localID, cid types.ID) {
	c.localID = localID
	c.cid = cid
}

func (c *RaftCluster) SetStore(st v2store.Store) { c.v2store = st }

func (c *RaftCluster) SetBackend(be backend.Backend) {
	c.be = be
	mustCreateBackendBuckets(c.be)
}

func (c *RaftCluster) Recover(onSet func(*zap.Logger, *semver.Version)) {
	c.Lock()
	defer c.Unlock()

	c.members, c.removed = membersFromStore(c.lg, c.v2store)
	c.version = clusterVersionFromStore(c.lg, c.v2store)
	mustDetectDowngrade(c.lg, c.version)
	onSet(c.lg, c.version)

	for _, m := range c.members {
		if c.lg != nil {
			c.lg.Info(
				"recovered/added member from store",
				zap.String("cluster-id", c.cid.String()),
				zap.String("local-member-id", c.localID.String()),
				zap.String("recovered-remote-peer-id", m.ID.String()),
				zap.Strings("recovered-remote-peer-urls", m.PeerURLs),
			)
		} else {
			plog.Infof("added member %s %v to cluster %s from store", m.ID, m.PeerURLs, c.cid)
		}
	}
	if c.version != nil {
		if c.lg != nil {
			c.lg.Info(
				"set cluster version from store",
				zap.String("cluster-version", version.Cluster(c.version.String())),
			)
		} else {
			plog.Infof("set the cluster version to %v from store", version.Cluster(c.version.String()))
		}
	}
}

// ValidateConfigurationChange takes a proposed ConfChange and
// ensures that it is still valid.
//
// ValidateConfigurationChange 检测待修改的节点信息是否合法
func (c *RaftCluster) ValidateConfigurationChange(cc raftpb.ConfChange) error {
	// 从 v2 存储中获取当前集群中存在的节点信息（members 变量）, 以及已移除的节点信息（removed 变量）.
	// 这里 members 的类型是 map[types.ID]*Member, removed 的类型是 map[types.ID]bool.
	// members 的内容实际上记录在 v2 存储中的 "/0/members" 节点下, removed 的内容记录在 v2 存储的
	// "/0/removed_members" 节点下, membersFromStore() 方法就是从这两个节点下查找相应信息的.
	members, removed := membersFromStore(c.lg, c.v2store)
	id := types.ID(cc.NodeID) // 从 ConfChange 实例中获取待操作的节点 id
	// 检测该节点是否在 removed 中, 如果存在, 则返回错误
	if removed[id] {
		return ErrIDRemoved
	}
	// 根据 ConfChange 的类型进行分类处理
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		// 将 ConfChange.Context 中的数据反序列化成 ConfigChangeContext 实例
		confChangeContext := new(ConfigChangeContext)
		if err := json.Unmarshal(cc.Context, confChangeContext); err != nil {
			if c.lg != nil {
				c.lg.Panic("failed to unmarshal confChangeContext", zap.Error(err))
			} else {
				plog.Panicf("unmarshal confChangeContext should never fail: %v", err)
			}
		}

		if confChangeContext.IsPromote { // promoting a learner member to voting member
			if members[id] == nil {
				return ErrIDNotFound
			}
			if !members[id].IsLearner {
				return ErrMemberNotLearner
			}
		} else { // adding a new member
			// 检测新增节点是否在 members 中, 如果存在, 则返回错误
			if members[id] != nil {
				return ErrIDExists
			}

			urls := make(map[string]bool)
			// 遍历当前集群中全部的 Member 实例, 并将每个节点暴露的 URL 记录到 urls 这个 map 中
			for _, m := range members {
				for _, u := range m.PeerURLs {
					urls[u] = true
				}
			}
			// 遍历新增节点提供的 URL 是否与集群中已有节点提供的 URL 地址冲突
			for _, u := range confChangeContext.Member.PeerURLs {
				if urls[u] {
					return ErrPeerURLexists
				}
			}

			if confChangeContext.Member.IsLearner { // the new member is a learner
				numLearners := 0
				for _, m := range members {
					if m.IsLearner {
						numLearners++
					}
				}
				if numLearners+1 > maxLearners {
					return ErrTooManyLearners
				}
			}
		}
	case raftpb.ConfChangeRemoveNode:
		// 检测待删除节点是否存在于 members 中
		if members[id] == nil {
			return ErrIDNotFound
		}

	case raftpb.ConfChangeUpdateNode:
		// 检测待更新节点是否存在于 members 中
		if members[id] == nil {
			return ErrIDNotFound
		}
		urls := make(map[string]bool)
		// 检测节点更新之后提供的 URL 地址是否与集群中其他节点提供的 URL 地址冲突
		for _, m := range members {
			if m.ID == id {
				continue
			}
			for _, u := range m.PeerURLs {
				urls[u] = true
			}
		}
		m := new(Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			if c.lg != nil {
				c.lg.Panic("failed to unmarshal member", zap.Error(err))
			} else {
				plog.Panicf("unmarshal member should never fail: %v", err)
			}
		}
		for _, u := range m.PeerURLs {
			if urls[u] {
				return ErrPeerURLexists
			}
		}

	default:
		if c.lg != nil {
			c.lg.Panic("unknown ConfChange type", zap.String("type", cc.Type.String()))
		} else {
			plog.Panicf("ConfChange type should be either AddNode, RemoveNode or UpdateNode")
		}
	}
	return nil
}

// AddMember adds a new Member into the cluster, and saves the given member's
// raftAttributes into the store. The given member should have empty attributes.
// A Member with a matching id must not exist.
func (c *RaftCluster) AddMember(m *Member) {
	c.Lock()
	defer c.Unlock()
	if c.v2store != nil {
		// 将 Member 序列化后保存到 v2 存储中
		mustSaveMemberToStore(c.v2store, m)
	}
	if c.be != nil {
		// 将 Member 序列化后保存到 v3 存储中
		mustSaveMemberToBackend(c.be, m)
	}

	// 将该 Member 实例与其 ID 的对应关系存储到 RaftCluster.members 这个 map 中
	c.members[m.ID] = m

	if c.lg != nil {
		c.lg.Info(
			"added member",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("added-peer-id", m.ID.String()),
			zap.Strings("added-peer-peer-urls", m.PeerURLs),
		)
	} else {
		plog.Infof("added member %s %v to cluster %s", m.ID, m.PeerURLs, c.cid)
	}
}

// RemoveMember removes a member from the store.
// The given id MUST exist, or the function panics.
func (c *RaftCluster) RemoveMember(id types.ID) {
	c.Lock()
	defer c.Unlock()
	if c.v2store != nil {
		mustDeleteMemberFromStore(c.v2store, id)
	}
	if c.be != nil {
		mustDeleteMemberFromBackend(c.be, id)
	}

	m, ok := c.members[id]
	delete(c.members, id)
	c.removed[id] = true

	if c.lg != nil {
		if ok {
			c.lg.Info(
				"removed member",
				zap.String("cluster-id", c.cid.String()),
				zap.String("local-member-id", c.localID.String()),
				zap.String("removed-remote-peer-id", id.String()),
				zap.Strings("removed-remote-peer-urls", m.PeerURLs),
			)
		} else {
			c.lg.Warn(
				"skipped removing already removed member",
				zap.String("cluster-id", c.cid.String()),
				zap.String("local-member-id", c.localID.String()),
				zap.String("removed-remote-peer-id", id.String()),
			)
		}
	} else {
		plog.Infof("removed member %s from cluster %s", id, c.cid)
	}
}

func (c *RaftCluster) UpdateAttributes(id types.ID, attr Attributes) {
	c.Lock()
	defer c.Unlock()

	if m, ok := c.members[id]; ok {
		m.Attributes = attr
		if c.v2store != nil {
			mustUpdateMemberAttrInStore(c.v2store, m)
		}
		if c.be != nil {
			mustSaveMemberToBackend(c.be, m)
		}
		return
	}

	_, ok := c.removed[id]
	if !ok {
		if c.lg != nil {
			c.lg.Panic(
				"failed to update; member unknown",
				zap.String("cluster-id", c.cid.String()),
				zap.String("local-member-id", c.localID.String()),
				zap.String("unknown-remote-peer-id", id.String()),
			)
		} else {
			plog.Panicf("error updating attributes of unknown member %s", id)
		}
	}

	if c.lg != nil {
		c.lg.Warn(
			"skipped attributes update of removed member",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("updated-peer-id", id.String()),
		)
	} else {
		plog.Warningf("skipped updating attributes of removed member %s", id)
	}
}

// PromoteMember marks the member's IsLearner RaftAttributes to false.
func (c *RaftCluster) PromoteMember(id types.ID) {
	c.Lock()
	defer c.Unlock()

	c.members[id].RaftAttributes.IsLearner = false
	if c.v2store != nil {
		mustUpdateMemberInStore(c.v2store, c.members[id])
	}
	if c.be != nil {
		mustSaveMemberToBackend(c.be, c.members[id])
	}

	if c.lg != nil {
		c.lg.Info(
			"promote member",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
		)
	} else {
		plog.Noticef("promote member %s in cluster %s", id, c.cid)
	}
}

func (c *RaftCluster) UpdateRaftAttributes(id types.ID, raftAttr RaftAttributes) {
	c.Lock()
	defer c.Unlock()

	c.members[id].RaftAttributes = raftAttr
	if c.v2store != nil {
		mustUpdateMemberInStore(c.v2store, c.members[id])
	}
	if c.be != nil {
		mustSaveMemberToBackend(c.be, c.members[id])
	}

	if c.lg != nil {
		c.lg.Info(
			"updated member",
			zap.String("cluster-id", c.cid.String()),
			zap.String("local-member-id", c.localID.String()),
			zap.String("updated-remote-peer-id", id.String()),
			zap.Strings("updated-remote-peer-urls", raftAttr.PeerURLs),
		)
	} else {
		plog.Noticef("updated member %s %v in cluster %s", id, raftAttr.PeerURLs, c.cid)
	}
}

func (c *RaftCluster) Version() *semver.Version {
	c.Lock()
	defer c.Unlock()
	if c.version == nil {
		return nil
	}
	return semver.Must(semver.NewVersion(c.version.String()))
}

func (c *RaftCluster) SetVersion(ver *semver.Version, onSet func(*zap.Logger, *semver.Version)) {
	c.Lock()
	defer c.Unlock()
	if c.version != nil {
		if c.lg != nil {
			c.lg.Info(
				"updated cluster version",
				zap.String("cluster-id", c.cid.String()),
				zap.String("local-member-id", c.localID.String()),
				zap.String("from", version.Cluster(c.version.String())),
				zap.String("from", version.Cluster(ver.String())),
			)
		} else {
			plog.Noticef("updated the cluster version from %v to %v", version.Cluster(c.version.String()), version.Cluster(ver.String()))
		}
	} else {
		if c.lg != nil {
			c.lg.Info(
				"set initial cluster version",
				zap.String("cluster-id", c.cid.String()),
				zap.String("local-member-id", c.localID.String()),
				zap.String("cluster-version", version.Cluster(ver.String())),
			)
		} else {
			plog.Noticef("set the initial cluster version to %v", version.Cluster(ver.String()))
		}
	}
	oldVer := c.version
	c.version = ver
	mustDetectDowngrade(c.lg, c.version)
	if c.v2store != nil {
		mustSaveClusterVersionToStore(c.v2store, ver)
	}
	if c.be != nil {
		mustSaveClusterVersionToBackend(c.be, ver)
	}
	if oldVer != nil {
		ClusterVersionMetrics.With(prometheus.Labels{"cluster_version": version.Cluster(oldVer.String())}).Set(0)
	}
	ClusterVersionMetrics.With(prometheus.Labels{"cluster_version": version.Cluster(ver.String())}).Set(1)
	onSet(c.lg, ver)
}

func (c *RaftCluster) IsReadyToAddVotingMember() bool {
	nmembers := 1
	nstarted := 0

	for _, member := range c.VotingMembers() {
		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	if nstarted == 1 && nmembers == 2 {
		// a case of adding a new node to 1-member cluster for restoring cluster data
		// https://github.com/etcd-io/etcd/blob/master/Documentation/v2/admin_guide.md#restoring-the-cluster
		if c.lg != nil {
			c.lg.Debug("number of started member is 1; can accept add member request")
		} else {
			plog.Debugf("The number of started member is 1. This cluster can accept add member request.")
		}
		return true
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		if c.lg != nil {
			c.lg.Warn(
				"rejecting member add; started member will be less than quorum",
				zap.Int("number-of-started-member", nstarted),
				zap.Int("quorum", nquorum),
				zap.String("cluster-id", c.cid.String()),
				zap.String("local-member-id", c.localID.String()),
			)
		} else {
			plog.Warningf("Reject add member request: the number of started member (%d) will be less than the quorum number of the cluster (%d)", nstarted, nquorum)
		}
		return false
	}

	return true
}

func (c *RaftCluster) IsReadyToRemoveVotingMember(id uint64) bool {
	nmembers := 0
	nstarted := 0

	for _, member := range c.VotingMembers() {
		if uint64(member.ID) == id {
			continue
		}

		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		if c.lg != nil {
			c.lg.Warn(
				"rejecting member remove; started member will be less than quorum",
				zap.Int("number-of-started-member", nstarted),
				zap.Int("quorum", nquorum),
				zap.String("cluster-id", c.cid.String()),
				zap.String("local-member-id", c.localID.String()),
			)
		} else {
			plog.Warningf("Reject remove member request: the number of started member (%d) will be less than the quorum number of the cluster (%d)", nstarted, nquorum)
		}
		return false
	}

	return true
}

func (c *RaftCluster) IsReadyToPromoteMember(id uint64) bool {
	nmembers := 1 // We count the learner to be promoted for the future quorum
	nstarted := 1 // and we also count it as started.

	for _, member := range c.VotingMembers() {
		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		if c.lg != nil {
			c.lg.Warn(
				"rejecting member promote; started member will be less than quorum",
				zap.Int("number-of-started-member", nstarted),
				zap.Int("quorum", nquorum),
				zap.String("cluster-id", c.cid.String()),
				zap.String("local-member-id", c.localID.String()),
			)
		} else {
			plog.Warningf("Reject promote member request: the number of started member (%d) will be less than the quorum number of the cluster (%d)", nstarted, nquorum)
		}
		return false
	}

	return true
}

func membersFromStore(lg *zap.Logger, st v2store.Store) (map[types.ID]*Member, map[types.ID]bool) {
	members := make(map[types.ID]*Member)
	removed := make(map[types.ID]bool)
	e, err := st.Get(StoreMembersPrefix, true, true)
	if err != nil {
		if isKeyNotFound(err) {
			return members, removed
		}
		if lg != nil {
			lg.Panic("failed to get members from store", zap.String("path", StoreMembersPrefix), zap.Error(err))
		} else {
			plog.Panicf("get storeMembers should never fail: %v", err)
		}
	}
	for _, n := range e.Node.Nodes {
		var m *Member
		m, err = nodeToMember(n)
		if err != nil {
			if lg != nil {
				lg.Panic("failed to nodeToMember", zap.Error(err))
			} else {
				plog.Panicf("nodeToMember should never fail: %v", err)
			}
		}
		members[m.ID] = m
	}

	e, err = st.Get(storeRemovedMembersPrefix, true, true)
	if err != nil {
		if isKeyNotFound(err) {
			return members, removed
		}
		if lg != nil {
			lg.Panic(
				"failed to get removed members from store",
				zap.String("path", storeRemovedMembersPrefix),
				zap.Error(err),
			)
		} else {
			plog.Panicf("get storeRemovedMembers should never fail: %v", err)
		}
	}
	for _, n := range e.Node.Nodes {
		removed[MustParseMemberIDFromKey(n.Key)] = true
	}
	return members, removed
}

func clusterVersionFromStore(lg *zap.Logger, st v2store.Store) *semver.Version {
	e, err := st.Get(path.Join(storePrefix, "version"), false, false)
	if err != nil {
		if isKeyNotFound(err) {
			return nil
		}
		if lg != nil {
			lg.Panic(
				"failed to get cluster version from store",
				zap.String("path", path.Join(storePrefix, "version")),
				zap.Error(err),
			)
		} else {
			plog.Panicf("unexpected error (%v) when getting cluster version from store", err)
		}
	}
	return semver.Must(semver.NewVersion(*e.Node.Value))
}

// ValidateClusterAndAssignIDs validates the local cluster by matching the PeerURLs
// with the existing cluster. If the validation succeeds, it assigns the IDs
// from the existing cluster to the local cluster.
// If the validation fails, an error will be returned.
//
// ValidateClusterAndAssignIDs 将从远端获取到 RaftCluster 实例（即 existing）与本地生成
// 的 RaftCluster 实例（即 local）进行比较.
func ValidateClusterAndAssignIDs(lg *zap.Logger, local *RaftCluster, existing *RaftCluster) error {
	// 获取本地 RaftCluster 实例和远端 RaftCluster 实例中记录的 Member 实例, 需要注意的是,
	// 这里返回的是 RaftCluster.members 字段中记录的 Member 实例的副本.
	ems := existing.Members()
	lms := local.Members()
	// 首先, 比较 ems 和 lms 的长度是否相等, 若不相等, 则直接返回错误
	if len(ems) != len(lms) {
		return fmt.Errorf("member count is unequal")
	}
	// 对 ems 和 lms 进行排序
	sort.Sort(MembersByPeerURLs(ems))
	sort.Sort(MembersByPeerURLs(lms))

	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	// 遍历 ems, 检测 lms 中对应节点暴露的 URL 地址是否匹配
	for i := range ems {
		if ok, err := netutil.URLStringsEqual(ctx, lg, ems[i].PeerURLs, lms[i].PeerURLs); !ok {
			return fmt.Errorf("unmatched member while checking PeerURLs (%v)", err)
		}
		// 记录匹配的 Member 实例
		lms[i].ID = ems[i].ID
	}
	// 清空本地 RaftCluster.members 字段
	local.members = make(map[types.ID]*Member)
	// 更新本地 RaftCluster.members 字段
	for _, m := range lms {
		local.members[m.ID] = m
	}
	return nil
}

func mustDetectDowngrade(lg *zap.Logger, cv *semver.Version) {
	lv := semver.Must(semver.NewVersion(version.Version))
	// only keep major.minor version for comparison against cluster version
	lv = &semver.Version{Major: lv.Major, Minor: lv.Minor}
	if cv != nil && lv.LessThan(*cv) {
		if lg != nil {
			lg.Fatal(
				"invalid downgrade; server version is lower than determined cluster version",
				zap.String("current-server-version", version.Version),
				zap.String("determined-cluster-version", version.Cluster(cv.String())),
			)
		} else {
			plog.Fatalf("cluster cannot be downgraded (current version: %s is lower than determined cluster version: %s).", version.Version, version.Cluster(cv.String()))
		}
	}
}

// IsLocalMemberLearner returns if the local member is raft learner
func (c *RaftCluster) IsLocalMemberLearner() bool {
	c.Lock()
	defer c.Unlock()
	localMember, ok := c.members[c.localID]
	if !ok {
		if c.lg != nil {
			c.lg.Panic(
				"failed to find local ID in cluster members",
				zap.String("cluster-id", c.cid.String()),
				zap.String("local-member-id", c.localID.String()),
			)
		} else {
			plog.Panicf("failed to find local ID %s in cluster %s", c.localID.String(), c.cid.String())
		}
	}
	return localMember.IsLearner
}

// IsMemberExist returns if the member with the given id exists in cluster.
func (c *RaftCluster) IsMemberExist(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	_, ok := c.members[id]
	return ok
}

// VotingMemberIDs returns the ID of voting members in cluster.
func (c *RaftCluster) VotingMemberIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for _, m := range c.members {
		if !m.IsLearner {
			ids = append(ids, m.ID)
		}
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}
