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

package v2store

import (
	"path"
	"sort"
	"time"

	"go.etcd.io/etcd/etcdserver/api/v2error"

	"github.com/jonboulle/clockwork"
)

// explanations of Compare function result
const (
	CompareMatch = iota
	CompareIndexNotMatch
	CompareValueNotMatch
	CompareNotMatch
)

var Permanent time.Time

// node is the basic element in the store system.
// A key-value pair will have a string value
// A directory will have a children map
//
// node 结构体是 etcd v2 版本最基本的元素, v2 版本存储是树形结构的, node 既可以表示其中的一个键值对（叶子节点）,
// 也可以表示一个目录（非叶子节点）.
type node struct {
	// 当前节点的路径, 格式类似 "/dir/dir2/key"
	Path string

	// 记录创建当前节点时对应的 CurrentIndex 值.
	CreatedIndex  uint64
	// 记录最后一次更新当前节点时对应的 CurrentIndex 值.
	ModifiedIndex uint64

	// 指向父节点的指针
	Parent *node `json:"-"` // should not encode this field! avoid circular dependency.

	// 当前节点的过期时间, 如果该字段被设置为 0, 则表示当前节点是一个 "永久节点"（即不会被过期删除）.
	// node.IsPermanent() 方法通天检测该字段判断当前节点是否为 "永久节点".
	ExpireTime time.Time
	// 如果当前节点是一个键值对, 则该字段记录了对应的值.
	Value      string           // for key-value pair
	// 如果当前节点表示一个目录节点, 则该字段记录了其子节点. node.IsDir() 方法通过检查该字段判断
	// 当前节点是否为目录节点.
	Children   map[string]*node // for directory

	// A reference to the store this node is attached to.
	//
	// 记录当前节点关联的 v2 版本存储实例
	store *store
}

// newKV creates a Key-Value pair
//
// newKV 创建一个表示键值对类型的 node 实例
func newKV(store *store, nodePath string, value string, createdIndex uint64, parent *node, expireTime time.Time) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		store:         store,
		ExpireTime:    expireTime,
		Value:         value,
	}
}

// newDir creates a directory
//
// newDir 创建一个表示目录类型的 node 实例
func newDir(store *store, nodePath string, createdIndex uint64, parent *node, expireTime time.Time) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		ExpireTime:    expireTime,
		Children:      make(map[string]*node),
		store:         store,
	}
}

// IsHidden function checks if the node is a hidden node. A hidden node
// will begin with '_'
// A hidden node will not be shown via get command under a directory
// For example if we have /foo/_hidden and /foo/notHidden, get "/foo"
// will only return /foo/notHidden
//
// IsHidden 检测当前节点是否为隐藏节点. v2 存户将以下划线开头的节点定义为 "隐藏节点".
func (n *node) IsHidden() bool {
	// 按照最后一个 "/" 符号进行切分, 获取节点所在的目录以及节点名称
	_, name := path.Split(n.Path)

	// 检查当前节点是否以下划线开头
	return name[0] == '_'
}

// IsPermanent function checks if the node is a permanent one.
//
// IsPermanent 通过检测 node.ExpireTime 是否为 0 来判断当前节点是否为永久节点.
func (n *node) IsPermanent() bool {
	// we use a uninitialized time.Time to indicate the node is a
	// permanent one.
	// the uninitialized time.Time should equal zero.
	return n.ExpireTime.IsZero()
}

// IsDir function checks whether the node is a directory.
// If the node is a directory, the function will return true.
// Otherwise the function will return false.
func (n *node) IsDir() bool {
	return n.Children != nil
}

// Read function gets the value of the node.
// If the receiver node is not a key-value pair, a "Not A File" error will be returned.
//
// Read 方法用来读取键值对节点的值
func (n *node) Read() (string, *v2error.Error) {
	// 若为目录节点, 则抛出异常
	if n.IsDir() {
		return "", v2error.NewError(v2error.EcodeNotFile, "", n.store.CurrentIndex)
	}

	return n.Value, nil
}

// Write function set the value of the node to the given value.
// If the receiver node is a directory, a "Not A File" error will be returned.
func (n *node) Write(value string, index uint64) *v2error.Error {
	if n.IsDir() {
		return v2error.NewError(v2error.EcodeNotFile, "", n.store.CurrentIndex)
	}

	n.Value = value         // 更新值
	n.ModifiedIndex = index // 记录更新时对应的 CurrentIndex 值

	return nil
}

// expirationAndTTL 计算当前节点的存活时间
func (n *node) expirationAndTTL(clock clockwork.Clock) (*time.Time, int64) {
	// 检测当前节点是否为永久节点
	if !n.IsPermanent() {
		/* compute ttl as:
		   ceiling( (expireTime - timeNow) / nanosecondsPerSecond )
		   which ranges from 1..n
		   rather than as:
		   ( (expireTime - timeNow) / nanosecondsPerSecond ) + 1
		   which ranges 1..n+1
		*/
		// 节点过期时间减去当前时间, 即为剩余存活时间
		ttlN := n.ExpireTime.Sub(clock.Now())
		ttl := ttlN / time.Second
		if (ttlN % time.Second) > 0 { // 以秒为单位, 不足一秒按一秒算
			ttl++
		}
		t := n.ExpireTime.UTC()
		return &t, int64(ttl) // 返回当前节点的过期时间和剩余存活时间
	}
	// 永久节点对应返回值
	return nil, 0
}

// List function return a slice of nodes under the receiver node.
// If the receiver node is not a directory, a "Not A Directory" error will be returned.
//
// List 获取当前目录节点的子节点
func (n *node) List() ([]*node, *v2error.Error) {
	// 非目录节点则抛出异常
	if !n.IsDir() {
		return nil, v2error.NewError(v2error.EcodeNotDir, "", n.store.CurrentIndex)
	}

	nodes := make([]*node, len(n.Children))

	i := 0
	// 获取子节点
	for _, node := range n.Children {
		nodes[i] = node
		i++
	}

	return nodes, nil
}

// GetChild function returns the child node under the directory node.
// On success, it returns the file node
//
// GetChild 获取指定的子节点
func (n *node) GetChild(name string) (*node, *v2error.Error) {
	if !n.IsDir() {
		return nil, v2error.NewError(v2error.EcodeNotDir, n.Path, n.store.CurrentIndex)
	}

	child, ok := n.Children[name]

	if ok {
		return child, nil
	}

	return nil, nil
}

// Add function adds a node to the receiver node.
// If the receiver is not a directory, a "Not A Directory" error will be returned.
// If there is an existing node with the same name under the directory, a "Already Exist"
// error will be returned
//
// Add 在当前节点下添加子节点
func (n *node) Add(child *node) *v2error.Error {
	// 检测当前节点是否为目录节点
	if !n.IsDir() {
		return v2error.NewError(v2error.EcodeNotDir, "", n.store.CurrentIndex)
	}

	// 按照 "/" 进行切分, 得到待添加子节点对应的 key
	_, name := path.Split(child.Path)

	// 检测当前节点的 Child 字段中是否已存在待添加子节点
	if _, ok := n.Children[name]; ok {
		return v2error.NewError(v2error.EcodeNodeExist, "", n.store.CurrentIndex)
	}

	// 添加子节点
	n.Children[name] = child

	return nil
}

// Remove function remove the node.
//
// Remove 将当前节点从其父节点中删除, 其中会根据参数决定是否递归删除当前节点的子节点.
func (n *node) Remove(dir, recursive bool, callback func(path string)) *v2error.Error {
	// 若当前节点为 KV 节点
	if !n.IsDir() { // key-value pair
		// 获取当前节点对应的 key
		_, name := path.Split(n.Path)

		// find its parent and remove the node from the map
		//
		// 查找当前节点的父节点, 并将当前节点从父节点的 Child 字段中删除
		if n.Parent != nil && n.Parent.Children[name] == n {
			delete(n.Parent.Children, name)
		}

		// 回调 callback 函数
		if callback != nil {
			callback(n.Path)
		}

		// 当前节点非永久节点, 则将其从 store.ttlKeyHeap 中删除,
		// store.ttlKeyHeap 中的节点是按照过期时间排序的
		if !n.IsPermanent() {
			n.store.ttlKeyHeap.remove(n)
		}

		return nil
	}

	// 下面是针对目录节点的删除操作

	// 只有 dir 参数设置为 true, 才能删除该目录节点
	if !dir {
		// cannot delete a directory without dir set to true
		return v2error.NewError(v2error.EcodeNotFile, n.Path, n.store.CurrentIndex)
	}

	// 如果当前目录节点还存在子节点, 且不能进行递归删除（recursive 参数设置为 false, 则不能删除该目录节点）
	if len(n.Children) != 0 && !recursive {
		// cannot delete a directory if it is not empty and the operation
		// is not recursive
		return v2error.NewError(v2error.EcodeDirNotEmpty, n.Path, n.store.CurrentIndex)
	}

	// 递归删除当前目录节点的全部子节点
	for _, child := range n.Children { // delete all children
		child.Remove(true, true, callback)
	}

	// delete self
	// 删除当前节点自身
	_, name := path.Split(n.Path)
	if n.Parent != nil && n.Parent.Children[name] == n {
		delete(n.Parent.Children, name)

		if callback != nil {
			callback(n.Path)
		}

		if !n.IsPermanent() {
			n.store.ttlKeyHeap.remove(n)
		}
	}

	return nil
}

// Repr 会将当前节点和子节点（根据参数决定是否递归处理子节点以及子节点是否排序）转换成 NodeExtern 实例返回.
func (n *node) Repr(recursive, sorted bool, clock clockwork.Clock) *NodeExtern {
	// 针对目录节点的处理
	if n.IsDir() {
		node := &NodeExtern{
			Key:           n.Path,
			Dir:           true,
			ModifiedIndex: n.ModifiedIndex,
			CreatedIndex:  n.CreatedIndex,
		}
		// 计算当前节点的过期时间和 TTL（即当前节点还能存活多长时间）
		node.Expiration, node.TTL = n.expirationAndTTL(clock)

		// recursive 参数表示是否递归获取子节点
		if !recursive {
			return node
		}

		// 获取子节点
		children, _ := n.List()
		node.Nodes = make(NodeExterns, len(children))

		// we do not use the index in the children slice directly
		// we need to skip the hidden one
		i := 0

		// 遍历子节点
		for _, child := range children {

			// 忽略隐藏节点
			if child.IsHidden() { // get will not list hidden node
				continue
			}

			// 调用子节点的 Repr() 方法, 创建对应的 NodeExtern 实例
			node.Nodes[i] = child.Repr(recursive, sorted, clock)

			i++
		}

		// eliminate hidden nodes
		node.Nodes = node.Nodes[:i] // 进行简单压缩, 释放隐藏节点导致的空间间隙
		// 根据 sorted 参数, 决定是否对子节点进行排序
		if sorted {
			sort.Sort(node.Nodes)
		}

		return node
	}

	// since n.Value could be changed later, so we need to copy the value out
	//
	// 针对 KV 节点的处理
	value := n.Value // 复制当前节点的 Value 值
	// 根据当前节点创建对应的 NodeExtern 实例, 并初始化相应字段
	node := &NodeExtern{
		Key:           n.Path,
		Value:         &value,
		ModifiedIndex: n.ModifiedIndex,
		CreatedIndex:  n.CreatedIndex,
	}
	// 计算当前节点的过期时间和 TTL
	node.Expiration, node.TTL = n.expirationAndTTL(clock)
	return node
}

func (n *node) UpdateTTL(expireTime time.Time) {
	if !n.IsPermanent() {
		if expireTime.IsZero() {
			// from ttl to permanent
			n.ExpireTime = expireTime
			// remove from ttl heap
			n.store.ttlKeyHeap.remove(n)
			return
		}

		// update ttl
		n.ExpireTime = expireTime
		// update ttl heap
		n.store.ttlKeyHeap.update(n)
		return
	}

	if expireTime.IsZero() {
		return
	}

	// from permanent to ttl
	n.ExpireTime = expireTime
	// push into ttl heap
	n.store.ttlKeyHeap.push(n)
}

// Compare function compares node index and value with provided ones.
// second result value explains result and equals to one of Compare.. constants
func (n *node) Compare(prevValue string, prevIndex uint64) (ok bool, which int) {
	indexMatch := prevIndex == 0 || n.ModifiedIndex == prevIndex
	valueMatch := prevValue == "" || n.Value == prevValue
	ok = valueMatch && indexMatch
	switch {
	case valueMatch && indexMatch:
		which = CompareMatch
	case indexMatch && !valueMatch:
		which = CompareValueNotMatch
	case valueMatch && !indexMatch:
		which = CompareIndexNotMatch
	default:
		which = CompareNotMatch
	}
	return ok, which
}

// Clone function clone the node recursively and return the new node.
// If the node is a directory, it will clone all the content under this directory.
// If the node is a key-value pair, it will clone the pair.
func (n *node) Clone() *node {
	if !n.IsDir() {
		newkv := newKV(n.store, n.Path, n.Value, n.CreatedIndex, n.Parent, n.ExpireTime)
		newkv.ModifiedIndex = n.ModifiedIndex
		return newkv
	}

	clone := newDir(n.store, n.Path, n.CreatedIndex, n.Parent, n.ExpireTime)
	clone.ModifiedIndex = n.ModifiedIndex

	for key, child := range n.Children {
		clone.Children[key] = child.Clone()
	}

	return clone
}

// recoverAndclean function help to do recovery.
// Two things need to be done: 1. recovery structure; 2. delete expired nodes
//
// If the node is a directory, it will help recover children's parent pointer and recursively
// call this function on its children.
// We check the expire last since we need to recover the whole structure first and add all the
// notifications into the event history.
func (n *node) recoverAndclean() {
	if n.IsDir() {
		for _, child := range n.Children {
			child.Parent = n
			child.store = n.store
			child.recoverAndclean()
		}
	}

	if !n.ExpireTime.IsZero() {
		n.store.ttlKeyHeap.push(n)
	}
}
