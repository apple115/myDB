package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string // the reference data
	pages map[uint64]BNode  // in-memory pages
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) []byte {
				node, _ := pages[ptr]
				// assert(ok)
				return node
			},
			new: func(node []byte) uint64 {
				// assert(BNode(node).nbytes() <= BTREE_PAGE_SIZE)
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				// assert(pages[ptr] == nil)
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				// assert(pages[ptr] != nil)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func TestBTreeInsert(t *testing.T) {
	t.Run("插入到空树", func(t *testing.T) {
		c := newC() // 使用之前定义的测试辅助结构

		key := []byte("first")
		val := []byte("value")
		c.tree.Insert(key, val)

		// 验证根节点
		rootData := c.tree.get(c.tree.root)
		root := BNode(rootData)
		if root.nkeys() != 2 { // 空树插入会创建包含2个键的节点
			t.Errorf("根节点键数量错误: 期望 2, 得到 %d", root.nkeys())
		}
		testKV(t, root, 1, key, val) // 验证插入的键值对
	})

	t.Run("根节点分裂", func(t *testing.T) {
		c := newC()
		largeVal := strings.Repeat("x", 500)

		// 插入足够多的数据使根节点分裂
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("key%03d", i))
			c.tree.Insert(key, []byte(largeVal))
		}
		// 验证根节点现在是内部节点
		rootData := c.tree.get(c.tree.root)
		root := BNode(rootData)
		if root.btype() != BNODE_NODE {
			t.Error("根节点未升级为内部节点")
		}
		// 验证所有键都存在
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("key%03d", i))
			foundVal,found := treeSearch(&c.tree, c.tree.root, key)
			if !found{
				t.Errorf("键未找到：%q",key)
			}
			if !bytes.Equal(foundVal, []byte(largeVal)){
				t.Errorf("键 %q 的值不匹配",key)
			}
		}
	})
}
