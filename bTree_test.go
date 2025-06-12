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
				node := pages[ptr]
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
func (c *C) PrintTree() {
	// fmt.Printf("Root page: %d\n", c.pages[c.tree.root])
	fmt.Println("Pages:")
	for pt, node := range c.pages {
		fmt.Println("Pointer:", pt)
		fmt.Println("data: ", node)
	}
}

func treeSearch(tree *BTree, ptr uint64, key []byte) ([]byte, bool) {
	node := BNode(tree.get(ptr))
	idx := nodeLookupLE(node, key)
	if idx == 0{
		return nil, false
	}
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(node.getKey(idx), key) {
			return node.getVal(idx), true
		}
	case BNODE_NODE:
		return treeSearch(tree, node.getPtr(idx), key)
	default:
		panic("treeSearch: bad node type")
	}
	return nil, false
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
			foundVal, found := treeSearch(&c.tree, c.tree.root, key)
			if !found {
				t.Errorf("键未找到：%q", key)
			}
			if !bytes.Equal(foundVal, []byte(largeVal)) {
				t.Errorf("键 %q 的值不匹配", key)
			}
		}
	})
}

func TestTreeDelete(t *testing.T) {
	t.Run("delete from leaf node", func(t *testing.T) {
		c := newC()

		// Setup test data
		c.add("key1", "val1")
		c.add("key2", "val2")
		c.add("key3", "val3")

		// Get the leaf node
		leafPtr := c.tree.root
		if BNode(c.tree.get(leafPtr)).btype() == BNODE_NODE {
			leafPtr = BNode(c.tree.get(leafPtr)).getPtr(0)
		}

		testKV(t, c.tree.get(c.tree.root), 1, []byte("key1"), []byte("val1"))
		testKV(t, c.tree.get(c.tree.root), 2, []byte("key2"), []byte("val2"))
		testKV(t, c.tree.get(c.tree.root), 3, []byte("key3"), []byte("val3"))
		leaf := BNode(c.tree.get(leafPtr))
		// Test deletion
		result := treeDelete(&c.tree, leaf, []byte("key2"))
		if len(result) == 0 {
			t.Error("Failed to delete existing key")
		}
		// Verify result
		if result.nkeys() != 3 {
			t.Errorf("Expected 3 keys after deletion, got %d", result.nkeys())
		}
		testKV(t, result, 1, []byte("key1"), []byte("val1"))
		testKV(t, result, 2, []byte("key3"), []byte("val3"))
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		c := newC()
		c.add("key1", "val1")

		leafPtr := c.tree.root
		if BNode(c.tree.get(leafPtr)).btype() == BNODE_NODE {
			leafPtr = BNode(c.tree.get(leafPtr)).getPtr(0)
		}
		leaf := BNode(c.tree.get(leafPtr))

		result := treeDelete(&c.tree, leaf, []byte("nonexistent"))
		if len(result) != 0 {
			t.Error("Expected empty result for non-existent key")
		}
	})

	t.Run("delete from internal node", func(t *testing.T) {
		c := newC()

		// Insert enough data to create a multi-level tree
		//6*100 + 3+100
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key%03d", i)
			c.add(key, "value")
		}

		// Get an internal node
		root := BNode(c.tree.get(c.tree.root))
		if root.btype() != BNODE_NODE {
			t.Fatal("Expected internal node")
		}
		c.PrintTree()

		// Test deletion
		testKey := []byte("key050")
		result := treeDelete(&c.tree, root, testKey)
		if len(result) == 0 {
			t.Error("Failed to delete from internal node")
		}

		// Verify the key is actually gone
		if _, found := treeSearch(&c.tree, c.tree.root, testKey); found {
			t.Error("Key still exists after deletion")
		}
	})

	t.Run("invalid node type", func(t *testing.T) {
		c := newC()
		badNode := BNode(make([]byte, BTREE_PAGE_SIZE))
		badNode.setHeader(3, 0) // Invalid type

		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for bad node type")
			}
		}()

		treeDelete(&c.tree, badNode, []byte("any"))
	})
}

func TestBTree(t *testing.T) {
	t.Run("钥匙已排序", func(t *testing.T) {
		c := newC()
		keys := []string{"z", "a", "c", "b", "d"}
		for _, key := range keys {
			c.add(key, "value")
		}
		var prevKey []byte
		for _, key := range []string{"a", "b", "c", "d", "z"} {
			currentKey := []byte(key)
			// 检查键是否存在
			val, found := treeSearch(&c.tree, c.tree.root, currentKey)
			if !found {
				t.Errorf("键 %q 未找到", key)
			}
			if string(val) != "value" {
				t.Errorf("键 %q 的值不匹配", key)
			}
			// 检查顺序
			if prevKey != nil && bytes.Compare(prevKey, currentKey) >= 0 {
				t.Errorf("键顺序错误: %q 应该在 %q 之后", prevKey, currentKey)
			}
			prevKey = currentKey
		}
	})
	t.Run("节点大小在范围内", func(t *testing.T) {
		c := newC()
		// 插入足够数据使树有多层节点
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key%03d", i)
			c.add(key, strings.Repeat("x", 100)) // 中等大小的值
		}

		// 检查所有节点大小是否在合理范围内
		for ptr, node := range c.pages {
			size := node.nbytes()
			if size > BTREE_PAGE_SIZE {
				t.Errorf("节点 %d 大小 %d 超过最大页面大小 %d", ptr, size, BTREE_PAGE_SIZE)
			}
			if size < BTREE_PAGE_SIZE/4 && ptr != c.tree.root {
				t.Errorf("节点 %d 大小 %d 过小(小于页面大小的1/4)", ptr, size)
			}
		}
	})
}
