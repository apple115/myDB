package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func testPtr(t *testing.T, node BNode, idx uint16, ptrVal uint64) {
	t.Helper()
	if ptr := node.getPtr(idx); ptr != ptrVal {
		t.Errorf("在索引为'%d' 指针错误: 期望 %d, 得到 %d", idx, ptrVal, ptr)
	}
}

func testKV(t *testing.T, node BNode, idx uint16, keyVal, valVal []byte) {
	t.Helper()
	if key := node.getKey(idx); !bytes.Equal(key, keyVal) {
		t.Errorf("在索引为'%d' key错误: 期望 '%s', 得到 '%s'", idx, keyVal, key)
	}
	if val := node.getVal(idx); !bytes.Equal(val, valVal) {
		t.Errorf("在索引为'%d' val错误: 期望 '%s', 得到 '%s'", idx, valVal, val)
	}
}

func testOffset(t *testing.T, node BNode, idx uint16, offsetVal uint16) {
	t.Helper()
	if offset := node.getOffset(idx); offset != offsetVal {
		t.Errorf("在索引为'%d' 偏移量错误: 期望 %d, 得到 %d", idx, offsetVal, offset)
	}
}

func TestBNode(t *testing.T) {
	t.Run("小节点", func(t *testing.T) {
		nodeData := make([]byte, BTREE_PAGE_SIZE)

		//设置头部信息：type=2, nkeys=2
		binary.LittleEndian.PutUint16(nodeData[0:2], 2)
		binary.LittleEndian.PutUint16(nodeData[2:4], 2)

		// 设置偏移量：[8, 19]
		binary.LittleEndian.PutUint16(nodeData[20:22], 8)  // 第一个偏移量
		binary.LittleEndian.PutUint16(nodeData[22:24], 19) // 第二个偏移量

		// 设置第一个键值对：klen=2, vlen=2, key="k1", val="hi"
		binary.LittleEndian.PutUint16(nodeData[24:26], 2) // klen
		binary.LittleEndian.PutUint16(nodeData[26:28], 2) // vlen
		copy(nodeData[28:30], "k1")                       // key
		copy(nodeData[30:32], "hi")                       // value

		binary.LittleEndian.PutUint16(nodeData[32:34], 2) //klen
		binary.LittleEndian.PutUint16(nodeData[34:36], 5) //vlen
		copy(nodeData[36:38], "k3")
		copy(nodeData[38:43], "hello")

		node := BNode(nodeData)

		// 验证头部信息
		if node.btype() != 2 {
			t.Errorf("btype错误: 期望 2, 得到 %d", node.btype())
		}
		if node.nkeys() != 2 {
			t.Errorf("nkeys错误: 期望 2, 得到 %d", node.nkeys())
		}
		if node.nbytes() != 43 {
			t.Errorf("nbytes错误: 期望 43, 得到 %d", node.nbytes())
		}

		// 验证指针（叶节点应为0）
		testPtr(t, node, 0, 0)
		testPtr(t, node, 1, 0)
		// 验证偏移量
		testOffset(t, node, 0, 0)
		testOffset(t, node, 1, 8)

		// 验证键值对
		testKV(t, node, 0, []byte("k1"), []byte("hi"))
		testKV(t, node, 1, []byte("k3"), []byte("hello"))

	})
	t.Run("空节点", func(t *testing.T) {
		// 创建只包含头部的节点
		nodeData := make([]byte, 4)
		binary.LittleEndian.PutUint16(nodeData[0:2], 2) // leaf
		binary.LittleEndian.PutUint16(nodeData[2:4], 0) // nkeys=0

		node := BNode(nodeData)

		// 验证头部
		if node.btype() != 2 {
			t.Errorf("btype错误: 期望 2, 得到 %d", node.btype())
		}
		if node.nkeys() != 0 {
			t.Errorf("nkeys错误: 期望 0, 得到 %d", node.nkeys())
		}

		// 验证边界情况
		testOffset(t, node, 0, 0)

		// 验证无效索引的边界情况
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("访问无效索引时应有 panic")
			}
		}()
		node.getKey(0) // 应该 panic
	})
	t.Run("大节点", func(t *testing.T) {
		// 创建大节点缓冲区
		const nkeys = 100
		nodeData := make([]byte, 4096) // 假设页面大小为 4KB

		// 设置头部
		binary.LittleEndian.PutUint16(nodeData[0:2], 2) // leaf
		binary.LittleEndian.PutUint16(nodeData[2:4], nkeys)

		// 设置指针区（叶节点全0）
		ptrStart := 4
		ptrEnd := ptrStart + 8*nkeys

		// 设置偏移量区
		offsetStart := ptrEnd
		offsetEnd := offsetStart + 2*nkeys

		// 填充键值对
		kvPos := offsetEnd
		totalSize := 0

		for i := 0; i < nkeys; i++ {
			key := []byte(fmt.Sprintf("key%04d", i))
			val := []byte(fmt.Sprintf("value%04d", i))

			// 写入偏移量（除了索引0）
			if i > 0 {
				binary.LittleEndian.PutUint16(nodeData[offsetStart+2*(i-1):], uint16(totalSize))
			}

			// 写入键值对
			binary.LittleEndian.PutUint16(nodeData[kvPos:], uint16(len(key)))
			binary.LittleEndian.PutUint16(nodeData[kvPos+2:], uint16(len(val)))
			copy(nodeData[kvPos+4:], key)
			copy(nodeData[kvPos+4+len(key):], val)

			// 更新位置
			kvSize := 4 + len(key) + len(val)
			kvPos += kvSize
			totalSize += kvSize
		}

		// 设置最后一个偏移量
		binary.LittleEndian.PutUint16(nodeData[offsetStart+2*(nkeys-1):], uint16(totalSize))

		node := BNode(nodeData)

		// 验证头部
		if node.btype() != 2 {
			t.Errorf("btype错误: 期望 2, 得到 %d", node.btype())
		}
		if node.nkeys() != nkeys {
			t.Errorf("nkeys错误: 期望 %d, 得到 %d", nkeys, node.nkeys())
		}
		if node.nbytes() != 4+100*8+100*2+100*(4+7+9) {
			t.Errorf("nbytes错误: 期望 %d, 得到 %d", 4+100*8+100*2+100*(4+7+9), node.nbytes())
		}

		// 随机验证部分键值对
		for i := 0; i < 10; i++ {
			idx := uint16(rand.Intn(nkeys))
			expectedKey := []byte(fmt.Sprintf("key%04d", idx))
			expectedVal := []byte(fmt.Sprintf("value%04d", idx))

			testKV(t, node, idx, expectedKey, expectedVal)
		}

		// 验证偏移量链
		prevOffset := uint16(0)
		for i := uint16(0); i <= nkeys; i++ {
			offset := node.getOffset(i)
			if i > 0 && offset <= prevOffset {
				t.Errorf("偏移量不递增: offset[%d]=%d, offset[%d]=%d",
					i-1, prevOffset, i, offset)
			}
			prevOffset = offset
		}
	})
}

func TestNodeAppendKV(t *testing.T) {
	//向空节点添加第一个键值对
	t.Run("append first KV", func(t *testing.T) {

		data := make([]byte, 4+8+2+4+3+5) //4(头部) + 8(指针) + 2(偏移量) + 4(klen+vlen) + 3(key) + 5(value)
		new := BNode(data)
		new.setHeader(BNODE_LEAF, 1)
		//添加第一个键值对
		nodeAppendKV(new, 0, 123, []byte("key"), []byte("value"))

		testPtr(t, new, 0, 123)
		testKV(t, new, 0, []byte("key"), []byte("value"))
		expectOffset := uint16(4 + 3 + 5) // KV 4B + key3B + val5B
		testOffset(t, new, 1, expectOffset)
	})
	t.Run("append after existing kv", func(t *testing.T) {
		data := make([]byte, 49) // 4(头部)+8(指针)*2+2(偏移量)*2+4(KV头)+4(key)+4(val)+4(KV头)+4(key2)+5(val2)

		node := BNode(data)
		node.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(node, 0, 0, []byte("k1"), []byte("hi"))
		nodeAppendKV(node, 1, 0, []byte("k3"), []byte("hello"))

		testKV(t, node, 0, []byte("k1"), []byte("hi"))
		testKV(t, node, 1, []byte("k3"), []byte("hello"))

		testOffset(t, node, 0, 0)
		testOffset(t, node, 1, 8)
		testOffset(t, node, 2, 19)
	})
	//在已有节点上添加第二个键值对
	t.Run("append after existing kv2", func(t *testing.T) {
		data := make([]byte, 50) // 4(头部)+8(指针)*2+2(偏移量)*2+4(KV头)+4(key)+4(val)+4(KV头)+4(key2)+5(val2)

		node := BNode(data)
		node.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(node, 0, 0, []byte("key1"), []byte("val1"))
		nodeAppendKV(node, 1, 0, []byte("key2"), []byte("value2"))

		testKV(t, node, 0, []byte("key1"), []byte("val1"))
		testKV(t, node, 1, []byte("key2"), []byte("value2"))

		testOffset(t, node, 0, 0)
		testOffset(t, node, 1, 12) //KV头4B + key4B + val4B
		testOffset(t, node, 2, 26) //KV头4B + key4B + val5B
	})
}

func TestNodeAppendRange(t *testing.T) {
	t.Run("append range", func(t *testing.T) {
		data := make([]byte, 50) // 4(头部)+8(指针)*2+2(偏移量)*2+4(KV头)+4(key)+4(val)+4(KV头)+4(key2)+5(val2)
		node := BNode(data)
		node.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(node, 0, 0, []byte("key1"), []byte("val1"))
		nodeAppendKV(node, 1, 0, []byte("key2"), []byte("value2"))

		newData := make([]byte, 50)
		new := BNode(newData)
		new.setHeader(BNODE_LEAF, 2)
		nodeAppendRange(new, data, 0, 0, node.nkeys())

		testKV(t, new, 0, []byte("key1"), []byte("val1"))
		testKV(t, new, 1, []byte("key2"), []byte("value2"))

		testOffset(t, new, 0, 0)
		testOffset(t, new, 1, 12) //KV头4B + key4B + val4B
		testOffset(t, new, 2, 26) //KV头4B + key4B + val5B
	})
}

func TestLeafInsert(t *testing.T) {
	// Test inserting into empty node
	t.Run("insert into empty", func(t *testing.T) {
		old := BNode(make([]byte, 26)) //4+8+2+4+4+4
		old.setHeader(BNODE_LEAF, 0)

		new := BNode(make([]byte, 26))
		leafInsert(new, old, 0, []byte("key1"), []byte("val1"))

		if new.nkeys() != 1 {
			t.Errorf("Expected 1 key after insert, got %d", new.nkeys())
		}
		testKV(t, new, 0, []byte("key1"), []byte("val1"))
	})

	// Test inserting between existing keys
	t.Run("insert middle", func(t *testing.T) {
		old := BNode(make([]byte, 50))
		old.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(old, 0, 0, []byte("a"), []byte("1"))
		nodeAppendKV(old, 1, 0, []byte("c"), []byte("3"))

		new := BNode(make([]byte, 100))
		leafInsert(new, old, 1, []byte("b"), []byte("2"))

		if new.nkeys() != 3 {
			t.Errorf("Expected 3 keys, got %d", new.nkeys())
		}
		testKV(t, new, 0, []byte("a"), []byte("1"))
		testKV(t, new, 1, []byte("b"), []byte("2"))
		testKV(t, new, 2, []byte("c"), []byte("3"))
	})

	// Test inserting at beginning
	t.Run("insert beginning", func(t *testing.T) {
		old := BNode(make([]byte, 50))
		old.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(old, 0, 0, []byte("b"), []byte("2"))
		nodeAppendKV(old, 1, 0, []byte("c"), []byte("3"))

		new := BNode(make([]byte, 100))
		leafInsert(new, old, 0, []byte("a"), []byte("1"))

		testKV(t, new, 0, []byte("a"), []byte("1"))
		testKV(t, new, 1, []byte("b"), []byte("2"))
		testKV(t, new, 2, []byte("c"), []byte("3"))
	})

	// Test inserting at end
	t.Run("insert end", func(t *testing.T) {
		old := BNode(make([]byte, 50))
		old.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(old, 0, 0, []byte("a"), []byte("1"))
		nodeAppendKV(old, 1, 0, []byte("b"), []byte("2"))

		new := BNode(make([]byte, 100))
		leafInsert(new, old, 2, []byte("c"), []byte("3"))

		testKV(t, new, 0, []byte("a"), []byte("1"))
		testKV(t, new, 1, []byte("b"), []byte("2"))
		testKV(t, new, 2, []byte("c"), []byte("3"))
	})

	// Test offsets are correct after insert
	t.Run("check offsets", func(t *testing.T) {
		old := BNode(make([]byte, 50))
		old.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(old, 0, 0, []byte("a"), []byte("1"))
		nodeAppendKV(old, 1, 0, []byte("c"), []byte("333")) // Longer value

		new := BNode(make([]byte, 100))
		leafInsert(new, old, 1, []byte("b"), []byte("22"))

		// Expected offsets:
		// 0: 0 (by definition)
		// 1: len("a1") = 1+1+4 = 6
		// 2: previous + len("b22") = 6 + (1+2+4) = 13
		// 3: previous + len("c333") = 13 + (1+3+4) = 21
		testOffset(t, new, 1, 6)
		testOffset(t, new, 2, 13)
		testOffset(t, new, 3, 21)
	})
}

func TestLeafUpdata(t *testing.T) {
	// 测试用例1：更新中间位置的键值对
	t.Run("更新中间键值对", func(t *testing.T) {
		// 准备初始节点数据
		oldData := make([]byte, 100)
		old := BNode(oldData)
		old.setHeader(BNODE_LEAF, 3)
		nodeAppendKV(old, 0, 0, []byte("key1"), []byte("val1"))
		nodeAppendKV(old, 1, 0, []byte("key2"), []byte("val2"))
		nodeAppendKV(old, 2, 0, []byte("key3"), []byte("val3"))

		// 准备新节点用于更新
		newData := make([]byte, 100)
		new := BNode(newData)

		// 执行更新操作（更新第二个键值对）
		leafUpdata(new, old, 1, []byte("key2_new"), []byte("val2_new"))

		// 验证结果
		if new.nkeys() != old.nkeys() {
			t.Errorf("键数量错误: 期望 %d, 得到 %d", old.nkeys(), new.nkeys())
		}

		// 验证第一个键值对未被修改
		testKV(t, new, 0, []byte("key1"), []byte("val1"))
		// 验证第二个键值对已更新
		testKV(t, new, 1, []byte("key2_new"), []byte("val2_new"))
		// 验证第三个键值对未被修改
		testKV(t, new, 2, []byte("key3"), []byte("val3"))
	})

	// 测试用例2：更新第一个键值对
	t.Run("更新第一个键值对", func(t *testing.T) {
		old := BNode(make([]byte, 100))
		old.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(old, 0, 0, []byte("first"), []byte("old"))
		nodeAppendKV(old, 1, 0, []byte("second"), []byte("val"))

		new := BNode(make([]byte, 100))
		leafUpdata(new, old, 0, []byte("first"), []byte("new"))

		// 验证更新结果
		testKV(t, new, 0, []byte("first"), []byte("new"))
		testKV(t, new, 1, []byte("second"), []byte("val"))
	})

	// 测试用例3：更新最后一个键值对
	t.Run("更新最后一个键值对", func(t *testing.T) {
		old := BNode(make([]byte, 100))
		old.setHeader(BNODE_LEAF, 3)
		nodeAppendKV(old, 0, 0, []byte("k1"), []byte("v1"))
		nodeAppendKV(old, 1, 0, []byte("k2"), []byte("v2"))
		nodeAppendKV(old, 2, 0, []byte("k3"), []byte("v3"))

		new := BNode(make([]byte, 100))
		leafUpdata(new, old, 2, []byte("k3"), []byte("updated"))

		// 验证前两个键值对不变
		testKV(t, new, 0, []byte("k1"), []byte("v1"))
		testKV(t, new, 1, []byte("k2"), []byte("v2"))
		// 验证最后一个键值对已更新
		testKV(t, new, 2, []byte("k3"), []byte("updated"))
	})

	// 测试用例4：验证偏移量是否正确更新
	t.Run("验证偏移量更新", func(t *testing.T) {
		old := BNode(make([]byte, 100))
		old.setHeader(BNODE_LEAF, 2)
		// 添加两个键值对，第二个值较长
		nodeAppendKV(old, 0, 0, []byte("short"), []byte("val"))
		nodeAppendKV(old, 1, 0, []byte("long"), []byte("very_long_value"))

		new := BNode(make([]byte, 100))
		// 更新第一个键值对，使用更长的值
		leafUpdata(new, old, 0, []byte("short"), []byte("new_value"))

		// 验证偏移量
		// 第一个键值对偏移量应该为0（总是）
		testOffset(t, new, 0, 0)
		// 第二个键值对偏移量应该是第一个键值对的总长度
		// "short" (5) + "new_value" (9) + 头部(4) = 18
		testOffset(t, new, 1, 18)
		// 结束偏移量应该是前一个偏移量加上第二个键值对长度
		// "long" (4) + "very_long_value" (15) + 头部(4) = 23
		// 18 + 23 = 41
		testOffset(t, new, 2, 41)
	})

	// 测试用例5：更新后键数量不变
	t.Run("键数量不变验证", func(t *testing.T) {
		old := BNode(make([]byte, 100))
		old.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(old, 0, 0, []byte("k1"), []byte("v1"))
		nodeAppendKV(old, 1, 0, []byte("k2"), []byte("v2"))

		new := BNode(make([]byte, 100))
		leafUpdata(new, old, 1, []byte("k2"), []byte("new_v2"))

		if new.nkeys() != 2 {
			t.Errorf("键数量不应改变: 期望 2, 得到 %d", new.nkeys())
		}
	})
}

func TestNodeLookupLE(t *testing.T) {
	// 准备测试数据 - 创建一个包含多个键的B树节点
	createTestNode := func() BNode {
		data := make([]byte, 100)
		node := BNode(data)
		node.setHeader(BNODE_LEAF, 3)
		nodeAppendKV(node, 0, 0, []byte("apple"), []byte("red"))
		nodeAppendKV(node, 1, 0, []byte("banana"), []byte("yellow"))
		nodeAppendKV(node, 2, 0, []byte("cherry"), []byte("red"))
		return node
	}

	// 测试用例1：查找存在的键（完全匹配）
	t.Run("查找存在的键-完全匹配", func(t *testing.T) {
		node := createTestNode()
		// 测试查找中间键
		if idx := nodeLookupLE(node, []byte("banana")); idx != 1 {
			t.Errorf("查找'banana'错误: 期望 1, 得到 %d", idx)
		}
		// 测试查找第一个键
		if idx := nodeLookupLE(node, []byte("apple")); idx != 0 {
			t.Errorf("查找'apple'错误: 期望 0, 得到 %d", idx)
		}
	})

	// 测试用例2：查找不存在的键（返回小于等于的最大键）
	t.Run("查找不存在的键", func(t *testing.T) {
		node := createTestNode()
		// 测试查找位于中间的键
		if idx := nodeLookupLE(node, []byte("blueberry")); idx != 1 {
			t.Errorf("查找'blueberry'错误: 期望 1(red), 得到 %d", idx)
		}
		// 测试查找小于所有键的值
		if idx := nodeLookupLE(node, []byte("aardvark")); idx != 0xFFFF { // 无符号下溢
			t.Errorf("查找'aardvark'错误: 期望 0xFFFF, 得到 %d", idx)
		}
		// 测试查找大于所有键的值
		if idx := nodeLookupLE(node, []byte("date")); idx != 2 {
			t.Errorf("查找'date'错误: 期望 2 (cherry), 得到 %d", idx)
		}
	})

	// 测试用例3：空节点测试
	t.Run("空节点测试", func(t *testing.T) {
		emptyNode := BNode(make([]byte, 4))
		emptyNode.setHeader(BNODE_LEAF, 0)
		if idx := nodeLookupLE(emptyNode, []byte("any")); idx != 0xFFFF {
			t.Errorf("空节点查找错误: 期望 0xFFFF, 得到 %d", idx)
		}
	})

	// 测试用例4：边界值测试
	t.Run("边界值测试", func(t *testing.T) {
		node := createTestNode()
		// 测试刚好小于第一个键
		if idx := nodeLookupLE(node, []byte("app")); idx != 0xFFFF {
			t.Errorf("边界测试1错误: 期望 0xFFFF, 得到 %d", idx)
		}
		// 测试刚好大于最后一个键
		if idx := nodeLookupLE(node, []byte("cherryx")); idx != 2 {
			t.Errorf("边界测试2错误: 期望 2, 得到 %d", idx)
		}
	})

	// 测试用例5：重复键测试
	t.Run("重复键测试", func(t *testing.T) {
		data := make([]byte, 100)
		node := BNode(data)
		node.setHeader(BNODE_LEAF, 3)
		nodeAppendKV(node, 0, 0, []byte("apple"), []byte("red"))
		nodeAppendKV(node, 1, 0, []byte("apple"), []byte("green")) // 重复键
		nodeAppendKV(node, 2, 0, []byte("banana"), []byte("yellow"))

		// 应该返回第一个匹配的位置
		if idx := nodeLookupLE(node, []byte("apple")); idx != 0 {
			t.Errorf("重复键测试错误: 期望 0, 得到 %d", idx)
		}
	})
}

func TestNodeSplit2(t *testing.T) {
	// 测试用例1：正常分裂节点
	t.Run("正常分裂节点", func(t *testing.T) {
		// 创建一个包含4个键值对的节点
		oldData := make([]byte, 200)
		old := BNode(oldData)
		old.setHeader(BNODE_LEAF, 4)
		nodeAppendKV(old, 0, 0, []byte("key1"), []byte("val1"))
		nodeAppendKV(old, 1, 0, []byte("key2"), []byte("val2"))
		nodeAppendKV(old, 2, 0, []byte("key3"), []byte("val3"))
		nodeAppendKV(old, 3, 0, []byte("key4"), []byte("val4"))

		// 准备左右节点
		left := BNode(make([]byte, BTREE_PAGE_SIZE))
		right := BNode(make([]byte, BTREE_PAGE_SIZE))

		// 执行分裂
		nodeSplit2(left, right, old)

		// 验证左节点
		if left.nkeys() != 2 {
			t.Errorf("左节点键数量错误: 期望 2, 得到 %d", left.nkeys())
		}
		testKV(t, left, 0, []byte("key1"), []byte("val1"))
		testKV(t, left, 1, []byte("key2"), []byte("val2"))

		// 验证右节点
		if right.nkeys() != 2 {
			t.Errorf("右节点键数量错误: 期望 2, 得到 %d", right.nkeys())
		}
		testKV(t, right, 0, []byte("key3"), []byte("val3"))
		testKV(t, right, 1, []byte("key4"), []byte("val4"))
	})

	// 测试用例2：奇数个键的分裂
	t.Run("奇数个键的分裂", func(t *testing.T) {
		old := BNode(make([]byte, 200))
		old.setHeader(BNODE_LEAF, 3)
		nodeAppendKV(old, 0, 0, []byte("k1"), []byte("v1"))
		nodeAppendKV(old, 1, 0, []byte("k2"), []byte("v2"))
		nodeAppendKV(old, 2, 0, []byte("k3"), []byte("v3"))

		left := BNode(make([]byte, BTREE_PAGE_SIZE))
		right := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeSplit2(left, right, old)

		// 验证分裂结果
		if left.nkeys()+right.nkeys() != old.nkeys() {
			t.Errorf("键总数不匹配: 期望 %d, 得到 %d",
				old.nkeys(), left.nkeys()+right.nkeys())
		}
	})

	// 测试用例3：中节点分裂（确保不超过页大小）
	t.Run("中节点分裂", func(t *testing.T) {
		// 创建一个大节点
		old := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
		old.setHeader(BNODE_LEAF, 10)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%02d", i) //key
			val := strings.Repeat("x", 360)  // 大值
			nodeAppendKV(old, uint16(i), 0, []byte(key), []byte(val))
		}

		left := BNode(make([]byte, BTREE_PAGE_SIZE))
		right := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeSplit2(left, right, old)

		// 验证分裂后的节点大小不超过页大小
		if left.nbytes() > BTREE_PAGE_SIZE {
			t.Errorf("左节点大小 %d 超过页大小 %d", left.nbytes(), BTREE_PAGE_SIZE)
		}
		if right.nbytes() > BTREE_PAGE_SIZE {
			t.Errorf("右节点大小 %d 超过页大小 %d", right.nbytes(), BTREE_PAGE_SIZE)
		}

		// 验证所有键都被保留
		totalKeys := left.nkeys() + right.nkeys()
		if totalKeys != old.nkeys() {
			t.Errorf("键总数不匹配: 期望 %d, 得到 %d", old.nkeys(), totalKeys)
		}
	})

	// 测试用例4：大节点分裂（确保超过页大小）
	t.Run("大节点分裂", func(t *testing.T) {
		// 创建一个大节点
		old := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
		old.setHeader(BNODE_LEAF, 10)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%02d", i) //key
			val := strings.Repeat("x", 500)  // 大值
			nodeAppendKV(old, uint16(i), 0, []byte(key), []byte(val))
		}

		left := BNode(make([]byte, BTREE_PAGE_SIZE))
		right := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeSplit2(left, right, old)

		// 验证分裂后的节点大小不超过页大小
		if left.nbytes() > BTREE_PAGE_SIZE {
			t.Errorf("左节点大小 %d 超过页大小 %d", left.nbytes(), BTREE_PAGE_SIZE)
		}
		if right.nbytes() > BTREE_PAGE_SIZE {
			t.Errorf("右节点大小 %d 超过页大小 %d", right.nbytes(), BTREE_PAGE_SIZE)
		}

		// 验证所有键都被保留
		totalKeys := left.nkeys() + right.nkeys()
		if totalKeys != old.nkeys() {
			t.Errorf("键总数不匹配: 期望 %d, 得到 %d", old.nkeys(), totalKeys)
		}
	})

	// 测试用例5：内部节点分裂
	t.Run("内部节点分裂", func(t *testing.T) {
		old := BNode(make([]byte, 200))
		old.setHeader(BNODE_NODE, 4)
		nodeAppendKV(old, 0, 100, []byte("key1"), nil) // 内部节点val为nil
		nodeAppendKV(old, 1, 200, []byte("key2"), nil)
		nodeAppendKV(old, 2, 300, []byte("key3"), nil)
		nodeAppendKV(old, 3, 400, []byte("key4"), nil)

		left := BNode(make([]byte, BTREE_PAGE_SIZE))
		right := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeSplit2(left, right, old)

		// 验证节点类型保持不变
		if left.btype() != BNODE_NODE || right.btype() != BNODE_NODE {
			t.Error("节点类型改变")
		}

		// 验证指针被正确复制
		if left.getPtr(0) != 100 || right.getPtr(0) != 300 {
			t.Error("指针复制错误")
		}
	})
}

func TestNodeSplit3(t *testing.T) {
	// 测试用例1：小节点不需要分裂
	t.Run("小节点不分裂", func(t *testing.T) {
		old := BNode(make([]byte, BTREE_PAGE_SIZE)) // 创建小节点
		old.setHeader(BNODE_LEAF, 1)
		nodeAppendKV(old, 0, 0, []byte("key"), []byte("val"))

		count, nodes := nodeSplit3(old)
		if count != 1 {
			t.Errorf("期望不分裂(1个节点), 得到 %d 个节点", count)
		}
		if nodes[0].nkeys() != 1 {
			t.Errorf("节点键数量错误: 期望 1, 得到 %d", nodes[0].nkeys())
		}
	})
	// 测试用例2：中等大小节点分裂为2个
	t.Run("分裂为2个节点", func(t *testing.T) {
		old := BNode(make([]byte, BTREE_PAGE_SIZE*1.5)) // 创建中等节点
		old.setHeader(BNODE_LEAF, 10)
		//nbytes 大于 BTREE_PAGE_SIZE, 但小于 1.5*BTREE_PAGE_SIZE
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%02d", i)
			val := strings.Repeat("x", 500) // 中等大小值
			nodeAppendKV(old, uint16(i), 0, []byte(key), []byte(val))
		}

		count, nodes := nodeSplit3(old)
		if count != 2 {
			t.Errorf("期望分裂为2个节点, 得到 %d 个节点", count)
		}
		// 验证总键数不变
		totalKeys := nodes[0].nkeys() + nodes[1].nkeys()
		if totalKeys != old.nkeys() {
			t.Errorf("键总数不匹配: 期望 %d, 得到 %d", old.nkeys(), totalKeys)
		}
		// 验证节点大小不超过页大小
		if nodes[0].nbytes() > BTREE_PAGE_SIZE || nodes[1].nbytes() > BTREE_PAGE_SIZE {
			t.Error("分裂后节点大小超过页限制")
		}
	})

	// 测试用例3：大节点分裂为3个
	t.Run("分裂为3个节点", func(t *testing.T) {
		old := BNode(make([]byte, BTREE_PAGE_SIZE*2.5)) // 创建大节点
		old.setHeader(BNODE_LEAF, 18)
		for i := 0; i < 18; i++ {
			key := fmt.Sprintf("key%02d", i)
			val := strings.Repeat("y", 500) // 较大值
			nodeAppendKV(old, uint16(i), 0, []byte(key), []byte(val))
		}
		fmt.Printf("old nbytes: %d\n", old.nbytes())

		count, nodes := nodeSplit3(old)
		if count != 3 {
			t.Errorf("期望分裂为3个节点, 得到 %d 个节点", count)
		}
		// 验证总键数
		totalKeys := nodes[0].nkeys() + nodes[1].nkeys() + nodes[2].nkeys()
		if totalKeys != old.nkeys() {
			t.Errorf("键总数不匹配: 期望 %d, 得到 %d", old.nkeys(), totalKeys)
		}
		// 验证所有节点大小
		for i := 0; i < int(count); i++ {
			if nodes[i].nbytes() > BTREE_PAGE_SIZE {
				t.Errorf("节点 %d 大小 %d 超过页限制 %d", i, nodes[i].nbytes(), BTREE_PAGE_SIZE)
			}
		}
	})
}
