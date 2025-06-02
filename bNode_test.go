package main

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBNode(t *testing.T) {
	nodeData := make([]byte, 43)

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

	// 验证指针（叶节点应为0）
	if ptr0 := node.getPtr(0); ptr0 != 0 {
		t.Errorf("指针0错误: 期望 0, 得到 %d", ptr0)
	}
	if ptr1 := node.getPtr(1); ptr1 != 0 {
		t.Errorf("指针1错误: 期望 0, 得到 %d", ptr1)
	}

	// 验证偏移量
	if offset0 := node.getOffset(0); offset0 != 0 {
		t.Errorf("偏移量0错误: 期望 0, 得到 %d", offset0)
	}
	if offset1 := node.getOffset(1); offset1 != 8 {
		t.Errorf("偏移量1错误: 期望 8, 得到 %d", offset1)
	}

	// 验证键值对
	key0 := node.getKey(0)
	if !bytes.Equal(key0, []byte("k1")) {
		t.Errorf("key0错误: 期望 'k1', 得到 '%s'", key0)
	}

	val0 := node.getVal(0)
	if !bytes.Equal(val0, []byte("hi")) {
		t.Errorf("val0错误: 期望 'hi', 得到 '%s'", val0)
	}

	key1 := node.getKey(1)
	if !bytes.Equal(key1, []byte("k3")) {
		t.Errorf("key1错误: 期望 'k3', 得到 '%s'", key1)
	}

	val1 := node.getVal(1)
	if !bytes.Equal(val1, []byte("hello")) {
		t.Errorf("val1错误: 期望 'hello', 得到 '%s'", val1)
	}

}
