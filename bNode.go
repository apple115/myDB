package main

import (
	"bytes"
	"encoding/binary"
)

// | type | nkeys |  pointers  |  offsets   | key-values | unused |
// |  2B  |   2B  | nkeys × 8B | nkeys × 2B |     ...    |        |
// type 是节点的类型(leaf 或 internal ), nkeys 是键的数量

// 例如，叶节点 {“k1”：“hi”， “k3”：“hello”} 编码为：

// | type | nkeys | pointers | offsets |            key-values           | unused |
// |   2  |   2   | nil nil  |  8 19   | 2 2 "k1" "hi"  2 5 "k3" "hello" |        |
// |  2B  |  2B   |   2×8B   |  2×2B   | 4B + 2B + 2B + 4B + 2B + 5B     |        |

// 一个B树的节点
type BNode []byte

const (
	BNODE_NODE = 1 // internal nodes with pointers
	BNODE_LEAF = 2 // leaf nodes with values
)

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

// func init() {
// 	node1max := 4 + 1*8 + 1*2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
// 	// assert(node1max <= BTREE_PAGE_SIZE) // maximum KV
// }

// 获得节点的类型
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

// 获取节点的类型
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// 设置节点的类型和键的数量,设置当前的header
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// idx 是键的索引
// 得到索引之后的键
// 读取和写入指针数组（用于内部节点）
func (node BNode) getPtr(idx uint16) uint64 {
	// assert(idx < node.nkeys())
	pos := 4 + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

// 设置索引之后的键
func (node BNode) setPtr(idx uint16, val uint64) {
	// assert(idx < node.nkeys())
	pos := 4 + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// 得到偏移量
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

// 更改偏移量
func (node BNode) setOffset(idx uint16, val uint16) {
	if idx == 0 {
		return
	}
	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	//取两个字节的偏移量
	binary.LittleEndian.PutUint16(node[pos:], val)
}

// 得到索引之后的键值对位置
func (node BNode) kvPos(idx uint16) uint16 {
	//assert(idx <= node.nkeys())
	return 4 + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// 以 slice 形式获取第 n 个 key 数据。
func (node BNode) getKey(idx uint16) []byte {
	//assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

// 以 slice 形式获取第 n 个 key-value 数据。
func (node BNode) getVal(idx uint16) []byte {
	// assert(idx < node.nkeys()
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

// node的大小
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// node添加键值对
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx) // uses the offset value of the previous key
	// 4-bytes KV sizes
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	// KV data
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	// update the offset value for the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// 将旧节点的键值对复制到新节点中
// dstNew 是新节点的索引位置
// srcOld 是旧节点的索引位置
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}

}

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)                   //添加idx的键值对之前的所有键值对 [0,idx)
	nodeAppendKV(new, idx, 0, key, val)                    //将新键值对添加到新节点中，new
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) //添加idx之后的所有键值对 [idx,nkeys)
}

func leafUpdata(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}

// nodeLookupLE 查找小于等于给定键的最大索引
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	var i uint16
	for i = 0; i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i
		}
		if cmp > 0 {
			return i - 1
		}
	}
	return i - 1
}

// 将节点分裂为两个节点
func nodeSplit2(left BNode, right BNode, old BNode) {
	nleft := old.nkeys() / 2

	left_bytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}

	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}

	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + 4
	}

	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}

	nright := old.nkeys() - nleft
	//new nodes
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
}

// nodeSplit3 将节点分裂为三个节点，如果节点很大
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) //可能还要分一次
	right := BNode(make([]byte, BTREE_PAGE_SIZE))

	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left := left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle,left)
	// assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}
