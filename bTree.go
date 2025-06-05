package main

import (
	"bytes"
)

type BTree struct {
	root uint64
	get  func(uint64) []byte // read a page
	new  func([]byte) uint64 //append a page
	del  func(uint64)        // ignored in this chapter
}

// 如果树为空，则创立根节点
// 如果根节点分裂，则创建新根
func (tree *BTree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		// 这个根节点需要分离，添加新的一层
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

func (tree *BTree) Delete(key []byte) (bool, error) {
	panic("not implementation")
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	//额外的尺寸允许其暂时超过1页。
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	idx := nodeLookupLE(node, key) //寻找索引
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdata(new, node, idx, key, val) //发现，更新它
		} else {
			leafInsert(new, node, idx+1, key, val) //没有发现，插入
		}
	case BNODE_NODE:
		//internal node,插入子节点
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return new
}

// 将一个链接替换为多个链接
// 对于内部节点，链接到子节点的链接始终使用写时复制方案进行更新，如果子节点被拆分，则可以成为多个链接
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	panic("not implementation")
}

// 删除一个key从一个internal node；treeDelete的一部分
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	//递归去子节点
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{} // 没有发现
	}
	tree.del(kptr)
	//检查合并
	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	mergeDir, sibing := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: //left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibing, updated)
		tree.del(node.getPtr((idx - 1)))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: //right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibing)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		//assert(node.nkeys()==1 && idx ==0) //1 空
		new.setHeader(BNODE_NODE, 0)
	case mergeDir == 0 && updated.nkeys() > 0:
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// treeInsert()的一部分，KV 插入对于internal 节点
func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	kptr := node.getPtr(idx)
	//递归插入子节点
	knode := treeInsert(tree, tree.get(kptr), key, val)
	//分离结果
	nsplit, split := nodeSplit3(knode)
	//释放子节点
	tree.del(kptr)
	//更新子的连接
	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}

// 更新后的子节点是否应该与兄弟节点合并？
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling //左
		}
	}
	if idx+1 < node.nkeys() {
		sibling := BNode(tree.get(node.getPtr(idx + 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling //右
		}
	}
	return 0, BNode{}
}

func treeSearch(tree *BTree, ptr uint64, key []byte) ([]byte, bool) {
	node := BNode(tree.get(ptr))
	switch node.btype() {
	case BNODE_LEAF:
		idx := nodeLookupLE(node, key)
		if bytes.Equal(node.getKey(idx), key) {
			return node.getVal(idx), true
		}
	case BNODE_NODE:
		idx := nodeLookupLE(node, key)
		return treeSearch(tree, node.getPtr(idx), key)
	default:
		panic("not imp")
	}
	return nil, false
}
