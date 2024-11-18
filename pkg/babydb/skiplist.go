package babydb

import (
	"math/rand"
)

const sklMaxHeight = 12
const sklCapacity = 1 << 16

type node struct {
	row   Row
	stack []*node
}

type SkiplistIterator struct {
	sk   *Skiplist
	node *node
}

func (it *SkiplistIterator) Next() (Row, bool) {
	if it.node == nil {
		return Row{}, false
	}
	row := it.node.row
	it.node = it.node.stack[0]
	return row, true
}

type Skiplist struct {
	heap []byte
	ptrs []*node
	// TODO: this should be atomic?
	heapPtr int
	nodes   []node
}

func (sk *Skiplist) Iter() *SkiplistIterator {
	return &SkiplistIterator{
		node: sk.nodes[0].stack[0],
	}
}

func (sk *Skiplist) Insert(key []byte, value []byte, version uint64) bool {
	if sk.heapPtr+len(key)+len(value) > len(sk.heap) {
		// Does not fit, this Skiplist should be kicked out and a new one should be allocated.
		return false
	}
	keyLoc := sk.heapPtr
	valueLoc := sk.heapPtr + len(key)
	copy(sk.heap[keyLoc:], key)
	copy(sk.heap[valueLoc:], value)
	sk.heapPtr += len(key) + len(value)

	height := 1
	for rand.Intn(2) == 0 && height < sklMaxHeight {
		height++
	}

	if len(sk.nodes) == cap(sk.nodes) {
		// No more space for nodes, this Skiplist should be kicked out and a new one should be allocated.
		return false
	}

	if len(sk.ptrs)+height > cap(sk.ptrs) {
		// No more space for pointers, this Skiplist should be kicked out and a new one should be allocated.
		return false
	}

	ptrsLoc := len(sk.ptrs)
	sk.ptrs = append(sk.ptrs, make([]*node, height)...)

	row := Row{
		Key:     sk.heap[keyLoc : keyLoc+len(key)],
		Value:   sk.heap[valueLoc : valueLoc+len(value)],
		Version: version,
	}

	sk.nodes = append(sk.nodes, node{
		row:   row,
		stack: sk.ptrs[ptrsLoc : ptrsLoc+height],
	})

	for i := 0; i < height; i++ {
		// Find the place to insert the new node
		prev := &sk.nodes[0]
		current := prev.stack[i]
		for current != nil && current.row.lt(row) {
			prev = current
			current = current.stack[i]
		}
		// First, make the new node point to `current`
		sk.nodes[len(sk.nodes)-1].stack[i] = current
		// Then, make `prev` point to the new node
		prev.stack[i] = &sk.nodes[len(sk.nodes)-1]
	}

	// node := &sk.nodes[len(sk.nodes)-1]

	return true
}

func NewSkiplist() *Skiplist {
	head := node{
		// TODO: should this be in the heap?
		stack: make([]*node, sklMaxHeight),
	}

	nodes := make([]node, 0, sklCapacity)
	nodes = append(nodes, head)

	return &Skiplist{
		heap:  make([]byte, sklCapacity),
		nodes: nodes,
		ptrs:  make([]*node, 0, 1<<10),
	}
}
