package babydb

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const sklMaxHeight = 12
const sklCapacity = 1 << 32

type node struct {
	row   Row
	stack []unsafe.Pointer
}

type SkiplistIterator struct {
	sk   *Skiplist
	node unsafe.Pointer
}

func (it *SkiplistIterator) Seek(key []byte) {
	// We'll position at the first row with the given key.
	rowKey := Row{
		Key:     key,
		Version: 0,
	}

	pred := &it.sk.nodes[0]
	succ := (*node)(atomic.LoadPointer(&pred.stack[sklMaxHeight-1]))

	for i := sklMaxHeight - 1; i >= 0; i-- {
		for succ != nil && succ.row.lt(rowKey) {
			pred = succ
			succ = (*node)(atomic.LoadPointer(&pred.stack[i]))
		}
		if i > 0 {
			succ = (*node)(atomic.LoadPointer(&pred.stack[i-1]))
		}
	}
	it.node = unsafe.Pointer(pred)
	it.Next()
}

func (it *SkiplistIterator) Next() (Row, bool) {
	if it.node == nil {
		return Row{}, false
	}
	node := (*node)(atomic.LoadPointer(&it.node))
	row := node.row
	it.node = node.stack[0]
	return row, true
}

type Skiplist struct {
	heap    []byte
	heapPtr atomic.Int64

	// These are pointers to *node
	ptrs    []unsafe.Pointer
	ptrsPtr atomic.Int64

	nodes    []node
	nodesPtr atomic.Int64
}

func (sk *Skiplist) Iter() *SkiplistIterator {
	return &SkiplistIterator{
		sk:   sk,
		node: sk.nodes[0].stack[0],
	}
}

func (sk *Skiplist) Find(key []byte) ([]byte, bool) {
	iter := sk.Iter()
	iter.Seek(key)
	row, ok := iter.Next()
	if !ok {
		return nil, false
	}
	if bytes.Equal(row.Key, key) {
		return row.Value, true
	}
	return nil, false
}

func (sk *Skiplist) Insert(key []byte, value []byte, version uint64) bool {
	bump := len(key) + len(value)
	bumped := sk.heapPtr.Add(int64(bump))
	if bumped > sklCapacity {
		// Does not fit, this Skiplist should be kicked out and a new one should be allocated.
		return false
	}

	// Now we own the space, we can write the key and value.
	keyLoc := bumped - int64(len(key)) - int64(len(value))
	valueLoc := bumped - int64(len(value))
	copy(sk.heap[keyLoc:], key)
	copy(sk.heap[valueLoc:], value)

	// height := 1 + bits.LeadingZeros64(uint64(rand.Uint64()))
	height := 1
	for rand.Float64() < P && height < sklMaxHeight {
		height += 1
	}

	bumpedNodesPtr := int(sk.nodesPtr.Add(1))

	if bumpedNodesPtr >= len(sk.nodes) {
		// No more space for nodes, this Skiplist should be kicked out and a new one should be allocated.
		return false
	}

	bumpedPtrsPtr := int(sk.ptrsPtr.Add(int64(height)))

	if bumpedPtrsPtr > len(sk.ptrs) {
		// No more space for pointers, this Skiplist should be kicked out and a new one should be allocated.
		return false
	}

	row := Row{
		Key:     sk.heap[keyLoc : int(keyLoc)+len(key)],
		Value:   sk.heap[valueLoc : int(valueLoc)+len(value)],
		Version: version,
	}

	ptrsLoc := bumpedPtrsPtr - height

	sk.nodes[bumpedNodesPtr] = node{
		row:   row,
		stack: sk.ptrs[ptrsLoc : ptrsLoc+height],
	}

	newNodePtr := unsafe.Pointer(&sk.nodes[bumpedNodesPtr])
	newNode := (*node)(newNodePtr)

	preds, succs := sk.findTowers(row.Key, row.Version)

	for i := 0; i < height; i++ {
		for {
			// predPtr := unsafe.Pointer(&preds[i])
			pred := (*node)(preds[i])

			succPtr := atomic.LoadPointer(&succs[i])
			// succ := (*node)(succPtr)

			newNode.stack[i] = succPtr

			// Then, make `prev` point to the new node
			if atomic.CompareAndSwapPointer(&pred.stack[i], succPtr, newNodePtr) {
				break
			}

			// We failed, so we need to recompute the towers.
			preds, succs = sk.findTowers(row.Key, row.Version)
			// Keep trying until we succeed
		}
	}

	return true
}

// findTowers ...
func (sk *Skiplist) findTowers(key []byte, version uint64) ([]unsafe.Pointer, []unsafe.Pointer) {
	rowKey := Row{
		Key:     key,
		Version: version,
	}
	// TODO: we should not need to allocate these slices.
	preds := make([]unsafe.Pointer, sklMaxHeight)
	succs := make([]unsafe.Pointer, sklMaxHeight)

	pred := &sk.nodes[0]
	succ := (*node)(atomic.LoadPointer(&pred.stack[sklMaxHeight-1]))
	for i := sklMaxHeight - 1; i >= 0; i-- {
		for succ != nil && succ.row.lt(rowKey) {
			pred = succ
			succ = (*node)(atomic.LoadPointer(&pred.stack[i]))
		}
		preds[i] = unsafe.Pointer(pred)
		succs[i] = unsafe.Pointer(succ)
		if i > 0 {
			succ = (*node)(atomic.LoadPointer(&pred.stack[i-1]))
		}
	}

	return preds, succs
}

var P = 0.5

func NewSkiplist() *Skiplist {
	head := node{
		// TODO: should this be in the heap?
		stack: make([]unsafe.Pointer, sklMaxHeight),
	}

	// how to size this?
	nodes := make([]node, 1<<16)
	nodes[0] = head

	sk := &Skiplist{
		heap:     make([]byte, sklCapacity),
		nodes:    nodes,
		nodesPtr: atomic.Int64{},
		ptrs:     make([]unsafe.Pointer, 1<<16),
		ptrsPtr:  atomic.Int64{},
	}
	sk.nodesPtr.Store(1)
	sk.ptrsPtr.Store(0)

	return sk
}
