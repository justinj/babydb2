package babydb

import (
	"bytes"
	"sort"
)

type Iterator interface {
	Next() (Row, bool)
	Seek(key []byte)
}

type SliceIterator struct {
	rows []Row
	idx  int
}

func NewSliceIterator(rows []Row) *SliceIterator {
	return &SliceIterator{rows: rows}
}

func (it *SliceIterator) Next() (Row, bool) {
	if it.idx >= len(it.rows) {
		return Row{}, false
	}
	nextRow := it.rows[it.idx]
	it.idx++
	return nextRow, true
}

func (it *SliceIterator) Seek(key []byte) {
	it.idx = sort.Search(len(it.rows), func(i int) bool {
		return bytes.Compare(it.rows[i].Key, key) >= 0
	})
}

// Invariant: the iterator is always at the first row with a key >= currentKey
type VersionedIterator struct {
	it         Iterator
	version    uint64
	more       bool
	currentRow Row
}

func NewVersionedIterator(it Iterator, version uint64) *VersionedIterator {
	first, ok := it.Next()
	if !ok {
		return &VersionedIterator{
			it:         it,
			version:    version,
			currentRow: Row{},
			more:       false,
		}
	}
	return &VersionedIterator{
		it:         it,
		version:    version,
		currentRow: first,
		more:       true,
	}
}

func (it *VersionedIterator) Next() (Row, bool) {
	if !it.more {
		return Row{}, false
	}
	nextRow, ok := it.it.Next()
	if !ok {
		it.more = false
		return it.currentRow, true
	}
	result := it.currentRow
	// We want the latest version of the row that is <= it.version
	for bytes.Equal(nextRow.Key, it.currentRow.Key) {
		if nextRow.Version <= it.version {
			result = nextRow
		}
		nextRow, ok = it.it.Next()
		if !ok {
			it.more = false
			return result, true
		}
	}
	// TODO: we need to clone this.
	it.currentRow = nextRow
	return result, true
}

func (it *VersionedIterator) Seek(key []byte) {
	it.it.Seek(key)
	it.currentRow, it.more = it.it.Next()
}

type PeekableIterator struct {
	it     Iterator
	peek   Row
	peeked bool
}

func NewPeekableIterator(it Iterator) *PeekableIterator {
	return &PeekableIterator{it: it, peeked: false}
}

func (it *PeekableIterator) Next() (Row, bool) {
	if it.peeked {
		it.peeked = false
		return it.peek, true
	}
	return it.it.Next()
}

func (it *PeekableIterator) Seek(key []byte) {
	it.it.Seek(key)
	it.peeked = false
}

func (it *PeekableIterator) Peek() (Row, bool) {
	if it.peeked {
		return it.peek, true
	}
	it.peek, it.peeked = it.it.Next()
	return it.peek, it.peeked
}

// MergedIterator merges multiple iterators into a single iterator.
type MergedIterator struct {
	// This is a min-heap of PeekableIterators.
	iters []*PeekableIterator
}

func (m *MergedIterator) Next() (Row, bool) {
	if len(m.iters) == 0 {
		return Row{}, false
	}
	result, ok := m.iters[0].Next()
	if !ok {
		return Row{}, false
	} else {
		m.down(0)
	}
	return result, true
}

func (m *MergedIterator) heapify() {
	for i := len(m.iters)/2 - 1; i >= 0; i-- {
		m.down(i)
	}
}

func (m *MergedIterator) Seek(key []byte) {
	for _, it := range m.iters {
		it.Seek(key)
	}
	m.heapify()
}

func NewMergedIterator(iters ...Iterator) *MergedIterator {
	peekableIters := make([]*PeekableIterator, len(iters))
	for i, it := range iters {
		peekableIters[i] = NewPeekableIterator(it)
	}
	m := &MergedIterator{iters: peekableIters}
	m.heapify()
	return m
}

func (m *MergedIterator) down(i int) {
	cur, ok := m.iters[i].Peek()
	// Check the left child.
	left := 2*i + 1
	if left >= len(m.iters) {
		return
	}
	lhs, lok := m.iters[left].Peek()
	if !ok || lok && !cur.lt(lhs) {
		// Swap with the left child.
		m.iters[i], m.iters[left] = m.iters[left], m.iters[i]
		m.down(left)
		return
	}
	// Check the right child.
	right := 2*i + 2
	if right >= len(m.iters) {
		return
	}
	rhs, rok := m.iters[right].Peek()
	if !ok || rok && !cur.lt(rhs) {
		// Swap with the right child.
		m.iters[i], m.iters[right] = m.iters[right], m.iters[i]
		m.down(right)
		return
	}
}
