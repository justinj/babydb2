package babydb

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkiplist(t *testing.T) {
	sk := NewSkiplist()

	threads := 8
	batchSize := 1000

	var data []Row
	for i := 0; i < batchSize*threads; i++ {
		data = append(data, Row{Key: []byte(fmt.Sprintf("foo%d", i)), Value: []byte(fmt.Sprintf("bar%d", i)), Version: 1})
	}
	rand.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})

	var tooBig atomic.Bool

	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		data := data[i*batchSize : (i+1)*batchSize]
		go func() {
			for _, row := range data {
				if !sk.Insert(row.Key, row.Value, row.Version) {
					tooBig.Store(true)
					break
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()

	expected := data
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].lt(expected[j])
	})

	actual := []Row{}

	it := sk.Iter()
	for row, ok := it.Next(); ok; row, ok = it.Next() {
		actual = append(actual, row)
	}

	if tooBig.Load() {
		t.Fatal("too many elements")
	}
	assert.ElementsMatch(t, expected, actual)

	// Test Find.

	for _, row := range data {
		v, ok := sk.Find(row.Key)
		assert.True(t, ok)
		assert.Equal(t, row.Value, v)
	}

}

func TestSkiplistSize(t *testing.T) {
	rows := make([]Row, 1000)
	for i := range rows {
		rows[i] = Row{Key: []byte(fmt.Sprintf("foo%d", i)), Value: []byte(fmt.Sprintf("bar%d", i)), Version: 1}
	}

	P = 0.5
	sk := NewSkiplist()
	for _, row := range rows {
		sk.Insert(row.Key, row.Value, row.Version)
	}
}
