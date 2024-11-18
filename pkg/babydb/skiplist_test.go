package babydb

import (
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestSkiplist(t *testing.T) {
	sk := NewSkiplist()

	for i := 0; i < 3; i++ {
		if !sk.Insert([]byte(fmt.Sprintf("foo%d", i)), []byte(fmt.Sprintf("bar%d", i)), 1) {
			t.Fatal("insert failed")
		}
	}

	n := 100
	it := sk.Iter()
	for row, ok := it.Next(); ok; row, ok = it.Next() {
		spew.Dump(row)
		n--
		if n == 0 {
			break
		}

	}
}
