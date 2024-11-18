package main

import (
	"fmt"

	"github.com/justinj/babydb2/pkg/babydb"
)

func main() {
	it := babydb.NewSliceIterator(
		[]babydb.Row{
			{Key: []byte("a"), Value: []byte("1"), Version: 1},
			{Key: []byte("a"), Value: []byte("2"), Version: 2},
			{Key: []byte("b"), Value: []byte("3"), Version: 1},
			{Key: []byte("b"), Value: []byte("4"), Version: 2},
		},
	)
	it2 := babydb.NewSliceIterator(
		[]babydb.Row{
			{Key: []byte("a"), Value: []byte("5"), Version: 3},
			{Key: []byte("a"), Value: []byte("6"), Version: 4},
			{Key: []byte("b"), Value: []byte("7"), Version: 3},
			{Key: []byte("b"), Value: []byte("8"), Version: 4},
		},
	)
	mit := babydb.NewMergedIterator(it, it2)

	for row, ok := mit.Next(); ok; row, ok = mit.Next() {
		fmt.Println(row)
	}
}
