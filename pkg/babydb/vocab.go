package babydb

import (
	"bytes"
	"fmt"
)

type Row struct {
	Key     []byte
	Value   []byte
	Version uint64
}

func (r *Row) lt(other Row) bool {
	cmp := bytes.Compare(r.Key, other.Key)
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	if r.Version < other.Version {
		return true
	}
	return false
}

func (r Row) String() string {
	return fmt.Sprintf("%s=%s@%d", string(r.Key), string(r.Value), r.Version)
}
