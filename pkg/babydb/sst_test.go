package babydb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSstBuild(t *testing.T) {
	// Create a new SstBuilder
	// Add a row to the builder
	// Write the block to a buffer
	// Verify the buffer contains the expected data

	var buf bytes.Buffer

	data := []Row{}
	for i := 0; i < 100; i++ {
		data = append(data, Row{Key: []byte(fmt.Sprintf("foo%d", i)), Value: []byte(fmt.Sprintf("bar%d", i)), Version: 1})
	}

	sb := NewSstBuilder(&buf)

	for i := 0; i < 100; i++ {
		if err := sb.Add(data[i]); err != nil {
			t.Fatal(err)
		}
	}

	if err := sb.Finish(); err != nil {
		t.Fatal(err)
	}

	reader := NewSstReader(&buf)

	result := []Row{}

	for r, ok, err := reader.Read(); ok || err != nil; r, ok, err = reader.Read() {
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, r)
	}

	assert.ElementsMatch(t, data, result)
}
