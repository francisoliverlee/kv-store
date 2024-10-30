package kvstore

import (
	"bytes"
	"time"
)

var (
	Split       = []byte("@")
	SplitLength = len(Split)
)

// BuildKey a new key with bucket and keys
func BuildKey(totalSize int, bb ...[]byte) []byte {
	if totalSize == 0 {
		return []byte{}
	}
	var newBB [][]byte
	for _, b := range bb {
		if b == nil || len(b) == 0 {
			continue
		}
		newBB = append(newBB, b)
	}

	bbLen := len(newBB)
	result := make([]byte, totalSize+SplitLength*(bbLen-1))
	bc := 0

	for i, b := range newBB {
		if b == nil || len(b) == 0 {
			continue
		}
		copy(result[bc:], b)
		bc = bc + len(b)

		if (i + 1) < bbLen {
			copy(result[bc:], Split)
			bc = bc + SplitLength
		}
	}
	return result
}

func Now() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

// RemovePrefix remove prefix and @kvstore.Split in byte array
func RemovePrefix(s []byte, prefix []byte) []byte {
	if bytes.HasPrefix(s, prefix) {
		return append([]byte{}, s[len(prefix)+1:]...) // s is a pointer that could be changed outside, we copy its value
	}
	return s
}
