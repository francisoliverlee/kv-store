package kvstore

import (
	"bytes"
	"time"
)

var (
	Split       = []byte("@")
	SplitLength = len(Split)
)

// build a new key with bucket and keys
func buildKey(totalSize int, bb ...[]byte) []byte {
	result := make([]byte, totalSize+SplitLength*(len(bb)-1))
	bc := 0
	for _, b := range bb {
		copy(result[bc:], b)
		bc = bc + len(b)
		copy(result[bc:], Split)
		bc = bc + SplitLength
	}
	return result
}

func Now() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

// RemovePrefix remove prefix in byte array
func RemovePrefix(s []byte, prefix []byte) []byte {
	if bytes.HasPrefix(s, prefix) {
		return append([]byte{}, s[len(prefix):]...) // s is a pointer that could be changed outside, we copy its value
	}
	return s
}
