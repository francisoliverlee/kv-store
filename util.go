package kvstore

import (
	"time"
)

func AppendBytes(totalSize int, orderBytes ...[]byte) []byte {
	result := make([]byte, totalSize)
	bc := 0
	for _, b := range orderBytes {
		copy(result[bc:], b)
		bc = bc + len(b)
	}
	return result
}

func Now() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
