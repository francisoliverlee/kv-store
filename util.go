package kvstore

func AppendBytes(totalSize int, orderBytes ...[]byte) []byte {
	result := make([]byte, totalSize)
	bc := 0
	for _, b := range orderBytes {
		copy(result[bc:], b)
		bc = bc + len(b)
	}
	return result
}
