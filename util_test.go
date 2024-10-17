package kvstore

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAppendBytes(t *testing.T) {
	arr1 := []byte{1, 2, 3}
	arr2 := []byte{4, 5, 6}

	arr3 := []byte{1, 2, 3, 4, 5, 6}

	r := AppendBytes(len(arr1)+len(arr2), arr1, arr2)

	for i := 0; i < len(arr3); i++ {
		assert.True(t, arr3[i] == r[i])
	}
}

func TestAppendBytes2(t *testing.T) {
	arr1 := []byte{1, 2, 3, 67, 78}
	arr2 := []byte{4, 5, 6}

	arr3 := []byte{1, 2, 3, 67, 78, 4, 5, 6}

	r := AppendBytes(len(arr1)+len(arr2), arr1, arr2)

	for i := 0; i < len(arr3); i++ {
		assert.True(t, arr3[i] == r[i])
	}
}

func TestAppendBytes3(t *testing.T) {
	arr1 := []byte{}
	arr2 := []byte{4, 5, 6}

	arr3 := []byte{4, 5, 6}

	r := AppendBytes(len(arr1)+len(arr2), arr1, arr2)

	for i := 0; i < len(arr3); i++ {
		assert.True(t, arr3[i] == r[i])
	}
}
