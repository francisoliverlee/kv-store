package kvstore

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// test concat bucket and key
func TestBuildKey1(t *testing.T) {
	bucket := []byte("hello")
	key := []byte("gmq")

	newKey := []byte("hello@gmq")

	r := BuildKey(len(bucket)+len(key), bucket, key)
	for i := 0; i < len(newKey); i++ {
		assert.True(t, newKey[i] == r[i])
	}
}

// test concat bucket (empty) and key
func TestBuildKey2(t *testing.T) {
	bucket := []byte("")
	key := []byte("gmq")
	newKey := []byte("@gmq")

	r := BuildKey(len(bucket)+len(key), bucket, key)
	for i := 0; i < len(newKey); i++ {
		assert.True(t, newKey[i] == r[i])
	}
}

// test concat bucket and key(empty)
func TestBuildKey3(t *testing.T) {
	bucket := []byte("hello")
	key := []byte("")

	newKey := []byte("hello@")

	r := BuildKey(len(bucket)+len(key), bucket, key)
	for i := 0; i < len(newKey); i++ {
		assert.True(t, newKey[i] == r[i])
	}
}

// test concat bucket(empty) and key(empty)
func TestBuildKey4(t *testing.T) {
	bucket := []byte("")
	key := []byte("")

	newKey := []byte("@")

	r := BuildKey(len(bucket)+len(key), bucket, key)
	for i := 0; i < len(newKey); i++ {
		assert.True(t, newKey[i] == r[i])
	}
}
