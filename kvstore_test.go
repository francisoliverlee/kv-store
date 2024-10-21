package kvstore

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	TestBucket = []byte("test_bucket")
)

func Test_badgerStore_SetAndDelete(t *testing.T) {
	var dir = os.TempDir()
	s, err := NewBadgerStore(badger.DefaultOptions(dir))
	defer func() {
		_ = os.Remove(dir)
	}()

	assert.True(t, err == nil)

	key := "aaaaaa"
	value := "bbbbbb"
	assert.True(t, s.Set(TestBucket, []byte(key), []byte(value)) == nil)
	assert.True(t, s.Delete(TestBucket, []byte(key)) == nil)

	a, b, c := s.Get(TestBucket, []byte(key))

	assert.True(t, a == nil || len(a) == 0)
	assert.True(t, b == false)
	assert.True(t, c != nil)
}
