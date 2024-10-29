package kvstore

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
)

var (
	TestBucket = []byte("test_bucket")
	counter    = int64(0)
)

func getDataPath() string {
	dir := fmt.Sprintf("%skvstore-%d", os.TempDir(), atomic.AddInt64(&counter, 1))
	os.RemoveAll(dir)

	return dir
}

// test set get
func Test_badgerStore_SetAndeGet(t *testing.T) {
	var dir = getDataPath()
	t.Logf("data path %s", dir)

	// new store
	s, err := NewBadgerStore(badger.DefaultOptions(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.True(t, err == nil)

	key := "tiger"
	value := "i-like-kv"

	// set key value
	assert.True(t, s.Set(TestBucket, []byte(key), []byte(value)) == nil)

	// get key value
	if val, f, err := s.Get(TestBucket, []byte(key)); err != nil {
		t.Fatalf("get error. bucket=%s, key=%s, %v", TestBucket, key, err)
	} else if !f {
		t.Fatalf("key not exist, should be there. bucket=%s, key=%s", TestBucket, key)
	} else if string(val) != value {
		t.Fatalf("value should be same but not bucket=%s, key=%s, set-value=%s, get-value=%s", TestBucket, key, value, string(val))
	}
}

// test p set, p get, get single
func Test_badgerStore_PSetAndePGet(t *testing.T) {
	var dir = getDataPath()
	t.Logf("data path %s", dir)

	// new store
	s, err := NewBadgerStore(badger.DefaultOptions(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.True(t, err == nil)

	// make 100 key value pair
	var keys = [][]byte{}
	var values = [][]byte{}
	key := "tiger-"
	value := "i-like-kv-"
	for i := 0; i < 100; i++ {
		keys = append(keys, []byte(key+strconv.Itoa(i)))
		values = append(values, []byte(value+strconv.Itoa(i)))
	}

	// set key value
	assert.True(t, s.PSet(TestBucket, keys, values) == nil)

	// check get 100 keys
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		if val, f, err := s.Get(TestBucket, []byte(key)); err != nil {
			t.Fatalf("get error. bucket=%s, key=%s, %v", TestBucket, key, err)
		} else if !f {
			t.Fatalf("key not exist, should be there. bucket=%s, key=%s", TestBucket, key)
		} else if string(val) != string(values[i]) {
			t.Fatalf("value should be same but not bucket=%s, key=%s, set-value=%s, get-value=%s", TestBucket, key, values[i], string(val))
		}
	}

	// check pget 100 keys
	if val, err := s.PGet(TestBucket, keys); err != nil {
		t.Fatalf("get error. bucket=%s, key=%s, %v", TestBucket, key, err)
	} else if len(val) != len(keys) {
		t.Fatalf("keys and values length not the same. keys=%d, values=%d", len(keys), len(val))
	} else {
		for i := 0; i < len(val); i++ {
			if string(val[i]) != string(values[i]) {
				t.Fatalf("value not same. index=%d, key=%s", i, string(keys[i]))
			}
		}
	}

}

// test set delete get
func Test_badgerStore_SetAndDelete(t *testing.T) {
	var dir = getDataPath()
	t.Logf("data path %s", dir)
	s, err := NewBadgerStore(badger.DefaultOptions(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.True(t, err == nil)

	key := "tiger"
	value := "i-like-kv"
	assert.True(t, s.Set(TestBucket, []byte(key), []byte(value)) == nil)

	if val, f, err := s.Get(TestBucket, []byte(key)); err != nil {
		t.Fatalf("get error. bucket=%s, key=%s, %v", TestBucket, key, err)
	} else if !f {
		t.Fatalf("key not exist, should be there. bucket=%s, key=%s", TestBucket, key)
	} else if string(val) != value {
		t.Fatalf("value should be same but not bucket=%s, key=%s, set-value=%s, get-value=%s", TestBucket, key, value, string(val))
	}

	if err := s.Delete(TestBucket, []byte(key)); err != nil {
		t.Fatalf("delete key error. bucket=%s, key=%s, %v", TestBucket, key, err)
	}

	if val, f, err := s.Get(TestBucket, []byte(key)); !errors.Is(err, KeyNotFoundError) {
		t.Fatalf("get error after delete. shoud be not exist. bucket=%s, key=%s, %v", TestBucket, key, err)
	} else if f {
		t.Fatalf("key should be not exist, but return found. bucket=%s, key=%s", TestBucket, key)
	} else if val != nil && len(val) > 0 {
		t.Fatalf("value should be empty but %s. bucket=%s, key=%s", string(val), TestBucket, key)
	}
}

// test p set, delete keys
func Test_badgerStore_PSetAndeDeleteKeys(t *testing.T) {
	var dir = getDataPath()
	t.Logf("data path %s", dir)

	// new store
	s, err := NewBadgerStore(badger.DefaultOptions(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.True(t, err == nil)

	// make 100 key value pair
	var keys = [][]byte{}
	var values = [][]byte{}
	keyPrefix := "tiger-"
	valuePrefix := "i-like-kv-"
	for i := 0; i < 100; i++ {
		keys = append(keys, []byte(keyPrefix+strconv.Itoa(i)))
		values = append(values, []byte(valuePrefix+strconv.Itoa(i)))
	}

	// set key value
	assert.True(t, s.PSet(TestBucket, keys, values) == nil)

	startIndexInclude := 20
	endIndexExclude := 40
	// delete some keys
	if err := s.DeleteKeys(TestBucket, keys[startIndexInclude:endIndexExclude]); err != nil {
		t.Fatalf("delete keys error %v", err)
	}

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		if i < startIndexInclude || i >= endIndexExclude {
			if val, f, err := s.Get(TestBucket, key); err != nil {
				t.Fatalf("get error. bucket=%s, key=%s, %v", TestBucket, key, err)
			} else if !f {
				t.Fatalf("key not exist, should be there. bucket=%s, key=%s", TestBucket, key)
			} else if string(val) != string(values[i]) {
				t.Fatalf("value should be same but not bucket=%s, key=%s, set-value=%s, get-value=%s", TestBucket, key, string(values[i]), string(val))
			}
		} else { // deleted keys
			if val, f, err := s.Get(TestBucket, key); !errors.Is(err, KeyNotFoundError) {
				t.Fatalf("get error after delete. shoud be not exist. bucket=%s, key=%s, %v", TestBucket, key, err)
			} else if f {
				t.Fatalf("key should be not exist, but return found. bucket=%s, key=%s", TestBucket, key)
			} else if val != nil && len(val) > 0 {
				t.Fatalf("value should be empty but %s. bucket=%s, key=%s", string(val), TestBucket, key)
			}
		}
	}
}

// test p set, keys
func Test_badgerStore_PSetAndeKeys(t *testing.T) {
	var dir = getDataPath()
	t.Logf("data path %s", dir)

	// new store
	s, err := NewBadgerStore(badger.DefaultOptions(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.True(t, err == nil)

	// make 100 key value pair
	var keys [][]byte
	var values [][]byte
	keyPrefix := "tiger-"
	valuePrefix := "i-like-kv-"
	for i := 0; i < 100; i++ {
		keys = append(keys, []byte(keyPrefix+strconv.Itoa(i)))
		values = append(values, []byte(valuePrefix+strconv.Itoa(i)))
	}

	// set key value
	assert.True(t, s.PSet(TestBucket, keys, values) == nil)

	// keys
	bucketPrefix := BuildKey(len(TestBucket)+len(keyPrefix), TestBucket, []byte(keyPrefix))
	if getKeys, getValues, err := s.Keys(TestBucket, bucketPrefix); err != nil {
		t.Fatalf("keys error %v", err)
	} else {
		for i := 0; i < len(keys); i++ {
			oldKey := string(keys[i])
			oldValue := string(values[i])

			val := ""
			for i, k := range getKeys {
				t.Logf("new key: " + string(k))
				if string(k) == oldKey {
					val = string(getValues[i])
					break
				}
			}
			if val == "" {
				t.Fatalf("set key %s , but keys not return", oldKey)
			}

			if val != oldValue {
				t.Fatalf("set key %s , keys return old value [%s] not same with new value [%s] ", oldKey, oldValue, val)
			}

		}
	}

}

func Test_badgerStore_PSetAndeKeys2(t *testing.T) {
	var dir = getDataPath()
	t.Logf("data path %s", dir)

	// new store
	s, err := NewBadgerStore(badger.DefaultOptions(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.True(t, err == nil)

	// make 100 key value pair
	var keys [][]byte
	var values [][]byte
	keyPrefix := "tiger-"
	valuePrefix := "i-like-kv-"
	for i := 0; i < 100; i++ {
		keys = append(keys, []byte(keyPrefix+strconv.Itoa(i)))
		values = append(values, []byte(valuePrefix+strconv.Itoa(i)))
	}

	// set key value
	assert.True(t, s.PSet(TestBucket, keys, values) == nil)

	// keys
	if getKeys, getValues, err := s.Keys(TestBucket, TestBucket); err != nil {
		t.Fatalf("keys error %v", err)
	} else {
		for i := 0; i < len(keys); i++ {
			oldKey := string(keys[i])
			oldValue := string(values[i])

			val := ""
			for i, k := range getKeys {
				t.Logf("new key: " + string(k))
				if string(k) == oldKey {
					val = string(getValues[i])
					break
				}
			}
			if val == "" {
				t.Fatalf("set key %s , but keys not return", oldKey)
			}

			if val != oldValue {
				t.Fatalf("set key %s , keys return old value [%s] not same with new value [%s] ", oldKey, oldValue, val)
			}

		}
	}

}

// test p set, key strings
func Test_badgerStore_PSetAndeKeyStrings(t *testing.T) {
	var dir = getDataPath()
	t.Logf("data path %s", dir)

	// new store
	s, err := NewBadgerStore(badger.DefaultOptions(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.True(t, err == nil)

	// make 100 key value pair
	var keys [][]byte
	var values [][]byte
	keyPrefix := "tiger-"
	valuePrefix := "i-like-kv-"
	for i := 0; i < 100; i++ {
		keys = append(keys, []byte(keyPrefix+strconv.Itoa(i)))
		values = append(values, []byte(valuePrefix+strconv.Itoa(i)))
	}

	// set key value
	assert.True(t, s.PSet(TestBucket, keys, values) == nil)

	// keys
	bucketPrefix := BuildKey(len(TestBucket)+len(keyPrefix), TestBucket, []byte(keyPrefix))
	if getKeys, getValues, err := s.KeyStrings(TestBucket, bucketPrefix); err != nil {
		t.Fatalf("keys error %v", err)
	} else {
		for i := 0; i < len(keys); i++ {
			oldKey := string(keys[i])
			oldValue := string(values[i])

			val := ""
			for i, k := range getKeys {
				t.Logf("new key: " + k)
				if k == oldKey {
					val = string(getValues[i])
					break
				}
			}
			if val == "" {
				t.Fatalf("set key %s , but keys not return", oldKey)
			}

			if val != oldValue {
				t.Fatalf("set key %s , keys return old value [%s] not same with new value [%s] ", oldKey, oldValue, val)
			}

		}
	}

}

// test p set, key without values
func Test_badgerStore_PSetAndeKeysWithoutValues(t *testing.T) {
	var dir = getDataPath()
	t.Logf("data path %s", dir)

	// new store
	s, err := NewBadgerStore(badger.DefaultOptions(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.True(t, err == nil)

	// make 100 key value pair
	var keys [][]byte
	var values [][]byte
	keyPrefix := "tiger-"
	valuePrefix := "i-like-kv-"
	for i := 0; i < 100; i++ {
		keys = append(keys, []byte(keyPrefix+strconv.Itoa(i)))
		values = append(values, []byte(valuePrefix+strconv.Itoa(i)))
	}

	// set key value
	assert.True(t, s.PSet(TestBucket, keys, values) == nil)

	// keys
	bucketPrefix := BuildKey(len(TestBucket)+len(keyPrefix), TestBucket, []byte(keyPrefix))
	if getKeys, err := s.KeysWithoutValues(TestBucket, bucketPrefix); err != nil {
		t.Fatalf("keys error %v", err)
	} else {
		for i := 0; i < len(keys); i++ {
			oldKey := string(keys[i])

			found := false
			for _, k := range getKeys {
				t.Logf("get key: " + string(k))
				if string(k) == oldKey {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("set key %s , but keys not return", oldKey)
			}

		}
	}

}

// test p set, key string without values
func Test_badgerStore_PSetAndeKeyStringsWithoutValues(t *testing.T) {
	var dir = getDataPath()
	t.Logf("data path %s", dir)

	// new store
	s, err := NewBadgerStore(badger.DefaultOptions(dir))
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.True(t, err == nil)

	// make 100 key value pair
	var keys [][]byte
	var values [][]byte
	keyPrefix := "tiger-"
	valuePrefix := "i-like-kv-"
	for i := 0; i < 100; i++ {
		keys = append(keys, []byte(keyPrefix+strconv.Itoa(i)))
		values = append(values, []byte(valuePrefix+strconv.Itoa(i)))
	}

	// set key value
	assert.True(t, s.PSet(TestBucket, keys, values) == nil)

	// keys
	bucketPrefix := BuildKey(len(TestBucket)+len(keyPrefix), TestBucket, []byte(keyPrefix))
	if getKeys, err := s.KeyStringsWithoutValues(TestBucket, bucketPrefix); err != nil {
		t.Fatalf("keys error %v", err)
	} else {
		for i := 0; i < len(keys); i++ {
			oldKey := string(keys[i])

			found := false
			for _, k := range getKeys {
				t.Logf("get key: " + k)
				if k == oldKey {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("set key %s , but keys not return", oldKey)
			}

		}
	}

}
