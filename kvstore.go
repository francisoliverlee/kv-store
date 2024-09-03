package kvstore

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"strings"
)

const (
	KeySpilt = "@"
)

var (
	CanLogKey = true
)

type KvStore interface {
	Set(bucket string, key []byte, value []byte) error

	Get(bucket string, key []byte) ([]byte, bool, error)

	PSet(bucket string, keys, values [][]byte) error

	PGet(bucket string, keys [][]byte) ([][]byte, error)

	Delete(bucket string, key []byte) error

	DeleteKeys(bucket string, keys [][]byte) error

	/*
		https://github.com/dgraph-io/badger/issues/2014
		TODO bug 使用 keyPrefix := "cluster#broker#name#@cluster-test-1@"， 结果返回如下， 明显不是前缀。。。
		// 并且 cluster#broker#name#@cluster-test@broker-test-890 等key在db中不存在
		// 使用 KeyStringsWithoutValues方法可以
		// 			  cluster#broker#name#@cluster-test@broker-test-890
		//            cluster#broker#name#@cluster-test@broker-test-9-1
		//            cluster#broker#name#@cluster-test@broker-test-9010
		//            cluster#broker#name#@cluster-test@broker-test-9111
		// 			  cluster#broker#name#@cluster-test@broker-test-922
		// 			  cluster#broker#name#@cluster-test@broker-test-933

	*/
	Keys(bucket string, pattern []byte) (keys [][]byte, values [][]byte, err error)

	KeyStrings(bucket string, pattern []byte) (keys []string, values [][]byte, err error)

	AllKeys(bucket string) (keys []string, err error)

	/*
		https://github.com/dgraph-io/badger/issues/2014
		TODO bug 使用 keyPrefix := "cluster#broker#name#@cluster-test-1@"， 结果返回如下， 明显不是前缀。。。
		// 并且 cluster#broker#name#@cluster-test@broker-test-890 等key在db中不存在
		// 使用 KeyStringsWithoutValues方法可以
		// 			  cluster#broker#name#@cluster-test@broker-test-890
		//            cluster#broker#name#@cluster-test@broker-test-9-1
		//            cluster#broker#name#@cluster-test@broker-test-9010
		//            cluster#broker#name#@cluster-test@broker-test-9111
		// 			  cluster#broker#name#@cluster-test@broker-test-922
		// 			  cluster#broker#name#@cluster-test@broker-test-933

	*/
	KeysWithoutValues(bucket string, pattern []byte) (keys [][]byte, err error)

	KeyStringsWithoutValues(bucket string, pattern []byte) (keys []string, err error)

	BuildKey(prefix string, keys ...string) (string, error)

	BuildKeyPrefix(prefix string, keys ...string) (string, error)

	UnBuildKey(key string) []string

	Close() error
}

type badgerStore struct {
	db   *badger.DB
	path string
	opts badger.Options
}

func NewBadgerStore(path string, fsync bool, opts badger.Options) (KvStore, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return badgerStore{
		db:   db,
		path: path,
		opts: opts,
	}, nil
}

func (b badgerStore) Set(bucket string, key []byte, value []byte) error {
	b.logKey("Set", key)
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (b badgerStore) Get(bucket string, key []byte) (result []byte, found bool, e error) {
	b.logKey("Get", key)
	var v []byte

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == nil {
			err = item.Value(func(value []byte) error {
				v = value
				return nil
			})
		}
		return err
	})

	if err == badger.ErrKeyNotFound {
		return nil, false, err
	}
	if err == nil {
		found = true
	}
	return v, found, err
}

func (b badgerStore) PSet(bucket string, keys, values [][]byte) error {
	b.logKey("PSet", keys...)
	wb := b.db.NewWriteBatch()
	for i := range keys {
		err := wb.Set(keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (b badgerStore) DeleteKeys(bucket string, keys [][]byte) error {
	b.logKey("DeleteKeys", keys...)
	return b.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b badgerStore) PGet(bucket string, keys [][]byte) ([][]byte, error) {
	b.logKey("PGet", keys...)

	var values = make([][]byte, len(keys))
	err := b.db.View(func(txn *badger.Txn) error {
		for i, k := range keys {
			item, err := txn.Get(k)
			if err == nil {
				v, err := item.ValueCopy(nil)
				if err == nil {
					values[i] = v
				}
			}
		}
		return nil
	})

	return values, err
}

func (b badgerStore) Delete(bucket string, key []byte) error {
	b.logKey("DeleteCluster", key)
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b badgerStore) KeysWithoutValues(bucket string, pattern []byte) (keys [][]byte, err error) {
	b.logKey("KeysWithoutValues", pattern)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			PrefetchSize:   100,
			Reverse:        false,
			AllVersions:    false,
		})
		defer it.Close()
		prefix := pattern

		var tmpKeys [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			k := item.Key()
			tmpKeys = append(tmpKeys, k)
		}
		keys = tmpKeys
		return nil
	})

	return keys, err
}

func (b badgerStore) Keys(bucket string, pattern []byte) (keys [][]byte, values [][]byte, err error) {
	b.logKey("Keys", pattern)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(pattern); it.ValidForPrefix(pattern); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			k := item.Key()
			keys = append(keys, k)
			v, err := item.ValueCopy(nil)
			values = append(values, v)
			if err != nil {
				return err
			}

		}
		return nil
	})

	return keys, values, err
}

func (b badgerStore) KeyStrings(bucket string, pattern []byte) (keys []string, values [][]byte, err error) {
	b.logKey("KeyStrings", pattern)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(pattern); it.ValidForPrefix(pattern); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			keys = append(keys, string(item.Key()))
			v, err := item.ValueCopy(nil)
			values = append(values, v)
			if err != nil {
				return err
			}

		}
		return nil
	})

	return keys, values, err
}

func (b badgerStore) KeyStringsWithoutValues(bucket string, pattern []byte) (keys []string, err error) {
	b.logKey("KeyStringsWithoutValues", pattern)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			PrefetchSize:   100,
			Reverse:        false,
			AllVersions:    false,
		})
		defer it.Close()
		for it.Seek(pattern); it.ValidForPrefix(pattern); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			k := item.Key()
			keys = append(keys, string(k))

		}
		return nil
	})

	return keys, err
}

func (b badgerStore) AllKeys(bucket string) (keys []string, err error) {
	b.logKey("AllKeys")
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			PrefetchSize:   100,
			Reverse:        false,
			AllVersions:    false,
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			keys = append(keys, string(item.Key()))
		}
		return nil
	})

	return keys, err
}

// 精确key构造
func (b badgerStore) BuildKey(prefix string, keys ...string) (string, error) {
	if len(keys) == 0 {
		return "", errors.New("keys are empty")
	}
	sb := strings.Builder{}
	sb.WriteString(prefix)
	for _, key := range keys {
		sb.WriteString(KeySpilt)
		sb.WriteString(key)
	}
	return sb.String(), nil
}

// BuildKeyPrefix 前缀匹配key构造
// 使用前缀匹配扫描时， keys不能是中每个元素不能只是部分
// 比如： 111@222@333@， 可以使用111@， 111@222@ 扫描前缀。
// 但是不能使用11作为前缀， 可能匹配到111@222@333， 也可能是11232323@asdasd@23fwdf
func (b badgerStore) BuildKeyPrefix(prefix string, keys ...string) (string, error) {
	str, err := b.BuildKey(prefix, keys...)
	if err != nil {
		return "", err
	}
	return str + KeySpilt, nil
}

func (b badgerStore) UnBuildKey(key string) []string {
	if key == "" {
		return nil
	}
	return strings.Split(key, KeySpilt)
}

func (b badgerStore) Close() error {
	if !b.db.IsClosed() {
		return b.db.Close()
	}
	return nil
}

func (b badgerStore) logKey(method string, keys ...[]byte) {
	if CanLogKey {
		for _, key := range keys {
			fmt.Println(fmt.Sprintf("db method=%s, key/prefix = %s", method, key))
		}
	}
}
