package kvstore

import (
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"os"
)

const (
	DebugFlag = "KvBadgerDebug"
)

var (
	CanDebug = os.Getenv(DebugFlag) != ""
)

type KvStore interface {
	ReadOnly() bool

	Set(bucket, k []byte, v []byte) error

	Get(bucket, k []byte) (result []byte, found bool, e error)

	PSet(bucket []byte, keys, values [][]byte) error

	PGet(bucket []byte, keys [][]byte) ([][]byte, error)

	Delete(bucket, key []byte) error

	DeleteKeys(bucket []byte, keys [][]byte) error

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
	Keys(bucket []byte) (keys [][]byte, values [][]byte, err error)

	KeyStrings(bucket []byte) (keys []string, values [][]byte, err error)

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
	KeysWithoutValues(bucket []byte) (keys [][]byte, err error)

	KeyStringsWithoutValues(bucket []byte) (keys []string, err error)

	AllKeys(async func(key string, deletedOrExpired bool)) error

	Close() error
}

type badgerStore struct {
	db   *badger.DB
	opts badger.Options
}

func NewBadgerStore(opts badger.Options) (KvStore, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return badgerStore{
		db:   db,
		opts: opts,
	}, nil
}

func (b badgerStore) ReadOnly() bool {
	return b.opts.ReadOnly
}

func (b badgerStore) Set(bucket, k []byte, v []byte) error {
	newKey := AppendBytes(len(bucket)+len(k), bucket, k)
	l("Set", newKey)
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(newKey, v)
	})
}

func (b badgerStore) Get(bucket, k []byte) (result []byte, found bool, e error) {
	newKey := AppendBytes(len(bucket)+len(k), bucket, k)
	l("Get", newKey)
	var v []byte

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(newKey)
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

func (b badgerStore) PSet(bucket []byte, keys, values [][]byte) error {
	wb := b.db.NewWriteBatch()
	for i, key := range keys {
		newKey := AppendBytes(len(bucket)+len(key), bucket, key)
		l("PSet", newKey)
		err := wb.Set(newKey, values[i])
		if err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (b badgerStore) PGet(bucket []byte, keys [][]byte) ([][]byte, error) {
	var values = make([][]byte, len(keys))
	err := b.db.View(func(txn *badger.Txn) error {
		for i, key := range keys {
			newKey := AppendBytes(len(bucket)+len(key), bucket, key)
			l("PGet", newKey)
			item, err := txn.Get(newKey)
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

func (b badgerStore) Delete(bucket, key []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		newKey := AppendBytes(len(bucket)+len(key), bucket, key)
		l("Delete", newKey)
		return txn.Delete(key)
	})
}

func (b badgerStore) DeleteKeys(bucket []byte, keys [][]byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			newKey := AppendBytes(len(bucket)+len(key), bucket, key)
			l("DeleteKeys", newKey)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b badgerStore) Keys(bucket []byte) (keys [][]byte, values [][]byte, err error) {
	l("Keys", bucket)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(bucket); it.ValidForPrefix(bucket); it.Next() {
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

func (b badgerStore) KeyStrings(bucket []byte) (keys []string, values [][]byte, err error) {
	l("KeyStrings", bucket)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(bucket); it.ValidForPrefix(bucket); it.Next() {
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

func (b badgerStore) KeysWithoutValues(bucket []byte) (keys [][]byte, err error) {
	l("KeysWithoutValues", bucket)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			PrefetchSize:   100,
			Reverse:        false,
			AllVersions:    false,
		})
		defer it.Close()

		var tmpKeys [][]byte
		for it.Seek(bucket); it.ValidForPrefix(bucket); it.Next() {
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

func (b badgerStore) KeyStringsWithoutValues(bucket []byte) (keys []string, err error) {
	l("KeyStringsWithoutValues", bucket)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			PrefetchSize:   100,
			Reverse:        false,
			AllVersions:    false,
		})
		defer it.Close()
		for it.Seek(bucket); it.ValidForPrefix(bucket); it.Next() {
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

func (b badgerStore) AllKeys(async func(key string, deletedOrExpired bool)) error {
	l("AllKeys")
	return b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			PrefetchSize:   100,
			Reverse:        false,
			AllVersions:    false,
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			async(string(item.Key()), item.IsDeletedOrExpired())
		}
		return nil
	})
}

func (b badgerStore) Close() error {
	l("Close")
	if !b.db.IsClosed() {
		return b.db.Close()
	}
	return nil
}

func l(method string, keys ...[]byte) {
	if CanDebug {
		for _, key := range keys {
			fmt.Println(fmt.Sprintf("[%s] %s=%s", DebugFlag, method, string(key)))
		}
	}
}
