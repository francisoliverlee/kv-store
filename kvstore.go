package kvstore

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"log"
	"os"
	"strings"
)

const (
	DebugFlag = "KvBadgerDebug"
)

var (
	CanDebug = os.Getenv(DebugFlag) != ""
)
var (
	KeyNotFoundError = errors.New("key not found")
)

type KvStore interface {
	// Set a key-value in a bucket
	Set(bucket, k []byte, v []byte) error

	// Get a key-value in a bucket
	Get(bucket, k []byte) (result []byte, found bool, e error)

	// PSet set multi key-values in a bucket
	PSet(bucket []byte, keys, values [][]byte) error

	// PGet get multi key-values in a bucket
	PGet(bucket []byte, keys [][]byte) ([][]byte, error)

	// Delete a key in a bucket
	Delete(bucket, key []byte) error

	// DeleteKeys delete multi keys in a bucket
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
	// Keys get key and value in a bucket
	Keys(bucket, prefix []byte) (keys [][]byte, values [][]byte, err error)

	// KeyStrings return key and values as bytes
	KeyStrings(bucket, prefix []byte) (keys []string, values [][]byte, err error)

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
	// KeysWithoutValues return key as []byte
	KeysWithoutValues(bucket, prefix []byte) (keys [][]byte, err error)

	// KeyStringsWithoutValues return key as string
	KeyStringsWithoutValues(bucket, prefix []byte) (keys []string, err error)

	// AllKeys to get
	AllKeys(async func(key string, deletedOrExpired bool)) error

	// Close a database conn
	Close() error

	// Sync flush
	Sync() error

	// Exec a transaction. should NOT use it
	Exec(f func(txn *badger.Txn) error) error

	// ReadOnly check db is read only
	ReadOnly() bool

	// Path store path for key and values'
	Path() []string
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

func (b badgerStore) Set(bucket, k []byte, v []byte) error {
	newKey := BuildKey(len(bucket)+len(k), bucket, k)
	L("Set", newKey, v)
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(newKey, v)
	})
}

func (b badgerStore) Get(bucket, k []byte) (result []byte, found bool, e error) {
	newKey := BuildKey(len(bucket)+len(k), bucket, k)
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
	L("Get", newKey, v)

	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, KeyNotFoundError
	}
	if err == nil {
		found = true
	}
	return v, found, err
}

func (b badgerStore) PSet(bucket []byte, keys, values [][]byte) error {
	wb := b.db.NewWriteBatch()
	for i, key := range keys {
		newKey := BuildKey(len(bucket)+len(key), bucket, key)
		L("PSet", newKey, values[i])
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
			newKey := BuildKey(len(bucket)+len(key), bucket, key)
			item, err := txn.Get(newKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				L("PGet", newKey, []byte(KeyNotFoundError.Error()))
				return KeyNotFoundError
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(val []byte) error {
				values[i] = val
				return nil
			}); err != nil {
				return err
			}
			L("PGet", newKey, values[i])
		}
		return nil
	})

	return values, err
}

func (b badgerStore) Delete(bucket, key []byte) error {
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()
	newKey := BuildKey(len(bucket)+len(key), bucket, key)
	L("Delete", newKey)
	if err := wb.Delete(newKey); err != nil {
		return err
	} else {
		return wb.Flush()
	}
}

func (b badgerStore) DeleteKeys(bucket []byte, keys [][]byte) error {
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()
	for _, key := range keys {
		newKey := BuildKey(len(bucket)+len(key), bucket, key)
		L("DeleteKeys", newKey)

		if err := wb.Delete(newKey); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (b badgerStore) Keys(bucket, prefix []byte) (keys [][]byte, values [][]byte, err error) {
	L("Keys", prefix)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			userKey := RemovePrefix(item.Key(), BuildKey(len(bucket), bucket, []byte{})) // Remove bucket and split in key
			keys = append(keys, userKey)
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

func (b badgerStore) KeyStrings(bucket, prefix []byte) (keys []string, values [][]byte, err error) {
	L("KeyStrings", prefix)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			userKey := RemovePrefix(item.Key(), BuildKey(len(bucket), bucket, []byte{}))
			keys = append(keys, string(userKey))
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

func (b badgerStore) KeysWithoutValues(bucket, prefix []byte) ([][]byte, error) {
	L("KeysWithoutValues", prefix)
	var keys [][]byte
	return keys, b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			PrefetchSize:   100,
			Reverse:        false,
			AllVersions:    false,
		})
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			userKey := RemovePrefix(item.Key(), BuildKey(len(bucket), bucket, []byte{}))
			keys = append(keys, userKey)
		}
		return nil
	})
}

func (b badgerStore) KeyStringsWithoutValues(bucket, prefix []byte) (keys []string, err error) {
	L("KeyStringsWithoutValues", prefix)
	err = b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			PrefetchSize:   100,
			Reverse:        false,
			AllVersions:    false,
		})
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			userKey := RemovePrefix(item.Key(), BuildKey(len(bucket), bucket, []byte{}))
			keys = append(keys, string(userKey))

		}
		return nil
	})

	return keys, err
}

func (b badgerStore) AllKeys(async func(key string, deletedOrExpired bool)) error {
	L("AllKeys")
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
	L("Close")
	if !b.db.IsClosed() {
		return b.db.Close()
	}
	return nil
}

func (b badgerStore) Sync() error {
	L("Sync")
	return b.db.Sync()
}

func (b badgerStore) Exec(f func(tx *badger.Txn) error) error {
	L("Exec")
	return b.db.Update(func(txn *badger.Txn) error {
		return f(txn)
	})
}

func (b badgerStore) ReadOnly() bool {
	return b.opts.ReadOnly
}

func (b badgerStore) Path() []string {
	return []string{
		b.opts.Dir,
		b.opts.ValueDir,
	}
}

func L(method string, keys ...[]byte) {
	if CanDebug {
		var arr []string
		if keys != nil && len(keys) > 0 {
			for _, key := range keys {
				arr = append(arr, string(key))
			}
		}
		log.Printf(fmt.Sprintf("%s,%s,%s:%s", Now(), DebugFlag, method, strings.Join(arr, ",")))
	}
}
