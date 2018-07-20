package raftbadger

import (
	"errors"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
)

var (
	// ErrKeyNotFound is an error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// BadgerStore provides access to Badger for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type BadgerStore struct {
	// conn is the underlying handle to the db.
	conn *badger.DB

	// The path to the Badger database directory.
	path string
}

// Options contains all the configuration used to open the Badger db
type Options struct {
	// Path is the directory path to the Badger db to use.
	Path string

	// BadgerOptions contains any specific Badger options you might
	// want to specify.
	BadgerOptions *badger.Options

	// NoSync causes the database to skip fsync calls after each
	// write to the log. This is unsafe, so it should be used
	// with caution.
	NoSync bool
}

// NewBadgerStore takes a file path and returns a connected Raft backend.
func NewBadgerStore(path string) (*BadgerStore, error) {
	return New(Options{Path: path})
}

// func NewDefaultStableStore(path string) (*BadgerStore, error) {
// 	opts := badger.DefaultOptions
// 	opts.MaxLevels = 2
// 	return New(Options{Path: path, BadgerOptions: &opts})
// }

// New uses the supplied options to open the Badger db and prepare it for
// use as a raft backend.
func New(options Options) (*BadgerStore, error) {

	// build badger options
	if options.BadgerOptions == nil {
		defaultOpts := badger.DefaultOptions
		options.BadgerOptions = &defaultOpts
	}
	options.BadgerOptions.Dir = options.Path
	options.BadgerOptions.ValueDir = options.Path
	options.BadgerOptions.SyncWrites = !options.NoSync

	// Try to connect
	handle, err := badger.Open(*options.BadgerOptions)
	if err != nil {
		return nil, err
	}

	// Create the new store
	store := &BadgerStore{
		conn: handle,
		path: options.Path,
	}

	return store, nil
}

// Close is used to gracefully close the DB connection.
func (b *BadgerStore) Close() error {
	return b.conn.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (b *BadgerStore) FirstIndex() (uint64, error) {
	return b.firstIndex(false)
}

// LastIndex returns the last known index from the Raft log.
func (b *BadgerStore) LastIndex() (uint64, error) {
	return b.firstIndex(true)
}

func (b *BadgerStore) firstIndex(reverse bool) (uint64, error) {
	var value uint64
	err := b.conn.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Reverse:        reverse,
		})
		defer it.Close()

		it.Rewind()
		if it.Valid() {
			value = bytesToUint64(it.Item().Key())
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return value, nil
}

// GetLog gets a log entry from Badger at a given index.
func (b *BadgerStore) GetLog(index uint64, log *raft.Log) error {
	err := b.conn.View(func(txn *badger.Txn) error {
		item, err := txn.Get(uint64ToBytes(index))
		if err != nil {
			switch err {
			case badger.ErrKeyNotFound:
				return raft.ErrLogNotFound
			default:
				return err
			}
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		return decodeMsgPack(val, log)
	})
	if err != nil {
		return err
	}
	return nil
}

// StoreLog stores a single raft log.
func (b *BadgerStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores a set of raft logs.
func (b *BadgerStore) StoreLogs(logs []*raft.Log) error {
	err := b.conn.Update(func(txn *badger.Txn) error {
		for _, log := range logs {
			key := uint64ToBytes(log.Index)
			val, err := encodeMsgPack(log)
			if err != nil {
				return err
			}
			if err := txn.Set(key, val.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// DeleteRange deletes logs within a given range inclusively.
func (b *BadgerStore) DeleteRange(min, max uint64) error {
	// we manage the transaction manually in order to avoid ErrTxnTooBig errors
	txn := b.conn.NewTransaction(true)
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Reverse:        false,
	})

	for it.Seek(uint64ToBytes(min)); it.Valid(); it.Next() {
		key := make([]byte, 8)
		it.Item().KeyCopy(key)
		// Handle out-of-range log index
		if bytesToUint64(key) > max {
			break
		}
		// Delete in-range log index
		if err := txn.Delete(key); err != nil {
			if err == badger.ErrTxnTooBig {
				it.Close()
				err = txn.Commit(nil)
				if err != nil {
					return err
				}
				return b.DeleteRange(bytesToUint64(key), max)
			}
			return err
		}
	}
	it.Close()
	err := txn.Commit(nil)
	if err != nil {
		return err
	}
	return nil
}

// Set is used to set a key/value set outside of the raft log.
func (b *BadgerStore) Set(key []byte, val []byte) error {
	return b.conn.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

// Get is used to retrieve a value from the k/v store by key
func (b *BadgerStore) Get(key []byte) ([]byte, error) {
	var value []byte
	err := b.conn.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			switch err {
			case badger.ErrKeyNotFound:
				return ErrKeyNotFound
			default:
				return err
			}
		}
		value, err = item.ValueCopy(value)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *BadgerStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (b *BadgerStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}
