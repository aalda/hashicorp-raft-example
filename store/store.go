// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm, specifically the
// Hashicorp implementation.
package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/aalda/hashicorp-raft-example/raftbadger"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	DataDir      string
	RaftDir      string
	RaftBindAddr string

	mu   sync.Mutex
	data *badger.DB
	//data map[string]string // the key-value store for the system

	raft *raft.Raft // the consensus mechanism

	logger *log.Logger
}

// New returns a new Store
func New(dataDir, raftDir, raftBindAddr string) *Store {
	return &Store{
		DataDir:      dataDir,
		RaftDir:      raftDir,
		RaftBindAddr: raftBindAddr,
		logger:       log.New(os.Stderr, "[store] ", log.LstdFlags),
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *Store) Open(enableSingle bool, localID string) error {

	// Open data storage
	opts := badger.DefaultOptions
	opts.Dir = s.DataDir
	opts.ValueDir = s.DataDir
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.data = db

	// Setup Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	// Setup Raft communication
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBindAddr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store
	var logStore raft.LogStore
	var stableStore raft.StableStore
	// temporary in memory:
	// logStore = raft.NewInmemStore()
	// stableStore = raft.NewInmemStore()
	logStore, err = raftbadger.NewBadgerStore(s.RaftDir + "/logs")
	if err != nil {
		return fmt.Errorf("new badger store: %s", err)
	}
	stableStore, err = raftbadger.NewBadgerStore(s.RaftDir + "/config")
	if err != nil {
		return fmt.Errorf("new badger store: %s", err)
	}

	// Instantiate the Raft system
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil

}

// Get returns the value for the given key.
func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var value []byte
	err := s.data.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(value)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return string(value[:]), nil
}

// Set sets the value for the given key.
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader { // TODO redirect to leader???
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:  "delete",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(nodeID, addr string) error {
	s.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

type fsm Store // TODO extract to db.go with the database => balloon

type fsmGenericResponse struct {
	error error
}

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &fsmSnapshot{store: f.data}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.data.Load(rc)
	return nil
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.data.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(value))
	})
	return &fsmGenericResponse{error: err}
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.data.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	return &fsmGenericResponse{error: err}
}

type fsmSnapshot struct {
	store *badger.DB
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		_, err := f.store.Backup(sink, 0)
		if err == nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
