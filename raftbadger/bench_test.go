package raftbadger

import (
	"os"
	"testing"

	"github.com/hashicorp/raft/bench"
)

func BenchmarkBadgerStore_FirstIndex(b *testing.B) {
	store, path := testBadgerStore(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkBadgerStore_LastIndex(b *testing.B) {
	store, path := testBadgerStore(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.LastIndex(b, store)
}

func BenchmarkBadgerStore_GetLog(b *testing.B) {
	store, path := testBadgerStore(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.GetLog(b, store)
}

func BenchmarkBadgerStore_StoreLog(b *testing.B) {
	store, path := testBadgerStore(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.StoreLog(b, store)
}

func BenchmarkBadgerStore_StoreLogs(b *testing.B) {
	store, path := testBadgerStore(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkBadgerStore_DeleteRange(b *testing.B) {
	store, path := testBadgerStore(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkBadgerStore_Set(b *testing.B) {
	store, path := testBadgerStore(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.Set(b, store)
}

func BenchmarkBadgerStore_Get(b *testing.B) {
	store, path := testBadgerStore(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.Get(b, store)
}

func BenchmarkBadgerStore_SetUint64(b *testing.B) {
	store, path := testBadgerStore(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.SetUint64(b, store)
}

func BenchmarkBadgerStore_GetUint64(b *testing.B) {
	store, path := testBadgerStore(b)
	defer store.Close()
	defer os.Remove(path)

	raftbench.GetUint64(b, store)
}
