package syncer

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStateRoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "nested", "state.json")
	want := state{Tables: map[string]checkpoint{
		"public.users->mirror.users": {Values: []string{"2026-09-14T10:00:00Z", "42"}},
	}}
	if err := saveState(path, want); err != nil {
		t.Fatalf("saveState() error = %v", err)
	}
	got, err := loadState(path)
	if err != nil {
		t.Fatalf("loadState() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loadState() = %#v, want %#v", got, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestLoadStateMissingFile(t *testing.T) {
	t.Parallel()
	got, err := loadState(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("loadState() error = %v", err)
	}
	if got.Tables == nil || len(got.Tables) != 0 {
		t.Fatalf("loadState() = %#v", got)
	}
}

func TestCheckpointStoreDefersPersistenceUntilFlush(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.json")
	store := checkpointStore{path: path, state: state{Tables: make(map[string]checkpoint)}}
	want := checkpoint{Values: []string{"cursor-10", "42"}}
	store.update("source->target", want)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("state file before flush error = %v, want not exist", err)
	}
	if err := store.flush(); err != nil {
		t.Fatalf("flush() error = %v", err)
	}
	got, err := loadState(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Tables["source->target"], want) {
		t.Fatalf("checkpoint = %#v, want %#v", got.Tables["source->target"], want)
	}
	if err := store.flush(); err != nil {
		t.Fatalf("clean flush() error = %v", err)
	}
}

func TestCheckpointEveryBatches(t *testing.T) {
	t.Parallel()
	if got := checkpointEveryBatches(0); got != 10 {
		t.Fatalf("default checkpoint interval = %d, want 10", got)
	}
	if got := checkpointEveryBatches(25); got != 25 {
		t.Fatalf("configured checkpoint interval = %d, want 25", got)
	}
}
