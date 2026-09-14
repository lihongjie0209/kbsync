package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte(`
source:
  dsn: source-from-file
target:
  dsn: target-from-file
batch_size: 25
tables:
  - source: public.users
    cursor: updated_at
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path, map[string]string{"source.dsn": "source-from-flag"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Source.DSN != "source-from-flag" {
		t.Fatalf("source DSN = %q", cfg.Source.DSN)
	}
	if cfg.Target.DSN != "target-from-file" || cfg.BatchSize != 25 {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	if cfg.CheckpointBatches != 10 {
		t.Fatalf("checkpoint_every_batches = %d, want 10", cfg.CheckpointBatches)
	}
	if cfg.Tables[0].Target != "public.users" {
		t.Fatalf("default target = %q", cfg.Tables[0].Target)
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "missing DSNs", cfg: Config{}},
		{name: "missing tables", cfg: Config{Source: Database{DSN: "s"}, Target: Database{DSN: "t"}, BatchSize: 1}},
		{name: "invalid batch", cfg: Config{Source: Database{DSN: "s"}, Target: Database{DSN: "t"}, Tables: []Table{{Source: "x"}}}},
		{name: "invalid strategy", cfg: Config{Source: Database{DSN: "s"}, Target: Database{DSN: "t"}, Tables: []Table{{Source: "x"}}, BatchSize: 1, FullStrategy: "drop"}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}
