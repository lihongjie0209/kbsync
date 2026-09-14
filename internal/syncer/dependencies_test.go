package syncer

import (
	"strings"
	"testing"

	"kbsync/internal/config"
)

func TestTopologicalLevels(t *testing.T) {
	t.Parallel()
	mappings := map[string]config.Table{
		"public.users":  {Source: "public.users"},
		"public.orders": {Source: "public.orders"},
		"public.events": {Source: "public.events"},
	}
	dependencies := map[string]map[string]bool{
		"public.users":  {},
		"public.orders": {"public.users": true},
		"public.events": {},
	}
	levels, err := topologicalLevels([]string{"public.orders", "public.events", "public.users"}, mappings, dependencies)
	if err != nil {
		t.Fatalf("topologicalLevels() error = %v", err)
	}
	if len(levels) != 2 || len(levels[0]) != 2 || levels[1][0].Source != "public.orders" {
		t.Fatalf("unexpected levels: %#v", levels)
	}
}

func TestTopologicalLevelsRejectsCycle(t *testing.T) {
	t.Parallel()
	mappings := map[string]config.Table{
		"public.a": {Source: "public.a"},
		"public.b": {Source: "public.b"},
	}
	dependencies := map[string]map[string]bool{
		"public.a": {"public.b": true},
		"public.b": {"public.a": true},
	}
	_, err := topologicalLevels([]string{"public.a", "public.b"}, mappings, dependencies)
	if err == nil || !strings.Contains(err.Error(), "循环外键") {
		t.Fatalf("topologicalLevels() error = %v", err)
	}
}
