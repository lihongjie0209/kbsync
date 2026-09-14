//go:build integration

package syncer

import (
	"context"
	"testing"
	"time"

	"kbsync/internal/config"
)

// BenchmarkKingbaseIncrementalMetadata measures the catalog work performed
// before an incremental table is read. The single_snapshot case represents
// reusing metadata already collected by the all-table preflight in the same
// Incremental invocation; it deliberately is not a cross-invocation cache.
func BenchmarkKingbaseIncrementalMetadata(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	source := startDatabaseContainer(b, ctx)
	target := startDatabaseContainer(b, ctx)
	sourceDB := openTestDatabase(b, ctx, source.dsn)
	targetDB := openTestDatabase(b, ctx, target.dsn)

	if _, err := sourceDB.ExecContext(ctx, `
CREATE TABLE public.perf_metadata (
  tenant_id bigint NOT NULL,
  id bigint NOT NULL,
  payload varchar(100),
  updated_at timestamp with time zone NOT NULL,
  PRIMARY KEY (tenant_id, id)
);
CREATE INDEX perf_metadata_updated_idx ON public.perf_metadata (updated_at);`); err != nil {
		b.Fatal(err)
	}
	mapping := config.Table{
		Source: "public.perf_metadata",
		Target: "mirror.perf_metadata",
		Cursor: "updated_at",
	}
	runner := &Syncer{source: sourceDB, target: targetDB, config: config.Config{BatchSize: 1000}, log: func(string, ...any) {}}
	if _, _, _, err := runner.tableMetadata(ctx, mapping); err != nil {
		b.Fatal(err)
	}

	b.Run("current_repeated_inspection", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			sourceTable, targetTable, columns, err := runner.tableMetadata(ctx, mapping)
			if err != nil {
				b.Fatal(err)
			}
			meta, _, err := inspectTable(ctx, sourceDB, sourceTable)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := incrementalColumns(columns, meta.PrimaryKey, mapping); err != nil {
				b.Fatal(err)
			}
			_ = targetTable
		}
	})

	b.Run("single_catalog_snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			prepared, err := runner.prepareStructure(ctx, mapping, true)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := incrementalColumns(prepared.columns, prepared.primaryKey, mapping); err != nil {
				b.Fatal(err)
			}
		}
	})
}
