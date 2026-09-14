//go:build integration

package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"kbsync/internal/config"
)

func BenchmarkKingbaseTableParallelism(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	source := startDatabaseContainer(b, ctx)
	target := startDatabaseContainer(b, ctx)
	sourceDB := openTestDatabase(b, ctx, source.dsn)
	targetDB := openTestDatabase(b, ctx, target.dsn)

	var mappings []config.Table
	for i := 1; i <= 4; i++ {
		name := fmt.Sprintf("perf_parallel_%d", i)
		query := fmt.Sprintf(`CREATE TABLE public.%s (id bigint PRIMARY KEY, payload varchar(100), updated_at timestamp with time zone); INSERT INTO public.%s SELECT n, 'payload-' || n::text, '2026-09-14T00:00:00Z'::timestamptz FROM generate_series(1, 5000) n`, name, name)
		if _, err := sourceDB.ExecContext(ctx, query); err != nil {
			b.Fatal(err)
		}
		mappings = append(mappings, config.Table{Source: "public." + name, Target: "mirror." + name, Cursor: "updated_at", KeyColumns: []string{"id"}})
	}

	for _, parallelism := range []int{1, 2, 4} {
		parallelism := parallelism
		b.Run(fmt.Sprintf("parallel_%d", parallelism), func(b *testing.B) {
			for _, mapping := range mappings {
				table, _ := parseTableName(mapping.Target)
				_, _ = targetDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+table.SQL())
			}
			runner := &Syncer{
				source: sourceDB,
				target: targetDB,
				config: config.Config{Tables: mappings, BatchSize: 1000, Parallelism: parallelism, FullStrategy: "truncate"},
				log:    func(string, ...any) {},
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := runner.Full(ctx); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			for _, mapping := range mappings {
				assertRowCountBenchmark(b, ctx, targetDB, mapping.Target, 5000)
			}
		})
	}
}

func assertRowCountBenchmark(b *testing.B, ctx context.Context, db *sql.DB, table string, want int) {
	b.Helper()
	var got int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&got); err != nil || got != want {
		b.Fatalf("table %s count=%d want=%d error=%v", table, got, want, err)
	}
}
