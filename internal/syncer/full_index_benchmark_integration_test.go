//go:build integration

package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"kingbase.com/gokb"
)

func BenchmarkKingbaseFullIndexTiming(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	container := startDatabaseContainer(b, ctx)
	db := openTestDatabase(b, ctx, container.dsn)
	const rowCount = 5000
	rows := make([][]any, rowCount)
	for i := range rows {
		rows[i] = []any{int64(i + 1), int64(i % 100), fmt.Sprintf("payload-%05d", i), time.Date(2026, 9, 14, 0, 0, i%60, 0, time.UTC)}
	}

	for _, test := range []struct {
		name       string
		indexFirst bool
	}{
		{name: "index_before_copy", indexFirst: true},
		{name: "index_after_copy", indexFirst: false},
	} {
		test := test
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS public.perf_full")
				b.StartTimer()
				if err := fullIndexScenario(ctx, db, rows, test.indexFirst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func fullIndexScenario(ctx context.Context, db *sql.DB, rows [][]any, indexFirst bool) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE public.perf_full (id bigint PRIMARY KEY, tenant_id bigint NOT NULL, payload varchar(200) NOT NULL, updated_at timestamp with time zone NOT NULL)`); err != nil {
		return err
	}
	createIndex := func() error {
		_, err := db.ExecContext(ctx, `CREATE INDEX perf_full_tenant_updated_idx ON public.perf_full (tenant_id, updated_at)`)
		return err
	}
	if indexFirst {
		if err := createIndex(); err != nil {
			return err
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	statement, err := tx.PrepareContext(ctx, gokb.CopyInSchema("public", "perf_full", "id", "tenant_id", "payload", "updated_at"))
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for _, row := range rows {
		if _, err := statement.ExecContext(ctx, row...); err != nil {
			_ = statement.Close()
			_ = tx.Rollback()
			return err
		}
	}
	if _, err := statement.ExecContext(ctx); err != nil {
		_ = statement.Close()
		_ = tx.Rollback()
		return err
	}
	if err := statement.Close(); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if !indexFirst {
		return createIndex()
	}
	return nil
}
