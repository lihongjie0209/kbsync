//go:build integration

package syncer

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"kingbase.com/gokb"
)

func BenchmarkKingbaseNoKeyCursorGroups(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	container := startDatabaseContainer(b, ctx)
	db := openTestDatabase(b, ctx, container.dsn)
	if _, err := db.ExecContext(ctx, `CREATE TABLE public.perf_events (event_time timestamp with time zone NOT NULL, payload varchar(100) NOT NULL)`); err != nil {
		b.Fatal(err)
	}
	const rowCount = 200
	rows := make([][]any, rowCount)
	for i := range rows {
		rows[i] = []any{time.Date(2026, 9, 14, 0, 0, i, 0, time.UTC), "payload"}
	}

	for _, groupSize := range []int{1, 10, 50} {
		groupSize := groupSize
		b.Run("cursor_groups_"+strconv.Itoa(groupSize), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE public.perf_events"); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				for start := 0; start < len(rows); start += groupSize {
					end := min(start+groupSize, len(rows))
					if err := replaceNoKeyBenchmarkGroup(ctx, db, rows[start:end]); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(rowCount*b.N)/b.Elapsed().Seconds(), "rows/s")
			var count int
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.perf_events").Scan(&count); err != nil || count != rowCount {
				b.Fatalf("row count = %d, error = %v", count, err)
			}
		})
	}
}

func replaceNoKeyBenchmarkGroup(ctx context.Context, db *sql.DB, rows [][]any) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	placeholders := make([]string, len(rows))
	args := make([]any, len(rows))
	for i, row := range rows {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = row[0]
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM public.perf_events WHERE event_time IN ("+strings.Join(placeholders, ", ")+")", args...); err != nil {
		_ = tx.Rollback()
		return err
	}
	statement, err := tx.PrepareContext(ctx, gokb.CopyInSchema("public", "perf_events", "event_time", "payload"))
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
	return tx.Commit()
}
