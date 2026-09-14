//go:build integration

package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"kingbase.com/gokb"
)

func BenchmarkKingbaseUpsertStrategies(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	container := startDatabaseContainer(b, ctx)
	db := openTestDatabase(b, ctx, container.dsn)
	if _, err := db.ExecContext(ctx, `
CREATE TABLE public.perf_items (
    id bigint PRIMARY KEY,
    tenant_id bigint NOT NULL,
    payload varchar(200) NOT NULL,
    updated_at timestamp with time zone NOT NULL
)`); err != nil {
		b.Fatal(err)
	}

	const rowCount = 1000
	columns := []string{"id", "tenant_id", "payload", "updated_at"}
	keys := []string{"id"}
	rows := make([][]any, rowCount)
	for i := range rows {
		rows[i] = []any{int64(i + 1), int64(i % 10), fmt.Sprintf("payload-%04d", i), time.Date(2026, 9, 14, 0, 0, i%60, 0, time.UTC)}
	}
	table := tableName{Schema: "public", Name: "perf_items"}

	b.Run("row_by_row_baseline", func(b *testing.B) {
		benchmarkUpsert(b, ctx, db, rowCount, func(tx *sql.Tx) error {
			statement, err := tx.PrepareContext(ctx, buildUpsertSQL(table, columns, keys))
			if err != nil {
				return err
			}
			defer func() { _ = statement.Close() }()
			for _, values := range rows {
				if _, err := statement.ExecContext(ctx, values...); err != nil {
					return err
				}
			}
			return nil
		})
	})

	for _, batchSize := range []int{50, 100, 250, 500, 1000} {
		batchSize := batchSize
		b.Run(fmt.Sprintf("multi_row_batch_%d", batchSize), func(b *testing.B) {
			benchmarkUpsert(b, ctx, db, rowCount, func(tx *sql.Tx) error {
				for start := 0; start < len(rows); start += batchSize {
					end := min(start+batchSize, len(rows))
					query, args := buildBatchUpsertSQL(table, columns, keys, rows[start:end])
					if _, err := tx.ExecContext(ctx, query, args...); err != nil {
						return err
					}
				}
				return nil
			})
		})
	}

	b.Run("unchanged_unconditional", func(b *testing.B) {
		benchmarkUnchangedUpsert(b, ctx, db, table, columns, keys, rows, false)
	})
	b.Run("unchanged_skip_update", func(b *testing.B) {
		benchmarkUnchangedUpsert(b, ctx, db, table, columns, keys, rows, true)
	})
	b.Run("staging_copy_merge", func(b *testing.B) {
		benchmarkUpsert(b, ctx, db, rowCount, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE perf_items_stage (LIKE public.perf_items INCLUDING DEFAULTS) ON COMMIT DROP`); err != nil {
				return err
			}
			statement, err := tx.PrepareContext(ctx, gokb.CopyIn("perf_items_stage", columns...))
			if err != nil {
				return err
			}
			for _, values := range rows {
				if _, err := statement.ExecContext(ctx, values...); err != nil {
					_ = statement.Close()
					return err
				}
			}
			if _, err := statement.ExecContext(ctx); err != nil {
				_ = statement.Close()
				return err
			}
			if err := statement.Close(); err != nil {
				return err
			}
			singleRow := buildUpsertSQL(table, columns, keys)
			conflictClause := singleRow[strings.Index(singleRow, " ON CONFLICT "):]
			mergeSQL := "INSERT INTO " + table.SQL() + " (" + quoteList(columns) + ") SELECT " + quoteList(columns) + " FROM perf_items_stage WHERE true" + conflictClause
			_, err = tx.ExecContext(ctx, mergeSQL)
			return err
		})
	})
}

func benchmarkUnchangedUpsert(
	b *testing.B,
	ctx context.Context,
	db *sql.DB,
	table tableName,
	columns, keys []string,
	rows [][]any,
	skipUnchanged bool,
) {
	b.Helper()
	const batchSize = 50
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		if _, err := db.ExecContext(ctx, "TRUNCATE TABLE public.perf_items"); err != nil {
			b.Fatal(err)
		}
		if err := executeBatches(ctx, db, table, columns, keys, rows, batchSize, false); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := executeBatches(ctx, db, table, columns, keys, rows, batchSize, skipUnchanged); err != nil {
			b.Fatal(err)
		}
	}
}

func executeBatches(ctx context.Context, db *sql.DB, table tableName, columns, keys []string, rows [][]any, batchSize int, skipUnchanged bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		query, args := buildBatchUpsertSQL(table, columns, keys, rows[start:end])
		if skipUnchanged {
			query += unchangedPredicate(table, columns, keys)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func unchangedPredicate(table tableName, columns, keys []string) string {
	keySet := make(map[string]bool, len(keys))
	for _, key := range keys {
		keySet[key] = true
	}
	var current, incoming []string
	for _, column := range columns {
		if keySet[column] {
			continue
		}
		quoted := quoteIdentifier(column)
		current = append(current, quoteIdentifier(table.Name)+"."+quoted)
		incoming = append(incoming, "EXCLUDED."+quoted)
	}
	return " WHERE (" + strings.Join(current, ", ") + ") IS DISTINCT FROM (" + strings.Join(incoming, ", ") + ")"
}

func benchmarkUpsert(b *testing.B, ctx context.Context, db *sql.DB, rowCount int, write func(*sql.Tx) error) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		if _, err := db.ExecContext(ctx, "TRUNCATE TABLE public.perf_items"); err != nil {
			b.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := write(tx); err != nil {
			_ = tx.Rollback()
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(rowCount*b.N)/b.Elapsed().Seconds(), "rows/s")
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.perf_items").Scan(&count); err != nil {
		b.Fatal(err)
	}
	if count != rowCount {
		b.Fatalf("row count = %d, want %d", count, rowCount)
	}
}
