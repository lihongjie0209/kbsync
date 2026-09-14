//go:build integration

package syncer

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"kingbase.com/gokb"
)

func BenchmarkKingbaseFullForeignKeyTiming(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	container := startDatabaseContainer(b, ctx)
	db := openTestDatabase(b, ctx, container.dsn)
	const rowCount = 10000

	for _, test := range []struct {
		name     string
		fkBefore bool
	}{
		{name: "foreign_key_before_copy", fkBefore: true},
		{name: "foreign_key_after_copy", fkBefore: false},
	} {
		test := test
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS public.perf_fk_child")
				_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS public.perf_fk_parent")
				b.StartTimer()
				if err := fullForeignKeyScenario(ctx, db, rowCount, test.fkBefore); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func fullForeignKeyScenario(ctx context.Context, db *sql.DB, rowCount int, fkBefore bool) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE public.perf_fk_parent (id bigint PRIMARY KEY); CREATE TABLE public.perf_fk_child (id bigint PRIMARY KEY, parent_id bigint NOT NULL)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.perf_fk_parent SELECT n FROM generate_series(1, $1) n`, rowCount); err != nil {
		return err
	}
	createForeignKey := func() error {
		_, err := db.ExecContext(ctx, `ALTER TABLE public.perf_fk_child ADD CONSTRAINT perf_fk_child_parent_fk FOREIGN KEY (parent_id) REFERENCES public.perf_fk_parent(id)`)
		return err
	}
	if fkBefore {
		if err := createForeignKey(); err != nil {
			return err
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	statement, err := tx.PrepareContext(ctx, gokb.CopyInSchema("public", "perf_fk_child", "id", "parent_id"))
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for id := 1; id <= rowCount; id++ {
		if _, err := statement.ExecContext(ctx, int64(id), int64(id)); err != nil {
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
	if !fkBefore {
		return createForeignKey()
	}
	return nil
}
