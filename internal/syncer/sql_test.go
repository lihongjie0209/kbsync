package syncer

import (
	"reflect"
	"testing"
)

func TestParseTableName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		want    tableName
		wantErr bool
	}{
		{name: "qualified", input: "sales.orders", want: tableName{Schema: "sales", Name: "orders"}},
		{name: "default schema", input: "users", want: tableName{Schema: "public", Name: "users"}},
		{name: "reject injection", input: "public.users;DROP TABLE x", wantErr: true},
		{name: "reject too many parts", input: "db.public.users", wantErr: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseTableName(test.input)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseTableName() error = %v, wantErr %v", err, test.wantErr)
			}
			if !test.wantErr && got != test.want {
				t.Fatalf("parseTableName() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestBuildIncrementalQuery(t *testing.T) {
	t.Parallel()
	query, args := buildIncrementalQuery(
		tableName{Schema: "public", Name: "users"},
		[]string{"id", "updated_at", "name"},
		[]string{"updated_at", "id"},
		[]string{"2026-01-01T00:00:00Z", "42"},
		500,
	)
	wantQuery := `SELECT "id", "updated_at", "name" FROM "public"."users" WHERE "updated_at" IS NOT NULL AND ("updated_at", "id") > ($1, $2) ORDER BY "updated_at", "id" LIMIT 500`
	if query != wantQuery {
		t.Fatalf("query = %q, want %q", query, wantQuery)
	}
	wantArgs := []any{"2026-01-01T00:00:00Z", "42"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", args, wantArgs)
	}
}

func TestBuildUpsertSQL(t *testing.T) {
	t.Parallel()
	got := buildUpsertSQL(
		tableName{Schema: "archive", Name: "users"},
		[]string{"id", "name", "updated_at"},
		[]string{"id"},
	)
	want := `INSERT INTO "archive"."users" ("id", "name", "updated_at") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "updated_at" = EXCLUDED."updated_at"`
	if got != want {
		t.Fatalf("buildUpsertSQL() = %q, want %q", got, want)
	}
}

func TestBuildUpsertSQLCompositeKey(t *testing.T) {
	t.Parallel()
	got := buildUpsertSQL(
		tableName{Schema: "archive", Name: "orders"},
		[]string{"tenant_id", "order_id", "status", "updated_at"},
		[]string{"tenant_id", "order_id"},
	)
	want := `INSERT INTO "archive"."orders" ("tenant_id", "order_id", "status", "updated_at") VALUES ($1, $2, $3, $4) ON CONFLICT ("tenant_id", "order_id") DO UPDATE SET "status" = EXCLUDED."status", "updated_at" = EXCLUDED."updated_at"`
	if got != want {
		t.Fatalf("buildUpsertSQL() = %q, want %q", got, want)
	}
}

func TestBuildBatchUpsertSQL(t *testing.T) {
	t.Parallel()
	query, args := buildBatchUpsertSQL(
		tableName{Schema: "public", Name: "users"},
		[]string{"id", "name"},
		[]string{"id"},
		[][]any{{int64(1), "alice"}, {int64(2), "bob"}},
	)
	wantQuery := `INSERT INTO "public"."users" ("id", "name") VALUES ($1, $2), ($3, $4) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"`
	if query != wantQuery {
		t.Fatalf("query = %q, want %q", query, wantQuery)
	}
	wantArgs := []any{int64(1), "alice", int64(2), "bob"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", args, wantArgs)
	}
}

func TestEffectiveBatchSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                     string
		configured, params, cols int
		want                     int
	}{
		{name: "configured limit", configured: 1000, params: 30000, cols: 10, want: 1000},
		{name: "parameter limit", configured: 1000, params: 30000, cols: 100, want: 300},
		{name: "at least one", configured: 1000, params: 10, cols: 100, want: 1},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := effectiveBatchSize(test.configured, test.params, test.cols); got != test.want {
				t.Fatalf("effectiveBatchSize() = %d, want %d", got, test.want)
			}
		})
	}
}

func BenchmarkBuildBatchUpsertSQL(b *testing.B) {
	columns := []string{"id", "tenant_id", "name", "status", "updated_at"}
	rows := make([][]any, 1000)
	for i := range rows {
		rows[i] = []any{i, 1, "name", "ready", "2026-09-14T00:00:00Z"}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = buildBatchUpsertSQL(tableName{Schema: "public", Name: "items"}, columns, []string{"id"}, rows)
	}
}
