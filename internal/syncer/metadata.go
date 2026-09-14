package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"kbsync/internal/config"
)

type column struct {
	Name       string
	Type       string
	Nullable   bool
	DefaultSQL sql.NullString
}

type index struct {
	Name   string
	Unique bool
	Suffix string
}

type sequence struct {
	Schema      string
	Name        string
	IncrementBy int64
	MinValue    int64
	MaxValue    int64
	StartValue  int64
	CacheSize   int64
	Cycle       bool
	LastValue   int64
	IsCalled    bool
	OwnedColumn string
}

type tableMetadata struct {
	Columns    []column
	PrimaryKey []string
	Indexes    []index
	Sequences  []sequence
}

type preparedTable struct {
	source        tableName
	target        tableName
	columns       []string
	primaryKey    []string
	sourceIndexes []index
	targetIndexes []index
}

const columnsQuery = `
SELECT a.attname,
       pg_catalog.format_type(a.atttypid, a.atttypmod),
       NOT a.attnotnull,
       pg_catalog.pg_get_expr(d.adbin, d.adrelid)
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = $1 AND c.relname = $2
  AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`

const primaryKeyQuery = `
SELECT a.attname
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN unnest(con.conkey) WITH ORDINALITY AS key(attnum, ord) ON true
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = key.attnum
WHERE n.nspname = $1 AND c.relname = $2 AND con.contype = 'p'
ORDER BY key.ord`

const indexesQuery = `
SELECT idx.relname, i.indisunique,
       substring(pg_catalog.pg_get_indexdef(i.indexrelid) from ' USING .*$')
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class tbl ON tbl.oid = i.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = tbl.relnamespace
JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid
WHERE n.nspname = $1 AND tbl.relname = $2 AND NOT i.indisprimary
ORDER BY idx.relname`

const ownedSequencesQuery = `
SELECT sn.nspname, seq.relname,
       s.seqincrement, s.seqmin, s.seqmax, s.seqstart, s.seqcache, s.seqcycle
       , a.attname
FROM pg_catalog.pg_class tbl
JOIN pg_catalog.pg_namespace tn ON tn.oid = tbl.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = tbl.oid AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_catalog.pg_depend dep ON dep.refobjid = tbl.oid AND dep.refobjsubid = a.attnum AND dep.deptype IN ('a', 'i')
JOIN pg_catalog.pg_class seq ON seq.oid = dep.objid AND seq.relkind = 'S'
JOIN pg_catalog.pg_namespace sn ON sn.oid = seq.relnamespace
JOIN pg_catalog.pg_sequence s ON s.seqrelid = seq.oid
WHERE tn.nspname = $1 AND tbl.relname = $2
ORDER BY sn.nspname, seq.relname`

func inspectTable(ctx context.Context, db *sql.DB, table tableName) (tableMetadata, bool, error) {
	rows, err := db.QueryContext(ctx, columnsQuery, table.Schema, table.Name)
	if err != nil {
		return tableMetadata{}, false, err
	}
	var result tableMetadata
	for rows.Next() {
		var item column
		if err := rows.Scan(&item.Name, &item.Type, &item.Nullable, &item.DefaultSQL); err != nil {
			_ = rows.Close()
			return tableMetadata{}, false, err
		}
		result.Columns = append(result.Columns, item)
	}
	if err := rows.Close(); err != nil {
		return tableMetadata{}, false, err
	}
	if len(result.Columns) == 0 {
		return result, false, nil
	}
	result.PrimaryKey, err = queryStrings(ctx, db, primaryKeyQuery, table.Schema, table.Name)
	if err != nil {
		return tableMetadata{}, false, err
	}
	result.Indexes, err = inspectIndexes(ctx, db, table)
	if err != nil {
		return tableMetadata{}, false, err
	}
	result.Sequences, err = inspectSequences(ctx, db, table)
	if err != nil {
		return tableMetadata{}, false, err
	}
	// Kingbase identity columns may expose no pg_attrdef expression. Preserve
	// their insert behavior by reconstructing nextval from the owned sequence.
	for _, ownedSequence := range result.Sequences {
		for i := range result.Columns {
			if result.Columns[i].Name == ownedSequence.OwnedColumn && !result.Columns[i].DefaultSQL.Valid {
				result.Columns[i].DefaultSQL = sql.NullString{
					String: fmt.Sprintf("nextval('%s.%s'::regclass)", ownedSequence.Schema, ownedSequence.Name),
					Valid:  true,
				}
			}
		}
	}
	return result, true, nil
}

func queryStrings(ctx context.Context, db *sql.DB, query string, args ...any) ([]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func inspectIndexes(ctx context.Context, db *sql.DB, table tableName) ([]index, error) {
	rows, err := db.QueryContext(ctx, indexesQuery, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []index
	for rows.Next() {
		var item index
		if err := rows.Scan(&item.Name, &item.Unique, &item.Suffix); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func inspectSequences(ctx context.Context, db *sql.DB, table tableName) ([]sequence, error) {
	rows, err := db.QueryContext(ctx, ownedSequencesQuery, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}
	var result []sequence
	for rows.Next() {
		var item sequence
		if err := rows.Scan(&item.Schema, &item.Name, &item.IncrementBy, &item.MinValue, &item.MaxValue, &item.StartValue, &item.CacheSize, &item.Cycle, &item.OwnedColumn); err != nil {
			_ = rows.Close()
			return nil, err
		}
		result = append(result, item)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for i := range result {
		valueQuery := "SELECT last_value, is_called FROM " + (tableName{Schema: result[i].Schema, Name: result[i].Name}).SQL()
		if err := db.QueryRowContext(ctx, valueQuery).Scan(&result[i].LastValue, &result[i].IsCalled); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *Syncer) ensureStructure(ctx context.Context, mapping config.Table, syncIndexes bool) (tableName, tableName, []string, error) {
	prepared, err := s.prepareStructure(ctx, mapping, syncIndexes)
	if err != nil {
		return tableName{}, tableName{}, nil, err
	}
	return prepared.source, prepared.target, prepared.columns, nil
}

func (s *Syncer) prepareStructure(ctx context.Context, mapping config.Table, syncIndexes bool) (preparedTable, error) {
	sourceTable, err := parseTableName(mapping.Source)
	if err != nil {
		return preparedTable{}, err
	}
	targetTable, err := parseTableName(mapping.Target)
	if err != nil {
		return preparedTable{}, err
	}
	sourceMeta, exists, err := inspectTable(ctx, s.source, sourceTable)
	if err != nil {
		return preparedTable{}, fmt.Errorf("读取源表结构 %s: %w", mapping.Source, err)
	}
	if !exists {
		return preparedTable{}, fmt.Errorf("源表 %s 不存在", mapping.Source)
	}
	if _, err := s.target.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdentifier(targetTable.Schema)); err != nil {
		return preparedTable{}, fmt.Errorf("创建目标 schema %s: %w", targetTable.Schema, err)
	}
	targetMeta, targetExists, err := inspectTable(ctx, s.target, targetTable)
	if err != nil {
		return preparedTable{}, fmt.Errorf("读取目标表结构 %s: %w", mapping.Target, err)
	}
	if err := s.syncSequences(ctx, sourceTable, targetTable, sourceMeta.Sequences, false); err != nil {
		return preparedTable{}, err
	}
	if !targetExists {
		if err := s.createTable(ctx, sourceTable, targetTable, sourceMeta); err != nil {
			return preparedTable{}, err
		}
		s.log("已创建目标表: %s", mapping.Target)
	} else if err := s.reconcileTable(ctx, sourceTable, targetTable, sourceMeta, targetMeta); err != nil {
		return preparedTable{}, err
	}
	if err := s.syncSequences(ctx, sourceTable, targetTable, sourceMeta.Sequences, true); err != nil {
		return preparedTable{}, err
	}
	if syncIndexes {
		if err := s.reconcileIndexes(ctx, targetTable, sourceMeta.Indexes, targetMeta.Indexes); err != nil {
			return preparedTable{}, err
		}
	}
	columns := make([]string, len(sourceMeta.Columns))
	for i, item := range sourceMeta.Columns {
		columns[i] = item.Name
	}
	return preparedTable{
		source:        sourceTable,
		target:        targetTable,
		columns:       columns,
		primaryKey:    append([]string(nil), sourceMeta.PrimaryKey...),
		sourceIndexes: append([]index(nil), sourceMeta.Indexes...),
		targetIndexes: append([]index(nil), targetMeta.Indexes...),
	}, nil
}

func (s *Syncer) syncPreparedIndexes(ctx context.Context, mapping config.Table, prepared preparedTable) error {
	if err := s.reconcileIndexes(ctx, prepared.target, prepared.sourceIndexes, prepared.targetIndexes); err != nil {
		return err
	}
	return nil
}

func (s *Syncer) createTable(ctx context.Context, sourceTable, targetTable tableName, meta tableMetadata) error {
	definitions := make([]string, 0, len(meta.Columns)+1)
	for _, item := range meta.Columns {
		definitions = append(definitions, columnDefinition(item, sourceTable.Schema, targetTable.Schema))
	}
	if len(meta.PrimaryKey) > 0 {
		definitions = append(definitions, "PRIMARY KEY ("+quoteList(meta.PrimaryKey)+")")
	}
	query := "CREATE TABLE " + targetTable.SQL() + " (" + strings.Join(definitions, ", ") + ")"
	if _, err := s.target.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("创建目标表 %s: %w", targetTable.SQL(), err)
	}
	return nil
}

func (s *Syncer) reconcileTable(ctx context.Context, sourceTable, targetTable tableName, source, target tableMetadata) error {
	targetColumns := make(map[string]column, len(target.Columns))
	for _, item := range target.Columns {
		targetColumns[item.Name] = item
	}
	for _, wanted := range source.Columns {
		current, ok := targetColumns[wanted.Name]
		if !ok {
			query := "ALTER TABLE " + targetTable.SQL() + " ADD COLUMN " + columnDefinition(wanted, sourceTable.Schema, targetTable.Schema)
			if _, err := s.target.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("新增目标列 %s.%s: %w", targetTable.SQL(), wanted.Name, err)
			}
			s.log("已新增目标列: %s.%s", targetTable.SQL(), wanted.Name)
			continue
		}
		if current.Type != wanted.Type {
			query := "ALTER TABLE " + targetTable.SQL() + " ALTER COLUMN " + quoteIdentifier(wanted.Name) + " TYPE " + wanted.Type + " USING " + quoteIdentifier(wanted.Name) + "::" + wanted.Type
			if _, err := s.target.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("变更目标列类型 %s.%s (%s -> %s): %w", targetTable.SQL(), wanted.Name, current.Type, wanted.Type, err)
			}
		}
		wantedDefault := rewriteDefault(wanted.DefaultSQL.String, sourceTable.Schema, targetTable.Schema)
		currentDefault := current.DefaultSQL.String
		if wanted.DefaultSQL.Valid != current.DefaultSQL.Valid || (wanted.DefaultSQL.Valid && normalizeSQL(wantedDefault) != normalizeSQL(currentDefault)) {
			action := " DROP DEFAULT"
			if wanted.DefaultSQL.Valid {
				action = " SET DEFAULT " + wantedDefault
			}
			if _, err := s.target.ExecContext(ctx, "ALTER TABLE "+targetTable.SQL()+" ALTER COLUMN "+quoteIdentifier(wanted.Name)+action); err != nil {
				return fmt.Errorf("同步目标列默认值 %s.%s: %w", targetTable.SQL(), wanted.Name, err)
			}
		}
		if current.Nullable != wanted.Nullable {
			action := " SET NOT NULL"
			if wanted.Nullable {
				action = " DROP NOT NULL"
			}
			if _, err := s.target.ExecContext(ctx, "ALTER TABLE "+targetTable.SQL()+" ALTER COLUMN "+quoteIdentifier(wanted.Name)+action); err != nil {
				return fmt.Errorf("同步目标列空值约束 %s.%s: %w", targetTable.SQL(), wanted.Name, err)
			}
		}
	}
	if strings.Join(source.PrimaryKey, "\x00") != strings.Join(target.PrimaryKey, "\x00") {
		return fmt.Errorf("目标表 %s 的主键与源表不一致（源=%v，目标=%v），为避免破坏数据请人工处理", targetTable.SQL(), source.PrimaryKey, target.PrimaryKey)
	}
	return nil
}

func columnDefinition(item column, sourceSchema, targetSchema string) string {
	result := quoteIdentifier(item.Name) + " " + item.Type
	if item.DefaultSQL.Valid {
		result += " DEFAULT " + rewriteDefault(item.DefaultSQL.String, sourceSchema, targetSchema)
	}
	if !item.Nullable {
		result += " NOT NULL"
	}
	return result
}

func rewriteDefault(value, sourceSchema, targetSchema string) string {
	value = strings.ReplaceAll(value, "'"+sourceSchema+".", "'"+targetSchema+".")
	value = strings.ReplaceAll(value, `'"`+sourceSchema+`".`, `'"`+targetSchema+`".`)
	return value
}

func normalizeSQL(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(value)), " ")
}

func (s *Syncer) reconcileIndexes(ctx context.Context, target tableName, wanted, current []index) error {
	existing := make(map[string]index, len(current))
	for _, item := range current {
		existing[item.Name] = item
	}
	for _, item := range wanted {
		currentItem, ok := existing[item.Name]
		if ok {
			if currentItem.Unique != item.Unique || normalizeSQL(currentItem.Suffix) != normalizeSQL(item.Suffix) {
				return fmt.Errorf("目标索引 %s.%s 与源索引定义不一致，请人工处理", target.Schema, item.Name)
			}
			continue
		}
		unique := ""
		if item.Unique {
			unique = "UNIQUE "
		}
		query := "CREATE " + unique + "INDEX " + quoteIdentifier(item.Name) + " ON " + target.SQL() + item.Suffix
		if _, err := s.target.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("创建目标索引 %s.%s: %w", target.Schema, item.Name, err)
		}
		s.log("已创建目标索引: %s.%s", target.Schema, item.Name)
	}
	return nil
}

func (s *Syncer) syncSequences(ctx context.Context, sourceTable, targetTable tableName, sequences []sequence, setOwnership bool) error {
	seen := make(map[string]bool)
	for _, item := range sequences {
		targetSequence := tableName{Schema: targetTable.Schema, Name: item.Name}
		key := targetSequence.SQL()
		if seen[key] {
			continue
		}
		seen[key] = true
		cycle := "NO CYCLE"
		if item.Cycle {
			cycle = "CYCLE"
		}
		create := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s INCREMENT BY %d MINVALUE %d MAXVALUE %d START WITH %d CACHE %d %s", targetSequence.SQL(), item.IncrementBy, item.MinValue, item.MaxValue, item.StartValue, item.CacheSize, cycle)
		if _, err := s.target.ExecContext(ctx, create); err != nil {
			return fmt.Errorf("创建目标序列 %s: %w", targetSequence.SQL(), err)
		}
		alter := fmt.Sprintf("ALTER SEQUENCE %s INCREMENT BY %d MINVALUE %d MAXVALUE %d CACHE %d %s", targetSequence.SQL(), item.IncrementBy, item.MinValue, item.MaxValue, item.CacheSize, cycle)
		if _, err := s.target.ExecContext(ctx, alter); err != nil {
			return fmt.Errorf("同步目标序列结构 %s: %w", targetSequence.SQL(), err)
		}
		if _, err := s.target.ExecContext(ctx, "SELECT pg_catalog.setval($1::regclass, $2, $3)", targetTable.Schema+"."+item.Name, item.LastValue, item.IsCalled); err != nil {
			return fmt.Errorf("同步目标序列值 %s: %w", targetSequence.SQL(), err)
		}
		if setOwnership {
			ownedBy := "ALTER SEQUENCE " + targetSequence.SQL() + " OWNED BY " + targetTable.SQL() + "." + quoteIdentifier(item.OwnedColumn)
			if _, err := s.target.ExecContext(ctx, ownedBy); err != nil {
				return fmt.Errorf("设置目标序列归属 %s: %w", targetSequence.SQL(), err)
			}
		}
	}
	return nil
}
