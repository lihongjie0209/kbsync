package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type foreignKey struct {
	Name          string
	Parent        tableName
	Columns       []string
	ParentColumns []string
	UpdateAction  string
	DeleteAction  string
	MatchType     string
	Deferrable    bool
	Initially     bool
}

const foreignKeysQuery = `
SELECT con.conname, pn.nspname, parent.relname,
       child_attr.attname, parent_attr.attname,
       con.confupdtype::text, con.confdeltype::text, con.confmatchtype::text,
       con.condeferrable, con.condeferred
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class child ON child.oid = con.conrelid
JOIN pg_catalog.pg_namespace cn ON cn.oid = child.relnamespace
JOIN pg_catalog.pg_class parent ON parent.oid = con.confrelid
JOIN pg_catalog.pg_namespace pn ON pn.oid = parent.relnamespace
JOIN unnest(con.conkey) WITH ORDINALITY child_key(attnum, ord) ON true
JOIN unnest(con.confkey) WITH ORDINALITY parent_key(attnum, ord) ON parent_key.ord = child_key.ord
JOIN pg_catalog.pg_attribute child_attr ON child_attr.attrelid = child.oid AND child_attr.attnum = child_key.attnum
JOIN pg_catalog.pg_attribute parent_attr ON parent_attr.attrelid = parent.oid AND parent_attr.attnum = parent_key.attnum
WHERE con.contype = 'f' AND cn.nspname = $1 AND child.relname = $2
ORDER BY con.conname, child_key.ord`

func inspectForeignKeys(ctx context.Context, db *sql.DB, table tableName) ([]foreignKey, error) {
	rows, err := db.QueryContext(ctx, foreignKeysQuery, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []foreignKey
	var current *foreignKey
	for rows.Next() {
		var name, parentSchema, parentName, columnName, parentColumn, updateCode, deleteCode, matchCode string
		var deferrable, initially bool
		if err := rows.Scan(&name, &parentSchema, &parentName, &columnName, &parentColumn, &updateCode, &deleteCode, &matchCode, &deferrable, &initially); err != nil {
			return nil, err
		}
		if current == nil || current.Name != name {
			result = append(result, foreignKey{Name: name, Parent: tableName{Schema: parentSchema, Name: parentName}, UpdateAction: updateCode, DeleteAction: deleteCode, MatchType: matchCode, Deferrable: deferrable, Initially: initially})
			current = &result[len(result)-1]
		}
		current.Columns = append(current.Columns, columnName)
		current.ParentColumns = append(current.ParentColumns, parentColumn)
	}
	return result, rows.Err()
}

func (s *Syncer) reconcileForeignKeys(ctx context.Context) error {
	targetBySource := make(map[string]tableName, len(s.config.Tables))
	for _, mapping := range s.config.Tables {
		source, err := parseTableName(mapping.Source)
		if err != nil {
			return err
		}
		target, err := parseTableName(mapping.Target)
		if err != nil {
			return err
		}
		targetBySource[source.Schema+"."+source.Name] = target
	}
	for _, mapping := range s.config.Tables {
		source, _ := parseTableName(mapping.Source)
		target, _ := parseTableName(mapping.Target)
		foreignKeys, err := inspectForeignKeys(ctx, s.source, source)
		if err != nil {
			return fmt.Errorf("读取源表外键 %s: %w", mapping.Source, err)
		}
		existing, err := existingForeignKeyNames(ctx, s.target, target)
		if err != nil {
			return fmt.Errorf("读取目标表外键 %s: %w", mapping.Target, err)
		}
		for _, item := range foreignKeys {
			if existing[item.Name] {
				continue
			}
			parentTarget, configured := targetBySource[item.Parent.Schema+"."+item.Parent.Name]
			if !configured {
				s.log("跳过外部表外键 constraint=%s source=%s referenced=%s.%s reason=referenced_table_not_configured", item.Name, mapping.Source, item.Parent.Schema, item.Parent.Name)
				continue
			}
			definition := "FOREIGN KEY (" + quoteList(item.Columns) + ") REFERENCES " + parentTarget.SQL() + " (" + quoteList(item.ParentColumns) + ")"
			definition += foreignKeyOptions(item)
			query := "ALTER TABLE " + target.SQL() + " ADD CONSTRAINT " + quoteIdentifier(item.Name) + " " + definition
			if _, err := s.target.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("创建目标外键 %s.%s: %w", target.SQL(), item.Name, err)
			}
			s.log("已创建目标外键 constraint=%s source=%s target=%s referenced_target=%s", item.Name, mapping.Source, mapping.Target, parentTarget.SQL())
		}
	}
	return nil
}

func existingForeignKeyNames(ctx context.Context, db *sql.DB, table tableName) (map[string]bool, error) {
	values, err := queryStrings(ctx, db, `
SELECT con.conname
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE con.contype='f' AND n.nspname=$1 AND c.relname=$2`, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result, nil
}

func foreignKeyOptions(item foreignKey) string {
	actions := map[string]string{"a": "NO ACTION", "r": "RESTRICT", "c": "CASCADE", "n": "SET NULL", "d": "SET DEFAULT"}
	var result strings.Builder
	switch item.MatchType {
	case "f":
		result.WriteString(" MATCH FULL")
	case "p":
		result.WriteString(" MATCH PARTIAL")
	}
	if action := actions[item.UpdateAction]; action != "" && action != "NO ACTION" {
		result.WriteString(" ON UPDATE ")
		result.WriteString(action)
	}
	if action := actions[item.DeleteAction]; action != "" && action != "NO ACTION" {
		result.WriteString(" ON DELETE ")
		result.WriteString(action)
	}
	if item.Deferrable {
		result.WriteString(" DEFERRABLE")
		if item.Initially {
			result.WriteString(" INITIALLY DEFERRED")
		}
	}
	return result.String()
}
