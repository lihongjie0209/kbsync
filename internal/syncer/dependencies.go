package syncer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"kbsync/internal/config"
)

const referencedTablesQuery = `
SELECT pn.nspname, parent.relname
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class child ON child.oid = con.conrelid
JOIN pg_catalog.pg_namespace cn ON cn.oid = child.relnamespace
JOIN pg_catalog.pg_class parent ON parent.oid = con.confrelid
JOIN pg_catalog.pg_namespace pn ON pn.oid = parent.relnamespace
WHERE con.contype = 'f' AND cn.nspname = $1 AND child.relname = $2`

func (s *Syncer) dependencyLevels(ctx context.Context) ([][]config.Table, error) {
	mappings := make(map[string]config.Table, len(s.config.Tables))
	order := make([]string, 0, len(s.config.Tables))
	for _, mapping := range s.config.Tables {
		table, err := parseTableName(mapping.Source)
		if err != nil {
			return nil, err
		}
		key := table.Schema + "." + table.Name
		mappings[key] = mapping
		order = append(order, key)
	}
	dependencies := make(map[string]map[string]bool, len(mappings))
	for key := range mappings {
		dependencies[key] = make(map[string]bool)
		table, _ := parseTableName(key)
		rows, err := s.source.QueryContext(ctx, referencedTablesQuery, table.Schema, table.Name)
		if err != nil {
			return nil, fmt.Errorf("读取表 %s 的外键依赖: %w", key, err)
		}
		for rows.Next() {
			var schema, name string
			if err := rows.Scan(&schema, &name); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("扫描表 %s 的外键依赖: %w", key, err)
			}
			parentKey := schema + "." + name
			if _, configured := mappings[parentKey]; configured && parentKey != key {
				dependencies[key][parentKey] = true
			}
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("关闭表 %s 的外键依赖结果: %w", key, err)
		}
	}
	return topologicalLevels(order, mappings, dependencies)
}

func topologicalLevels(order []string, mappings map[string]config.Table, dependencies map[string]map[string]bool) ([][]config.Table, error) {
	remaining := make(map[string]bool, len(mappings))
	for key := range mappings {
		remaining[key] = true
	}
	var levels [][]config.Table
	for len(remaining) > 0 {
		var level []config.Table
		var keys []string
		for _, key := range order {
			if !remaining[key] {
				continue
			}
			ready := true
			for dependency := range dependencies[key] {
				if remaining[dependency] {
					ready = false
					break
				}
			}
			if ready {
				keys = append(keys, key)
				level = append(level, mappings[key])
			}
		}
		if len(level) == 0 {
			cycle := make([]string, 0, len(remaining))
			for _, key := range order {
				if remaining[key] {
					cycle = append(cycle, key)
				}
			}
			return nil, fmt.Errorf("检测到循环外键依赖: %s；请将约束改为可延迟约束或拆分同步任务", strings.Join(cycle, ", "))
		}
		for _, key := range keys {
			delete(remaining, key)
		}
		levels = append(levels, level)
	}
	return levels, nil
}

func (s *Syncer) cleanFullTargets(ctx context.Context, levels [][]config.Table) error {
	switch s.config.FullStrategy {
	case "append":
		return nil
	case "truncate":
		var targets []string
		for _, level := range levels {
			for _, mapping := range level {
				table, err := parseTableName(mapping.Target)
				if err != nil {
					return err
				}
				targets = append(targets, table.SQL())
			}
		}
		startedAt := time.Now()
		if _, err := s.target.ExecContext(ctx, "TRUNCATE TABLE "+strings.Join(targets, ", ")); err != nil {
			return fmt.Errorf("按外键依赖统一清理目标表: %w", err)
		}
		s.log("全量清理完成 strategy=truncate tables=%d duration_ms=%d", len(targets), time.Since(startedAt).Milliseconds())
		return nil
	case "delete":
		for levelIndex := len(levels) - 1; levelIndex >= 0; levelIndex-- {
			for _, mapping := range levels[levelIndex] {
				table, err := parseTableName(mapping.Target)
				if err != nil {
					return err
				}
				if _, err := s.target.ExecContext(ctx, "DELETE FROM "+table.SQL()); err != nil {
					return fmt.Errorf("按外键依赖清理目标表 %s: %w", mapping.Target, err)
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("不支持的全量清理策略 %q", s.config.FullStrategy)
	}
}
