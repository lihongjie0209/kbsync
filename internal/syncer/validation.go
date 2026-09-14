package syncer

import (
	"context"
	"fmt"
	"strings"

	"kbsync/internal/config"
)

func (s *Syncer) tableMetadata(ctx context.Context, mapping config.Table) (tableName, tableName, []string, error) {
	return s.ensureStructure(ctx, mapping, true)
}

func incrementalColumns(columns, primaryKey []string, mapping config.Table) ([]string, error) {
	if mapping.Cursor == "" || !validIdentifier(mapping.Cursor) {
		return nil, fmt.Errorf("表 %s 的 cursor 未配置或不是有效列名", mapping.Source)
	}
	available := make(map[string]bool, len(columns))
	for _, item := range columns {
		available[item] = true
	}
	if !available[mapping.Cursor] {
		return nil, fmt.Errorf("表 %s 不存在游标列 %s", mapping.Source, mapping.Cursor)
	}
	keys := append([]string(nil), mapping.KeyColumns...)
	if len(keys) == 0 {
		keys = append(keys, primaryKey...)
	}
	if len(keys) == 0 {
		return []string{mapping.Cursor}, nil
	}
	seen := map[string]bool{mapping.Cursor: true}
	order := []string{mapping.Cursor}
	for _, key := range keys {
		if !validIdentifier(key) || !available[key] {
			return nil, fmt.Errorf("表 %s 的 key_columns 包含无效列 %q", mapping.Source, key)
		}
		if !seen[key] {
			order = append(order, key)
			seen[key] = true
		}
	}
	// ON CONFLICT must use the actual configured/source primary key, not the
	// cursor-prefixed ordering tuple. Keep it after the cursor for the caller.
	result := []string{mapping.Cursor}
	result = append(result, keys...)
	if strings.Join(result, "\x00") == "" {
		return nil, fmt.Errorf("无法确定增量排序列")
	}
	return result, nil
}
