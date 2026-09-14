package syncer

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type checkpoint struct {
	Values []string `json:"values"`
}

type state struct {
	Tables map[string]checkpoint `json:"tables"`
}

func loadState(path string) (state, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return state{Tables: make(map[string]checkpoint)}, nil
	}
	if err != nil {
		return state{}, fmt.Errorf("读取状态文件: %w", err)
	}
	var result state
	if err := json.Unmarshal(data, &result); err != nil {
		return state{}, fmt.Errorf("解析状态文件: %w", err)
	}
	if result.Tables == nil {
		result.Tables = make(map[string]checkpoint)
	}
	return result, nil
}

func saveState(path string, value state) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("编码状态文件: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("创建状态目录: %w", err)
	}
	temporary, err := os.CreateTemp(dir, ".kbsync-state-*")
	if err != nil {
		return fmt.Errorf("创建临时状态文件: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("设置状态文件权限: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("写入状态文件: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("同步状态文件: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("关闭状态文件: %w", err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("替换状态文件: %w", err)
	}
	return nil
}
