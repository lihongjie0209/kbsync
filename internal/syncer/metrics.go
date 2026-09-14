package syncer

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type tableMetric struct {
	Mode     string
	Source   string
	Target   string
	Rows     int64
	Batches  int64
	Errors   int64
	Duration time.Duration
}

type metricSet struct {
	mu     sync.Mutex
	tables map[string]*tableMetric
}

func (m *metricSet) record(mode, source, target string, rows, batches int64, duration time.Duration, failed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tables == nil {
		m.tables = make(map[string]*tableMetric)
	}
	key := mode + "\x00" + source + "\x00" + target
	metric := m.tables[key]
	if metric == nil {
		metric = &tableMetric{Mode: mode, Source: source, Target: target}
		m.tables[key] = metric
	}
	metric.Rows += rows
	metric.Batches += batches
	metric.Duration += duration
	if failed {
		metric.Errors++
	}
}

func (m *metricSet) render() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.tables))
	for key := range m.tables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var output strings.Builder
	output.WriteString("# HELP kbsync_rows_total Rows successfully synchronized in the latest process run.\n# TYPE kbsync_rows_total gauge\n")
	for _, key := range keys {
		item := m.tables[key]
		labels := fmt.Sprintf(`mode=%q,source=%q,target=%q`, item.Mode, item.Source, item.Target)
		fmt.Fprintf(&output, "kbsync_rows_total{%s} %d\n", labels, item.Rows)
		fmt.Fprintf(&output, "kbsync_batches_total{%s} %d\n", labels, item.Batches)
		fmt.Fprintf(&output, "kbsync_errors_total{%s} %d\n", labels, item.Errors)
		fmt.Fprintf(&output, "kbsync_duration_seconds{%s} %.6f\n", labels, item.Duration.Seconds())
		fmt.Fprintf(&output, "kbsync_rows_per_second{%s} %.6f\n", labels, rowsPerSecond(item.Rows, item.Duration))
	}
	return output.String()
}

func (s *Syncer) WriteMetrics(path string) error {
	if path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("创建指标目录: %w", err)
	}
	temporary, err := os.CreateTemp(dir, ".kbsync-metrics-*")
	if err != nil {
		return fmt.Errorf("创建临时指标文件: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if _, err := temporary.WriteString(s.metrics.render()); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("写入指标文件: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("关闭指标文件: %w", err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("替换指标文件: %w", err)
	}
	return nil
}
