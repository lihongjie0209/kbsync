package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"kbsync/internal/config"
	"kingbase.com/gokb"
)

type Logger func(format string, args ...any)

type Syncer struct {
	source   *sql.DB
	target   *sql.DB
	config   config.Config
	log      Logger
	metrics  metricSet
	progress ProgressReporter
}

func Open(ctx context.Context, cfg config.Config, log Logger) (*Syncer, error) {
	source, err := openDatabase(ctx, cfg.Source)
	if err != nil {
		return nil, fmt.Errorf("连接源库: %w", err)
	}
	target, err := openDatabase(ctx, cfg.Target)
	if err != nil {
		_ = source.Close()
		return nil, fmt.Errorf("连接目标库: %w", err)
	}
	return &Syncer{source: source, target: target, config: cfg, log: log}, nil
}

func openDatabase(ctx context.Context, cfg config.Database) (*sql.DB, error) {
	db, err := sql.Open("kingbase", cfg.DSN)
	if err != nil {
		return nil, err
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.ConnMaxLifetime != "" {
		duration, err := time.ParseDuration(cfg.ConnMaxLifetime)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("无效 conn_max_lifetime: %w", err)
		}
		db.SetConnMaxLifetime(duration)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (s *Syncer) Close() error {
	return errorsJoin(s.source.Close(), s.target.Close())
}

func (s *Syncer) Validate(ctx context.Context, incremental bool) error {
	for _, mapping := range s.config.Tables {
		prepared, err := s.prepareStructure(ctx, mapping, true)
		if err != nil {
			return err
		}
		if incremental {
			if _, err := incrementalColumns(prepared.columns, prepared.primaryKey, mapping); err != nil {
				return err
			}
		}
		s.log("校验通过: %s -> %s (%d 列)", mapping.Source, mapping.Target, len(prepared.columns))
	}
	return nil
}

func (s *Syncer) Full(ctx context.Context) error {
	levels, err := s.dependencyLevels(ctx)
	if err != nil {
		return err
	}
	if s.progress != nil {
		s.progress.Begin(len(s.config.Tables))
		defer s.progress.Finish()
	}
	prepared := make(map[string]preparedTable, len(s.config.Tables))
	var preparedMu sync.Mutex
	// Ensure every referenced target table exists before data cleanup/copy.
	for _, level := range levels {
		if err := s.runMappings(ctx, level, func(ctx context.Context, mapping config.Table) error {
			item, err := s.prepareStructure(ctx, mapping, false)
			if err == nil {
				preparedMu.Lock()
				prepared[mapping.Source+"->"+mapping.Target] = item
				preparedMu.Unlock()
			}
			return err
		}); err != nil {
			return err
		}
	}
	if err := s.cleanFullTargets(ctx, levels); err != nil {
		return err
	}
	for _, level := range levels {
		if err := s.runMappings(ctx, level, func(ctx context.Context, mapping config.Table) error {
			startedAt := time.Now()
			item := prepared[mapping.Source+"->"+mapping.Target]
			err := s.fullPreparedTable(ctx, mapping, item, false)
			if err != nil {
				s.metrics.record("full", mapping.Source, mapping.Target, 0, 0, time.Since(startedAt), true)
			}
			return err
		}); err != nil {
			return err
		}
	}
	if err := s.reconcileForeignKeys(ctx); err != nil {
		return err
	}
	for _, level := range levels {
		if err := s.runMappings(ctx, level, func(ctx context.Context, mapping config.Table) error {
			return s.syncPreparedIndexes(ctx, mapping, prepared[mapping.Source+"->"+mapping.Target])
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Syncer) fullPreparedTable(ctx context.Context, mapping config.Table, prepared preparedTable, clean bool) (returnErr error) {
	startedAt := time.Now()
	sourceTable, targetTable, columns := prepared.source, prepared.target, prepared.columns
	var tableProgress TableProgress
	if s.progress != nil {
		totalRows, err := s.countRows(ctx, sourceTable, nil, nil)
		if err != nil {
			return fmt.Errorf("统计源表 %s 进度总数: %w", mapping.Source, err)
		}
		tableProgress = s.progress.Start(mapping.Source, mapping.Target, totalRows)
		defer func() { finishTableProgress(tableProgress, returnErr) }()
	}
	query := "SELECT " + quoteList(columns) + " FROM " + sourceTable.SQL()
	rows, err := s.source.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("读取源表 %s: %w", mapping.Source, err)
	}
	defer func() { returnErr = errorsJoin(returnErr, rows.Close()) }()

	tx, err := s.target.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开启目标事务 %s: %w", mapping.Target, err)
	}
	committed := false
	defer func() {
		if !committed {
			returnErr = errorsJoin(returnErr, tx.Rollback())
		}
	}()

	if clean {
		switch s.config.FullStrategy {
		case "truncate":
			_, err = tx.ExecContext(ctx, "TRUNCATE TABLE "+targetTable.SQL())
		case "delete":
			_, err = tx.ExecContext(ctx, "DELETE FROM "+targetTable.SQL())
		case "append":
			// Intentionally retain destination rows.
		}
		if err != nil {
			return fmt.Errorf("清理目标表 %s: %w", mapping.Target, err)
		}
	}

	statement, err := tx.PrepareContext(ctx, gokb.CopyInSchema(targetTable.Schema, targetTable.Name, columns...))
	if err != nil {
		return fmt.Errorf("准备 COPY %s: %w", mapping.Target, err)
	}
	defer func() { returnErr = errorsJoin(returnErr, statement.Close()) }()

	count := int64(0)
	reported := int64(0)
	progressStartedAt := time.Now()
	values, pointers := scanBuffer(len(columns))
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return fmt.Errorf("扫描源表 %s: %w", mapping.Source, err)
		}
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("写入目标表 %s 第 %d 行: %w", mapping.Target, count+1, err)
		}
		count++
		if tableProgress != nil && count-reported >= 100 {
			tableProgress.Add(count-reported, time.Since(progressStartedAt))
			reported = count
			progressStartedAt = time.Now()
		}
		if count%int64(s.config.BatchSize) == 0 {
			s.log("全量同步 %s: 已读取 %d 行", mapping.Source, count)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("遍历源表 %s: %w", mapping.Source, err)
	}
	if tableProgress != nil && count > reported {
		tableProgress.Add(count-reported, time.Since(progressStartedAt))
	}
	if _, err := statement.ExecContext(ctx); err != nil {
		return fmt.Errorf("结束 COPY %s: %w", mapping.Target, err)
	}
	if err := statement.Close(); err != nil {
		return fmt.Errorf("关闭 COPY %s: %w", mapping.Target, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交目标表 %s: %w", mapping.Target, err)
	}
	committed = true
	duration := time.Since(startedAt)
	s.metrics.record("full", mapping.Source, mapping.Target, count, 1, duration, false)
	s.log("全量同步完成 mode=full source=%s target=%s rows=%d duration_ms=%d rows_per_second=%.2f", mapping.Source, mapping.Target, count, duration.Milliseconds(), rowsPerSecond(count, duration))
	return nil
}

func (s *Syncer) Incremental(ctx context.Context) error {
	currentState, err := loadState(s.config.StateFile)
	if err != nil {
		return err
	}
	store := &checkpointStore{path: s.config.StateFile, state: currentState}
	levels, err := s.dependencyLevels(ctx)
	if err != nil {
		return err
	}
	if s.progress != nil {
		s.progress.Begin(len(s.config.Tables))
		defer s.progress.Finish()
	}
	prepared := make(map[string]preparedTable, len(s.config.Tables))
	var preparedMu sync.Mutex
	for _, level := range levels {
		if err := s.runMappings(ctx, level, func(ctx context.Context, mapping config.Table) error {
			item, err := s.prepareStructure(ctx, mapping, true)
			if err == nil {
				preparedMu.Lock()
				prepared[mapping.Source+"->"+mapping.Target] = item
				preparedMu.Unlock()
			}
			return err
		}); err != nil {
			return err
		}
	}
	if err := s.reconcileForeignKeys(ctx); err != nil {
		return err
	}
	for _, level := range levels {
		if err := s.runMappings(ctx, level, func(ctx context.Context, mapping config.Table) error {
			startedAt := time.Now()
			item := prepared[mapping.Source+"->"+mapping.Target]
			err := s.incrementalPreparedTable(ctx, mapping, item, store)
			if err != nil {
				s.metrics.record("incremental", mapping.Source, mapping.Target, 0, 0, time.Since(startedAt), true)
			}
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

type checkpointStore struct {
	mu    sync.Mutex
	path  string
	state state
	dirty bool
}

func (s *checkpointStore) get(key string) (checkpoint, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.state.Tables[key]
	return value, ok
}

func (s *checkpointStore) update(key string, value checkpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Tables[key] = value
	s.dirty = true
}

func (s *checkpointStore) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *checkpointStore) flushLocked() error {
	if !s.dirty {
		return nil
	}
	if err := saveState(s.path, s.state); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

func (s *Syncer) runMappings(ctx context.Context, mappings []config.Table, action func(context.Context, config.Table) error) error {
	parallelism := s.config.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism > len(mappings) {
		parallelism = len(mappings)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan config.Table, len(mappings))
	for _, mapping := range mappings {
		jobs <- mapping
	}
	close(jobs)
	var wg sync.WaitGroup
	var firstErr error
	var errorMu sync.Mutex
	for range parallelism {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case mapping, ok := <-jobs:
					if !ok {
						return
					}
					if err := action(ctx, mapping); err != nil {
						errorMu.Lock()
						if firstErr == nil {
							firstErr = err
							cancel()
						}
						errorMu.Unlock()
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func (s *Syncer) incrementalPreparedTable(ctx context.Context, mapping config.Table, prepared preparedTable, store *checkpointStore) (returnErr error) {
	defer func() { returnErr = errorsJoin(returnErr, store.flush()) }()
	sourceTable, targetTable, columns := prepared.source, prepared.target, prepared.columns
	orderColumns, err := incrementalColumns(columns, prepared.primaryKey, mapping)
	if err != nil {
		return err
	}
	stateKey := mapping.Source + "->" + mapping.Target
	checkpointValue, hasCheckpoint := store.get(stateKey)
	if hasCheckpoint && len(checkpointValue.Values) != len(orderColumns) {
		return fmt.Errorf("表 %s 的断点与当前 cursor/key_columns 不兼容，请删除该表断点后重试", mapping.Source)
	}
	var tableProgress TableProgress
	if s.progress != nil {
		totalRows, err := s.countRows(ctx, sourceTable, orderColumns, checkpointValue.Values)
		if err != nil {
			return fmt.Errorf("统计源表 %s 增量进度总数: %w", mapping.Source, err)
		}
		tableProgress = s.progress.Start(mapping.Source, mapping.Target, totalRows)
		defer func() { finishTableProgress(tableProgress, returnErr) }()
	}
	if len(orderColumns) == 1 {
		return s.incrementalTableWithoutKey(ctx, mapping, sourceTable, targetTable, columns, store, stateKey, checkpointValue, tableProgress)
	}

	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	total := int64(0)
	startedAt := time.Now()
	batchNumber := 0
	for {
		batchStartedAt := time.Now()
		query, args := buildIncrementalQuery(sourceTable, columns, orderColumns, checkpointValue.Values, batchSize)
		readStartedAt := time.Now()
		rows, err := s.source.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("增量读取 %s: %w", mapping.Source, err)
		}
		batch, last, err := readBatch(rows, columns, orderColumns)
		closeErr := rows.Close()
		if err != nil {
			return fmt.Errorf("读取增量批次 %s: %w", mapping.Source, err)
		}
		if closeErr != nil {
			return fmt.Errorf("关闭查询结果 %s: %w", mapping.Source, closeErr)
		}
		if len(batch) == 0 {
			break
		}
		readDuration := time.Since(readStartedAt)

		writeStartedAt := time.Now()
		tx, err := s.target.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("开启增量事务 %s: %w", mapping.Target, err)
		}
		if err := stagingUpsert(ctx, tx, targetTable, columns, orderColumns[1:], batch); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("暂存表 upsert %s: %w", mapping.Target, err)
		}
		commitStartedAt := time.Now()
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("提交增量批次 %s: %w", mapping.Target, err)
		}
		commitDuration := time.Since(commitStartedAt)
		writeDuration := time.Since(writeStartedAt)

		checkpointValue = checkpoint{Values: last}
		total += int64(len(batch))
		batchNumber++
		store.update(stateKey, checkpointValue)
		if batchNumber%checkpointEveryBatches(s.config.CheckpointBatches) == 0 {
			if err := store.flush(); err != nil {
				return err
			}
		}
		batchDuration := time.Since(batchStartedAt)
		if tableProgress != nil {
			tableProgress.Add(int64(len(batch)), batchDuration)
		}
		rate := float64(len(batch)) / batchDuration.Seconds()
		s.log("增量批次 mode=incremental source=%s target=%s batch=%d rows=%d total_rows=%d read_ms=%d write_ms=%d commit_ms=%d total_ms=%d rows_per_second=%.2f checkpoint=%v", mapping.Source, mapping.Target, batchNumber, len(batch), total, readDuration.Milliseconds(), writeDuration.Milliseconds(), commitDuration.Milliseconds(), batchDuration.Milliseconds(), rate, last)
		if len(batch) < batchSize {
			break
		}
	}
	duration := time.Since(startedAt)
	s.metrics.record("incremental", mapping.Source, mapping.Target, total, int64(batchNumber), duration, false)
	s.log("增量同步完成 mode=incremental source=%s target=%s rows=%d duration_ms=%d rows_per_second=%.2f", mapping.Source, mapping.Target, total, duration.Milliseconds(), rowsPerSecond(total, duration))
	return nil
}

func (s *Syncer) incrementalTableWithoutKey(
	ctx context.Context,
	mapping config.Table,
	sourceTable tableName,
	targetTable tableName,
	columns []string,
	store *checkpointStore,
	stateKey string,
	checkpointValue checkpoint,
	tableProgress TableProgress,
) error {
	total := int64(0)
	startedAt := time.Now()
	groups := int64(0)
	batchNumber := 0
	for {
		batchStartedAt := time.Now()
		cursorValues, err := s.nextCursorValues(ctx, sourceTable, mapping.Cursor, checkpointValue.Values, s.config.NoKeyCursorBatch)
		if err != nil {
			return fmt.Errorf("读取无主键表 %s 的下一组游标: %w", mapping.Source, err)
		}
		if len(cursorValues) == 0 {
			break
		}

		placeholders := placeholders(len(cursorValues), 1)
		cursorArgs := make([]any, len(cursorValues))
		copy(cursorArgs, cursorValues)
		selectSQL := "SELECT " + quoteList(columns) + " FROM " + sourceTable.SQL() + " WHERE " + quoteIdentifier(mapping.Cursor) + " IN (" + strings.Join(placeholders, ", ") + ")"
		rows, err := s.source.QueryContext(ctx, selectSQL, cursorArgs...)
		if err != nil {
			return fmt.Errorf("读取无主键表 %s 的游标组: %w", mapping.Source, err)
		}
		count, err := s.replaceCursorGroups(ctx, targetTable, columns, mapping.Cursor, cursorValues, rows)
		closeErr := rows.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return fmt.Errorf("关闭无主键表 %s 查询结果: %w", mapping.Source, closeErr)
		}

		checkpointValue = checkpoint{Values: []string{checkpointString(cursorValues[len(cursorValues)-1])}}
		total += count
		groups += int64(len(cursorValues))
		if tableProgress != nil {
			tableProgress.Add(count, time.Since(batchStartedAt))
		}
		batchNumber++
		store.update(stateKey, checkpointValue)
		if batchNumber%checkpointEveryBatches(s.config.CheckpointBatches) == 0 {
			if err := store.flush(); err != nil {
				return err
			}
		}
		s.log("无主键增量批次 mode=incremental_no_key source=%s target=%s cursor_groups=%d rows=%d total_rows=%d checkpoint=%s", mapping.Source, mapping.Target, len(cursorValues), count, total, checkpointValue.Values[0])
	}
	duration := time.Since(startedAt)
	s.metrics.record("incremental_no_key", mapping.Source, mapping.Target, total, groups, duration, false)
	s.log("无主键增量同步完成 mode=incremental_no_key source=%s target=%s rows=%d groups=%d duration_ms=%d rows_per_second=%.2f", mapping.Source, mapping.Target, total, groups, duration.Milliseconds(), rowsPerSecond(total, duration))
	return nil
}

func (s *Syncer) nextCursorValues(ctx context.Context, table tableName, cursor string, checkpoint []string, limit int) ([]any, error) {
	if limit <= 0 {
		limit = 50
	}
	query := "SELECT DISTINCT " + quoteIdentifier(cursor) + " FROM " + table.SQL() + " WHERE " + quoteIdentifier(cursor) + " IS NOT NULL"
	var args []any
	if len(checkpoint) > 0 {
		query += " AND " + quoteIdentifier(cursor) + " > $1"
		args = append(args, checkpoint[0])
	}
	query += " ORDER BY " + quoteIdentifier(cursor) + " LIMIT " + strconv.Itoa(limit)
	rows, err := s.source.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	values := make([]any, 0, limit)
	for rows.Next() {
		var value any
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *Syncer) countRows(ctx context.Context, table tableName, orderColumns, checkpoint []string) (int64, error) {
	query, args := buildCountQuery(table, orderColumns, checkpoint)
	var count int64
	if err := s.source.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Syncer) replaceCursorGroups(
	ctx context.Context,
	targetTable tableName,
	columns []string,
	cursor string,
	cursorValues []any,
	rows *sql.Rows,
) (count int64, returnErr error) {
	tx, err := s.target.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("开启无主键目标事务 %s: %w", targetTable.SQL(), err)
	}
	committed := false
	defer func() {
		if !committed {
			returnErr = errorsJoin(returnErr, tx.Rollback())
		}
	}()
	deleteSQL := "DELETE FROM " + targetTable.SQL() + " WHERE " + quoteIdentifier(cursor) + " IN (" + strings.Join(placeholders(len(cursorValues), 1), ", ") + ")"
	if _, err := tx.ExecContext(ctx, deleteSQL, cursorValues...); err != nil {
		return 0, fmt.Errorf("清理目标游标组 %s: %w", targetTable.SQL(), err)
	}
	statement, err := tx.PrepareContext(ctx, gokb.CopyInSchema(targetTable.Schema, targetTable.Name, columns...))
	if err != nil {
		return 0, fmt.Errorf("准备无主键 COPY %s: %w", targetTable.SQL(), err)
	}
	defer func() { returnErr = errorsJoin(returnErr, statement.Close()) }()
	values, pointers := scanBuffer(len(columns))
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return count, fmt.Errorf("扫描无主键源数据 %s: %w", targetTable.SQL(), err)
		}
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return count, fmt.Errorf("写入无主键目标表 %s: %w", targetTable.SQL(), err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("遍历无主键源数据 %s: %w", targetTable.SQL(), err)
	}
	if _, err := statement.ExecContext(ctx); err != nil {
		return count, fmt.Errorf("结束无主键 COPY %s: %w", targetTable.SQL(), err)
	}
	if err := statement.Close(); err != nil {
		return count, fmt.Errorf("关闭无主键 COPY %s: %w", targetTable.SQL(), err)
	}
	if err := tx.Commit(); err != nil {
		return count, fmt.Errorf("提交无主键游标组 %s: %w", targetTable.SQL(), err)
	}
	committed = true
	return count, nil
}

func readBatch(rows *sql.Rows, columns, orderColumns []string) ([][]any, []string, error) {
	index := make(map[string]int, len(columns))
	for i, column := range columns {
		index[column] = i
	}
	orderIndexes := make([]int, len(orderColumns))
	for i, column := range orderColumns {
		orderIndexes[i] = index[column]
	}
	var batch [][]any
	last := make([]string, len(orderColumns))
	for rows.Next() {
		values, pointers := scanBuffer(len(columns))
		if err := rows.Scan(pointers...); err != nil {
			return nil, nil, err
		}
		for i, valueIndex := range orderIndexes {
			last[i] = checkpointString(values[valueIndex])
		}
		batch = append(batch, values)
	}
	return batch, last, rows.Err()
}

func checkpointString(value any) string {
	switch typed := value.(type) {
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	case []byte:
		return string(typed)
	case nil:
		return ""
	default:
		return fmt.Sprint(typed)
	}
}

func scanBuffer(size int) ([]any, []any) {
	values := make([]any, size)
	pointers := make([]any, size)
	for i := range values {
		pointers[i] = &values[i]
	}
	return values, pointers
}

func checkpointEveryBatches(configured int) int {
	if configured <= 0 {
		return 10
	}
	return configured
}

func effectiveBatchSize(configured, maxParameters, columnCount int) int {
	if configured <= 0 {
		configured = 1
	}
	if maxParameters <= 0 {
		maxParameters = 30000
	}
	if columnCount <= 0 {
		return configured
	}
	byParameters := maxParameters / columnCount
	if byParameters < 1 {
		return 1
	}
	if configured > byParameters {
		return byParameters
	}
	return configured
}

func rowsPerSecond(rows int64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(rows) / duration.Seconds()
}

func buildIncrementalQuery(table tableName, columns, orderColumns, checkpoint []string, limit int) (string, []any) {
	query := "SELECT " + quoteList(columns) + " FROM " + table.SQL() + " WHERE " + quoteIdentifier(orderColumns[0]) + " IS NOT NULL"
	var args []any
	if len(checkpoint) > 0 {
		placeholders := make([]string, len(checkpoint))
		for i, value := range checkpoint {
			placeholders[i] = "$" + strconv.Itoa(i+1)
			args = append(args, value)
		}
		query += " AND (" + quoteList(orderColumns) + ") > (" + strings.Join(placeholders, ", ") + ")"
	}
	query += " ORDER BY " + quoteList(orderColumns) + " LIMIT " + strconv.Itoa(limit)
	return query, args
}

func buildCountQuery(table tableName, orderColumns, checkpoint []string) (string, []any) {
	query := "SELECT count(*) FROM " + table.SQL()
	if len(orderColumns) == 0 {
		return query, nil
	}
	query += " WHERE " + quoteIdentifier(orderColumns[0]) + " IS NOT NULL"
	var args []any
	if len(checkpoint) > 0 {
		values := make([]string, len(checkpoint))
		for i, value := range checkpoint {
			values[i] = "$" + strconv.Itoa(i+1)
			args = append(args, value)
		}
		query += " AND (" + quoteList(orderColumns) + ") > (" + strings.Join(values, ", ") + ")"
	}
	return query, args
}

func placeholders(count, start int) []string {
	result := make([]string, count)
	for i := range count {
		result[i] = "$" + strconv.Itoa(start+i)
	}
	return result
}

func buildUpsertSQL(table tableName, columns, keyColumns []string) string {
	placeholders := make([]string, len(columns))
	keySet := make(map[string]bool, len(keyColumns))
	for _, key := range keyColumns {
		keySet[key] = true
	}
	updates := make([]string, 0, len(columns))
	for i, column := range columns {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		if !keySet[column] {
			quoted := quoteIdentifier(column)
			updates = append(updates, quoted+" = EXCLUDED."+quoted)
		}
	}
	query := "INSERT INTO " + table.SQL() + " (" + quoteList(columns) + ") VALUES (" + strings.Join(placeholders, ", ") + ") ON CONFLICT (" + quoteList(keyColumns) + ") "
	if len(updates) == 0 {
		return query + "DO NOTHING"
	}
	return query + "DO UPDATE SET " + strings.Join(updates, ", ")
}

func buildBatchUpsertSQL(table tableName, columns, keyColumns []string, rows [][]any) (string, []any) {
	valueGroups := make([]string, len(rows))
	args := make([]any, 0, len(rows)*len(columns))
	parameter := 1
	for rowIndex, values := range rows {
		placeholders := make([]string, len(columns))
		for columnIndex := range columns {
			placeholders[columnIndex] = "$" + strconv.Itoa(parameter)
			parameter++
		}
		valueGroups[rowIndex] = "(" + strings.Join(placeholders, ", ") + ")"
		args = append(args, values...)
	}
	singleRow := buildUpsertSQL(table, columns, keyColumns)
	conflictIndex := strings.Index(singleRow, " ON CONFLICT ")
	conflictClause := singleRow[conflictIndex:]
	query := "INSERT INTO " + table.SQL() + " (" + quoteList(columns) + ") VALUES " + strings.Join(valueGroups, ", ") + conflictClause
	return query, args
}

func stagingUpsert(ctx context.Context, tx *sql.Tx, target tableName, columns, keyColumns []string, rows [][]any) (returnErr error) {
	const stagingTable = "kbsync_stage"
	createSQL := "CREATE TEMP TABLE " + quoteIdentifier(stagingTable) + " (LIKE " + target.SQL() + " INCLUDING DEFAULTS) ON COMMIT DROP"
	if _, err := tx.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("创建临时暂存表: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, gokb.CopyIn(stagingTable, columns...))
	if err != nil {
		return fmt.Errorf("准备暂存表 COPY: %w", err)
	}
	defer func() { returnErr = errorsJoin(returnErr, statement.Close()) }()
	for _, values := range rows {
		if _, err := statement.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("写入临时暂存表: %w", err)
		}
	}
	if _, err := statement.ExecContext(ctx); err != nil {
		return fmt.Errorf("结束暂存表 COPY: %w", err)
	}
	if err := statement.Close(); err != nil {
		return fmt.Errorf("关闭暂存表 COPY: %w", err)
	}
	singleRow := buildUpsertSQL(target, columns, keyColumns)
	conflictClause := singleRow[strings.Index(singleRow, " ON CONFLICT "):]
	mergeSQL := "INSERT INTO " + target.SQL() + " (" + quoteList(columns) + ") SELECT " + quoteList(columns) + " FROM " + quoteIdentifier(stagingTable) + " WHERE true" + conflictClause
	if _, err := tx.ExecContext(ctx, mergeSQL); err != nil {
		return fmt.Errorf("合并临时暂存表: %w", err)
	}
	return nil
}

func quoteList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = quoteIdentifier(value)
	}
	return strings.Join(quoted, ", ")
}

func errorsJoin(values ...error) error {
	var result error
	for _, value := range values {
		if value == nil || value == sql.ErrTxDone || value == io.EOF {
			continue
		}
		if result == nil {
			result = value
		} else {
			result = fmt.Errorf("%v; %w", result, value)
		}
	}
	return result
}
