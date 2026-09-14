package progressui

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"kbsync/internal/syncer"
)

type Reporter struct {
	progress      *mpb.Progress
	queued        atomic.Int64
	running       atomic.Int64
	completed     atomic.Int64
	failed        atomic.Int64
	tableCount    atomic.Int64
	totalRows     atomic.Int64
	completedRows atomic.Int64
	overallBar    *mpb.Bar
	overallMu     sync.Mutex
	queueBar      *mpb.Bar
	once          sync.Once
}

func New(output io.Writer, force bool) *Reporter {
	options := []mpb.ContainerOption{
		mpb.WithOutput(output),
		mpb.WithRefreshRate(120 * time.Millisecond),
	}
	if force {
		options = append(options, mpb.WithAutoRefresh())
	}
	return &Reporter{progress: mpb.New(options...)}
}

// LogWriter prints log lines above active bars without corrupting the dynamic
// terminal region.
func (r *Reporter) LogWriter() io.Writer {
	return r.progress
}

func (r *Reporter) Begin(tableCount int, totalRows int64) {
	r.tableCount.Store(int64(tableCount))
	r.queued.Store(int64(tableCount))
	r.totalRows.Store(totalRows)
	r.overallBar = r.progress.AddBar(max(totalRows, 1),
		mpb.PrependDecorators(
			decor.Name("整体 "),
			decor.Percentage(decor.WCSyncSpace),
		),
		mpb.AppendDecorators(
			decor.Any(func(decor.Statistics) string {
				return fmt.Sprintf(" %d/%d 行", r.completedRows.Load(), r.totalRows.Load())
			}, decor.WCSyncSpace),
			decor.EwmaSpeed(0, " %.0f 行/s", 30, decor.WCSyncSpace),
			decor.EwmaETA(decor.ET_STYLE_GO, 30, decor.WCSyncSpace),
			decor.Any(func(decor.Statistics) string {
				return fmt.Sprintf(" 完成 %d/%d 表 运行 %d 排队 %d",
					r.completed.Load(), r.tableCount.Load(), r.running.Load(), max(r.queued.Load(), 0))
			}),
		),
	)
	r.queueBar = r.progress.AddSpinner(0,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(decor.Any(func(decor.Statistics) string {
			return fmt.Sprintf("排队: %d ", max(r.queued.Load(), 0))
		})),
	)
	if tableCount == 0 {
		r.queueBar.Abort(true)
		r.overallBar.SetTotal(1, true)
	}
}

func (r *Reporter) Start(source, target string, totalRows int64) syncer.TableProgress {
	remaining := r.queued.Add(-1)
	r.running.Add(1)
	if remaining <= 0 && r.queueBar != nil {
		r.queueBar.Abort(true)
	}
	if totalRows < 1 {
		totalRows = 1
	}
	label := source
	if source != target {
		label += " → " + target
	}
	bar := r.progress.AddBar(totalRows,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(label+" ", decor.WC{W: len([]rune(label)) + 3, C: decor.DSyncWidth}),
			decor.Percentage(decor.WCSyncSpace),
		),
		mpb.AppendDecorators(
			decor.CountersNoUnit(" %d/%d 行", decor.WCSyncSpace),
			decor.EwmaSpeed(0, " %.0f 行/s", 30, decor.WCSyncSpace),
			decor.EwmaETA(decor.ET_STYLE_GO, 30, decor.WCSyncSpace),
		),
	)
	return &tableBar{bar: bar, reporter: r}
}

func (r *Reporter) Finish() {
	r.once.Do(func() {
		if r.queueBar != nil {
			r.queueBar.Abort(true)
		}
		if r.overallBar != nil {
			if r.failed.Load() > 0 {
				r.overallBar.Abort(false)
			} else {
				r.overallBar.SetTotal(max(r.completedRows.Load(), 1), true)
			}
		}
		r.progress.Wait()
	})
}

type tableBar struct {
	bar      *mpb.Bar
	reporter *Reporter
	current  atomic.Int64
	done     atomic.Bool
}

func (b *tableBar) Add(rows int64, elapsed time.Duration) {
	if rows <= 0 || b.done.Load() {
		return
	}
	b.current.Add(rows)
	b.bar.EwmaIncrInt64(rows, elapsed)
	b.reporter.completedRows.Add(rows)
	if b.reporter.overallBar != nil {
		b.reporter.overallMu.Lock()
		b.reporter.overallBar.EwmaIncrInt64(rows, elapsed)
		b.reporter.overallMu.Unlock()
	}
}

func (b *tableBar) Complete() {
	if b.done.CompareAndSwap(false, true) {
		b.bar.SetTotal(max(b.current.Load(), 1), true)
		b.reporter.running.Add(-1)
		b.reporter.completed.Add(1)
	}
}

func (b *tableBar) Abort() {
	if b.done.CompareAndSwap(false, true) {
		b.bar.Abort(true)
		b.reporter.running.Add(-1)
		b.reporter.failed.Add(1)
	}
}
