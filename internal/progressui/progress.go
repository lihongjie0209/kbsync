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
	progress *mpb.Progress
	queued   atomic.Int64
	queueBar *mpb.Bar
	once     sync.Once
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

func (r *Reporter) Begin(tableCount int) {
	r.queued.Store(int64(tableCount))
	r.queueBar = r.progress.AddSpinner(0,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(decor.Any(func(decor.Statistics) string {
			return fmt.Sprintf("排队: %d ", max(r.queued.Load(), 0))
		})),
	)
	if tableCount == 0 {
		r.queueBar.Abort(true)
	}
}

func (r *Reporter) Start(source, target string, totalRows int64) syncer.TableProgress {
	remaining := r.queued.Add(-1)
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
	return &tableBar{bar: bar}
}

func (r *Reporter) Finish() {
	r.once.Do(func() {
		if r.queueBar != nil {
			r.queueBar.Abort(true)
		}
		r.progress.Wait()
	})
}

type tableBar struct {
	bar     *mpb.Bar
	current atomic.Int64
	done    atomic.Bool
}

func (b *tableBar) Add(rows int64, elapsed time.Duration) {
	if rows <= 0 || b.done.Load() {
		return
	}
	b.current.Add(rows)
	b.bar.EwmaIncrInt64(rows, elapsed)
}

func (b *tableBar) Complete() {
	if b.done.CompareAndSwap(false, true) {
		b.bar.SetTotal(max(b.current.Load(), 1), true)
	}
}

func (b *tableBar) Abort() {
	if b.done.CompareAndSwap(false, true) {
		b.bar.Abort(true)
	}
}
