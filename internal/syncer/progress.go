package syncer

import "time"

// ProgressReporter receives table lifecycle events. Implementations must be
// safe for concurrent calls because independent tables may run in parallel.
type ProgressReporter interface {
	Begin(tableCount int)
	Start(source, target string, totalRows int64) TableProgress
	Finish()
}

// TableProgress tracks one table data-copy operation.
type TableProgress interface {
	Add(rows int64, elapsed time.Duration)
	Complete()
	Abort()
}

// SetProgressReporter installs an optional interactive progress reporter.
func (s *Syncer) SetProgressReporter(reporter ProgressReporter) {
	s.progress = reporter
}

func finishTableProgress(progress TableProgress, err error) {
	if progress == nil {
		return
	}
	if err != nil {
		progress.Abort()
		return
	}
	progress.Complete()
}
