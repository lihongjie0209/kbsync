package progressui

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func TestReporterLifecycle(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	reporter := New(&output, true)
	reporter.Begin(2)
	if _, err := reporter.LogWriter().Write([]byte("同步开始\n")); err != nil {
		t.Fatal(err)
	}
	first := reporter.Start("public.users", "mirror.users", 10).(*tableBar)
	second := reporter.Start("public.events", "mirror.events", 5).(*tableBar)
	first.Add(4, time.Second)
	first.Add(6, time.Second)
	second.Add(2, time.Second)
	first.Complete()
	second.Abort()
	reporter.Finish()

	if got := reporter.queued.Load(); got != 0 {
		t.Fatalf("queued = %d, want 0", got)
	}
	if got := first.current.Load(); got != 10 || !first.done.Load() {
		t.Fatalf("first = {current:%d done:%v}, want {10 true}", got, first.done.Load())
	}
	if got := second.current.Load(); got != 2 || !second.done.Load() {
		t.Fatalf("second = {current:%d done:%v}, want {2 true}", got, second.done.Load())
	}
	if !bytes.Contains(output.Bytes(), []byte("同步开始")) {
		t.Fatalf("progress output does not contain intercepted log: %q", output.String())
	}
}

func TestReporterConcurrentTables(t *testing.T) {
	t.Parallel()
	const tables = 20
	reporter := New(io.Discard, true)
	reporter.Begin(tables)
	var wg sync.WaitGroup
	for table := range tables {
		wg.Go(func() {
			bar := reporter.Start(fmt.Sprintf("source_%d", table), fmt.Sprintf("target_%d", table), 100)
			bar.Add(40, time.Millisecond)
			bar.Add(60, time.Millisecond)
			bar.Complete()
		})
	}
	wg.Wait()
	reporter.Finish()
	if got := reporter.queued.Load(); got != 0 {
		t.Fatalf("queued = %d, want 0", got)
	}
}
