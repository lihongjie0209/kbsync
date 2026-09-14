package syncer

import (
	"strings"
	"testing"
	"time"
)

func TestMetricSetRender(t *testing.T) {
	t.Parallel()
	var metrics metricSet
	metrics.record("incremental", "public.users", "mirror.users", 200, 2, 2*time.Second, false)
	output := metrics.render()
	for _, expected := range []string{
		`kbsync_rows_total{mode="incremental",source="public.users",target="mirror.users"} 200`,
		`kbsync_batches_total{mode="incremental",source="public.users",target="mirror.users"} 2`,
		`kbsync_rows_per_second{mode="incremental",source="public.users",target="mirror.users"} 100.000000`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("metrics output missing %q:\n%s", expected, output)
		}
	}
}
