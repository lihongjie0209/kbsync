package syncer

import (
	"testing"
	"time"
)

var checkpointValuesSink []string

func BenchmarkCheckpointExtraction(b *testing.B) {
	values := []any{time.Date(2026, 9, 14, 12, 0, 0, 123, time.UTC), int64(42), "payload"}
	indexes := []int{0, 1}
	const rows = 1000

	b.Run("allocate_per_row", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			var last []string
			for range rows {
				last = make([]string, len(indexes))
				for i, index := range indexes {
					last[i] = checkpointString(values[index])
				}
			}
			checkpointValuesSink = last
		}
	})

	b.Run("reuse_per_batch", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			last := make([]string, len(indexes))
			for range rows {
				for i, index := range indexes {
					last[i] = checkpointString(values[index])
				}
			}
			checkpointValuesSink = last
		}
	})
}
