package syncer

import "testing"

var scanBufferSink any

func BenchmarkScanBufferLifecycle(b *testing.B) {
	const (
		columns = 24
		rows    = 1000
	)

	b.Run("allocate_per_row", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for range rows {
				values, pointers := scanBuffer(columns)
				for column := range columns {
					*pointers[column].(*any) = column
				}
				scanBufferSink = values[columns-1]
			}
		}
	})

	b.Run("reuse_per_batch", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			values, pointers := scanBuffer(columns)
			for range rows {
				for column := range columns {
					*pointers[column].(*any) = column
				}
				scanBufferSink = values[columns-1]
			}
		}
	})
}
