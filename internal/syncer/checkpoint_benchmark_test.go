package syncer

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkCheckpointPersistence(b *testing.B) {
	for _, every := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("every_%d_batches", every), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "state.json")
			value := state{Tables: map[string]checkpoint{"public.source->mirror.target": {}}}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := range b.N {
				for batch := 1; batch <= 100; batch++ {
					value.Tables["public.source->mirror.target"] = checkpoint{Values: []string{fmt.Sprintf("%d-%d", iteration, batch)}}
					if batch%every == 0 || batch == 100 {
						if err := saveState(path, value); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}
}
