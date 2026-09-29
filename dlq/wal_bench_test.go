package dlq_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/justinclev/GoGuardLib/dlq"
)

func benchAppend(b *testing.B, o dlq.WALOptions, parallel bool) {
	o.DisableAutoCompact = true
	s, err := dlq.OpenWAL(b.TempDir(), o)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	val := make([]byte, 512)
	var n atomic.Int64
	b.SetBytes(int64(len(val)))
	b.ResetTimer()
	do := func() {
		if err := s.Append(ctx, dlq.Record{ID: fmt.Sprintf("r%d", n.Add(1)), Value: val}); err != nil {
			b.Fatal(err)
		}
	}
	if parallel {
		b.SetParallelism(16)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				do()
			}
		})
		return
	}
	for i := 0; i < b.N; i++ {
		do()
	}
}

// Sequential appends each pay a full fsync; concurrent appends share one.
func BenchmarkWALAppendSyncAlwaysSequential(b *testing.B) { benchAppend(b, dlq.WALOptions{}, false) }
func BenchmarkWALAppendSyncAlwaysParallel(b *testing.B)   { benchAppend(b, dlq.WALOptions{}, true) }
func BenchmarkWALAppendSyncNone(b *testing.B) {
	benchAppend(b, dlq.WALOptions{Sync: dlq.SyncNone}, false)
}

func BenchmarkWALRecovery(b *testing.B) {
	dir := b.TempDir()
	s, err := dlq.OpenWAL(dir, dlq.WALOptions{Sync: dlq.SyncNone, DisableAutoCompact: true})
	if err != nil {
		b.Fatal(err)
	}
	val := make([]byte, 512)
	for i := 0; i < 50000; i++ {
		_ = s.Append(context.Background(), dlq.Record{ID: fmt.Sprintf("r%d", i), Value: val})
	}
	_ = s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := dlq.OpenWAL(dir, dlq.WALOptions{DisableAutoCompact: true})
		if err != nil {
			b.Fatal(err)
		}
		_ = s.Close()
	}
}
