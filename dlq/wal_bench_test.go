package dlq_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

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

// Leasing reads the payload back from disk unless PayloadsInMemory is set.
func benchLease(b *testing.B, inMemory bool) {
	s, err := dlq.OpenWAL(b.TempDir(), dlq.WALOptions{Sync: dlq.SyncNone, DisableAutoCompact: true, PayloadsInMemory: inMemory})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	val := make([]byte, 1024)
	for i := 0; i < 1000; i++ {
		_ = s.Append(ctx, dlq.Record{ID: fmt.Sprintf("r%d", i), Value: val})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ls, err := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Hour})
		if err != nil || len(ls) != 1 {
			b.Fatalf("lease: %v %d", err, len(ls))
		}
		if err := s.Release(ctx, ls[0].Record.ID, ls[0].Token); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALLeasePayloadsOnDisk(b *testing.B)   { benchLease(b, false) }
func BenchmarkWALLeasePayloadsInMemory(b *testing.B) { benchLease(b, true) }
