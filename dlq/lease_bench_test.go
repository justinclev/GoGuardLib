package dlq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
)

// During an outage most of the queue waits on a dependency whose circuit is open.
// Polling for other work must not cost time proportional to that backlog.
func BenchmarkLeaseWithBlockedBacklog(b *testing.B) {
	for _, n := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("backlog=%d", n), func(b *testing.B) {
			s := dlq.NewMemoryStore(dlq.MemoryOptions{MaxBytes: -1})
			ctx := context.Background()
			for i := 0; i < n; i++ {
				_ = s.Append(ctx, dlq.Record{ID: fmt.Sprintf("r%d", i), BlockedOn: "down", Value: []byte("x")})
			}
			req := dlq.LeaseRequest{Max: 8, TTL: time.Minute, Skip: []string{"down"}}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if ls, _ := s.Lease(ctx, req); len(ls) != 0 {
					b.Fatal("leased a skipped record")
				}
			}
		})
	}
}

// Records waiting out a backoff are not due. Polling must not re-inspect them all.
func BenchmarkLeaseWithDelayedBacklog(b *testing.B) {
	s := dlq.NewMemoryStore(dlq.MemoryOptions{MaxBytes: -1})
	ctx := context.Background()
	for i := 0; i < 100_000; i++ {
		_ = s.Append(ctx, dlq.Record{ID: fmt.Sprintf("r%d", i), NextAttempt: time.Now().Add(time.Hour), Value: []byte("x")})
	}
	req := dlq.LeaseRequest{Max: 8, TTL: time.Minute}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if ls, _ := s.Lease(ctx, req); len(ls) != 0 {
			b.Fatal("leased a record that is not due")
		}
	}
}

// Requeueing a large parked queue, oldest first, into a queue that already holds many
// newer records must not walk the newer records for every insert.
func BenchmarkRequeueParkedIntoALargeQueue(b *testing.B) {
	const n = 20_000
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := dlq.NewMemoryStore(dlq.MemoryOptions{MaxBytes: -1})
		for j := 0; j < n; j++ {
			_ = s.Append(ctx, dlq.Record{ID: fmt.Sprintf("p%06d", j), State: dlq.Parked, Value: []byte("x")})
		}
		for j := 0; j < n; j++ {
			_ = s.Append(ctx, dlq.Record{ID: fmt.Sprintf("q%06d", j), Value: []byte("x")})
		}
		b.StartTimer()
		for j := 0; j < n; j++ {
			if err := s.Requeue(ctx, fmt.Sprintf("p%06d", j)); err != nil {
				b.Fatal(err)
			}
		}
	}
}
