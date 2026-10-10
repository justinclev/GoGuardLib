package breaker

import (
	"context"
	"testing"
)

func BenchmarkDo(b *testing.B) {
	br := New(Config{Name: "bench"})
	ctx := context.Background()
	fn := func(context.Context) error { return nil }
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = br.Do(ctx, fn)
		}
	})
}
