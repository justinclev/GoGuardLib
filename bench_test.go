package goguard

import (
	"net/http"
	"testing"
)

func benchTransport(opts ...Option) *ResilientTransport {
	return NewResilientTransport(Config{
		Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) { return okResp(), nil }},
	}, opts...)
}

func BenchmarkGuardedRoundTrip(b *testing.B) {
	rt := benchTransport(WithEndpoint("svc", Host("bench.test"), Policy{}))
	defer rt.Close()
	req, _ := http.NewRequest(http.MethodGet, "http://bench.test/", nil)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, _ := rt.RoundTrip(req)
			resp.Body.Close()
		}
	})
}

func BenchmarkPassthroughRoundTrip(b *testing.B) {
	rt := benchTransport()
	defer rt.Close()
	req, _ := http.NewRequest(http.MethodGet, "http://bench.test/", nil)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, _ := rt.RoundTrip(req)
			resp.Body.Close()
		}
	})
}
