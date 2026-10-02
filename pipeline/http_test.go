package pipeline_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/pipeline"
)

type hit struct{ method, path, idem, ctype, body string }

// api records what reaches it and answers with a handler per path.
func api(t *testing.T, answer func(w http.ResponseWriter, r *http.Request)) (*httptest.Server, func() []hit) {
	t.Helper()
	var mu sync.Mutex
	var hits []hit
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		hits = append(hits, hit{r.Method, r.URL.Path, r.Header.Get("Idempotency-Key"), r.Header.Get("Content-Type"), string(b)})
		mu.Unlock()
		answer(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []hit { mu.Lock(); defer mu.Unlock(); return append([]hit(nil), hits...) }
}

func run(t *testing.T, p *pipeline.Pipeline, key, value string) pipeline.Result {
	t.Helper()
	res, _ := p.Execute(context.Background(), dlq.NewMemoryStore(dlq.MemoryOptions{}), pipeline.Input{ID: "m-1", Key: []byte(key), Value: []byte(value)})
	return res
}

// The README's short example: one answer feeds the next URL, with no code in the steps.
func TestHTTPStepsPassDataThroughURLs(t *testing.T) {
	srv, hits := api(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/account/c-7" {
			_, _ = w.Write([]byte(`{"id":"acct 9","limit":50,"vip":true}`))
		}
	})
	p, err := pipeline.New("checkout", "v1", []pipeline.Step{
		{Name: "fetch", HTTP: pipeline.Get(srv.URL + "/account/{key}"), SaveAs: "account"},
		{Name: "charge", HTTP: pipeline.Post(srv.URL + "/charge/{account.id}/{account.limit}/{account.vip}")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res := run(t, p, "c-7", `{"total":12}`); res != pipeline.Done {
		t.Fatalf("result %v, want Done", res)
	}
	got := hits()
	if len(got) != 2 {
		t.Fatalf("%d requests, want 2: %+v", len(got), got)
	}
	if got[0].method != "GET" || got[0].path != "/account/c-7" || got[0].ctype != "" || got[0].body != "" {
		t.Errorf("fetch = %+v", got[0])
	}
	if got[1].method != "POST" || got[1].path != "/charge/acct 9/50/true" || got[1].body != `{"total":12}` || got[1].ctype != "application/json" {
		t.Errorf("charge = %+v (the value is escaped on the wire, decoded by the server)", got[1])
	}
	if got[0].idem == "" || got[0].idem == got[1].idem {
		t.Errorf("each step needs its own idempotency key: %q %q", got[0].idem, got[1].idem)
	}
}

func TestHTTPStepSortsTheAnswer(t *testing.T) {
	for status, want := range map[int]pipeline.Result{200: pipeline.Done, 503: pipeline.Deferred, 404: pipeline.Parked} {
		srv, _ := api(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) })
		p, err := pipeline.New("p", "v1", []pipeline.Step{{Name: "call", HTTP: pipeline.Post(srv.URL)}})
		if err != nil {
			t.Fatal(err)
		}
		if res := run(t, p, "k", "v"); res != want {
			t.Errorf("status %d gave %v, want %v", status, res, want)
		}
	}
}

func TestHTTPStepMissingDataIsPermanent(t *testing.T) {
	srv, hits := api(t, func(w http.ResponseWriter, r *http.Request) {})
	p, err := pipeline.New("p", "v1", []pipeline.Step{{Name: "charge", HTTP: pipeline.Post(srv.URL + "/charge/{account.id}")}})
	if err != nil {
		t.Fatal(err)
	}
	if res := run(t, p, "k", "v"); res != pipeline.Parked {
		t.Fatalf("result %v, want Parked: no earlier step saved the account", res)
	}
	if len(hits()) != 0 {
		t.Fatal("nothing may be sent with a half-built URL")
	}
}

func TestHTTPStepRetriesShorthand(t *testing.T) {
	var n atomic.Int32
	srv, hits := api(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	p, err := pipeline.New("p", "v1", []pipeline.Step{{Name: "read", Retries: 2, HTTP: pipeline.Get(srv.URL)}})
	if err != nil {
		t.Fatal(err)
	}
	if res := run(t, p, "k", ""); res != pipeline.Done || len(hits()) != 3 {
		t.Fatalf("result %v after %d calls, want Done after 3", res, len(hits()))
	}
}

func TestHTTPStepCustomisation(t *testing.T) {
	srv, hits := api(t, func(w http.ResponseWriter, r *http.Request) {})
	call := pipeline.Post(srv.URL)
	call.Body = func(x *pipeline.Exec) ([]byte, error) { return []byte("custom"), nil }
	call.ContentType = "text/plain"
	call.Prepare = func(_ context.Context, r *http.Request, _ *pipeline.Exec) error {
		r.Header.Set("Authorization", "Bearer t")
		return nil
	}
	p, err := pipeline.New("p", "v1", []pipeline.Step{{Name: "call", HTTP: call}})
	if err != nil {
		t.Fatal(err)
	}
	if res := run(t, p, "k", "ignored"); res != pipeline.Done {
		t.Fatal(res)
	}
	if h := hits()[0]; h.body != "custom" || h.ctype != "text/plain" {
		t.Errorf("got %+v", h)
	}
}

func TestHTTPStepsAreCheckedWhenBuilt(t *testing.T) {
	ok := func(context.Context, *pipeline.Exec) error { return nil }
	bad := map[string][]pipeline.Step{
		"both Run and HTTP":   {{Name: "a", Run: ok, HTTP: pipeline.Get("http://x")}},
		"neither":             {{Name: "a"}},
		"no URL":              {{Name: "a", HTTP: &pipeline.HTTPCall{Method: "GET"}}},
		"no method":           {{Name: "a", HTTP: &pipeline.HTTPCall{URL: "http://x"}}},
		"open brace":          {{Name: "a", HTTP: pipeline.Get("http://x/{key")}},
		"stray brace":         {{Name: "a", HTTP: pipeline.Get("http://x/key}")}},
		"empty reference":     {{Name: "a", HTTP: pipeline.Get("http://x/{}")}},
		"SaveAs without HTTP": {{Name: "a", Run: ok, SaveAs: "x"}},
		"bad HealthURL":       {{Name: "a", Run: ok, HealthURL: "/health"}},
	}
	for name, steps := range bad {
		if _, err := pipeline.New("p", "v1", steps); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestHealthURLBuildsABreakerThatThePipelineOwns(t *testing.T) {
	srv, _ := api(t, func(w http.ResponseWriter, r *http.Request) {})
	p, err := pipeline.New("p", "v1", []pipeline.Step{
		{Name: "pay", Dependency: "payments", HealthURL: srv.URL + "/health", HTTP: pipeline.Post(srv.URL)},
		{Name: "ship", HealthURL: srv.URL + "/health", HTTP: pipeline.Post(srv.URL)},
	})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, b := range p.Breakers() {
		names = append(names, b.Name())
	}
	if strings.Join(names, ",") != "payments,ship" {
		t.Fatalf("breakers %v, want payments and ship", names)
	}
	_ = p.Close()
	_ = p.Close() // safe to repeat
}
