package retry_test

import (
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/justinclev/GoGuardLib/retry"
)

func answered(code int) *http.Response {
	return &http.Response{StatusCode: code, Request: &http.Request{URL: &url.URL{Host: "payments.internal", Path: "/orders/secret-id"}}}
}

func TestFromHTTPClassifiesTheOutcomeOfACall(t *testing.T) {
	boom := errors.New("connection refused")
	cases := []struct {
		name      string
		resp      *http.Response
		err       error
		wantErr   bool
		permanent bool
	}{
		{"transport error is temporary", nil, boom, true, false},
		{"200", answered(200), nil, false, false},
		{"204", answered(204), nil, false, false},
		{"302", answered(302), nil, false, false},
		{"400 is permanent", answered(400), nil, true, true},
		{"404 is permanent", answered(404), nil, true, true},
		{"422 is permanent", answered(422), nil, true, true},
		{"408 is temporary", answered(408), nil, true, false},
		{"429 is temporary", answered(429), nil, true, false},
		{"500 is temporary", answered(500), nil, true, false},
		{"503 is temporary", answered(503), nil, true, false},
	}
	for _, c := range cases {
		err := retry.FromHTTP(c.resp, c.err)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: err = %v, want error %v", c.name, err, c.wantErr)
			continue
		}
		if err != nil && retry.IsPermanent(err) != c.permanent {
			t.Errorf("%s: permanent = %v, want %v (%v)", c.name, retry.IsPermanent(err), c.permanent, err)
		}
	}
}

func TestFromHTTPNamesTheHostAndNeverThePath(t *testing.T) {
	err := retry.FromHTTP(answered(503), nil)
	if got := err.Error(); got != "payments.internal answered 503" {
		t.Fatalf("error = %q", got)
	}
	if err := retry.FromHTTP(nil, nil); err == nil {
		t.Fatal("no response and no error must not read as success")
	}
}

func TestIsOutageIgnoresRequestsThatAreWrong(t *testing.T) {
	if retry.IsOutage(nil) {
		t.Error("nil is not an outage")
	}
	if !retry.IsOutage(errors.New("down")) {
		t.Error("an ordinary error is")
	}
	if retry.IsOutage(retry.Permanent(errors.New("declined"))) {
		t.Error("a permanent error is a wrong request, not an outage")
	}
}
