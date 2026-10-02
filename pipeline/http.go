package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/justinclev/GoGuardLib/retry"
)

// HTTPCall describes a step that is one HTTP request, so that the common case needs no code. Set it
// as Step.HTTP (leave Run empty). The request carries the step's idempotency key in an
// Idempotency-Key header, and the answer is sorted by retry.FromHTTP: success, temporary (retried
// later) or permanent (a 4xx other than 408, 425 and 429: parked for a person).
//
// The URL may name data in braces:
//
//	{key}          the Kafka message key
//	{account}      what an earlier step saved under "account", as text
//	{account.id}   a field of that saved JSON (dots reach into objects)
//
// Values are escaped for a URL path. Data that is missing is a permanent error: retrying cannot
// make an earlier step save it.
type HTTPCall struct {
	// Method is the HTTP method. Required; Get and Post set it.
	Method string
	// URL is the address, with optional {name} parts. Required.
	URL string
	// Body builds the request body. Default: the message value for POST, PUT and PATCH, and no body
	// for the other methods.
	Body func(x *Exec) ([]byte, error)
	// ContentType is sent when there is a body. Default "application/json".
	ContentType string
	// Client sends the request. Default: http.DefaultClient. A goguard client works too.
	Client *http.Client
	// Prepare adjusts the request before it is sent: add credentials, change a header.
	Prepare func(ctx context.Context, req *http.Request, x *Exec) error
}

// Get is an HTTPCall that reads url.
func Get(url string) *HTTPCall { return &HTTPCall{Method: http.MethodGet, URL: url} }

// Post is an HTTPCall that posts the message value to url.
func Post(url string) *HTTPCall { return &HTTPCall{Method: http.MethodPost, URL: url} }

const maxHTTPBody = 1 << 20

// compile checks the call and returns the step function that makes it.
func (c *HTTPCall) compile(step, saveAs string) (func(context.Context, *Exec) error, error) {
	if c.URL == "" {
		return nil, fmt.Errorf("pipeline: step %q: HTTP needs a URL", step)
	}
	if _, err := http.NewRequest(c.Method, "http://example.invalid/", nil); err != nil || c.Method == "" {
		return nil, fmt.Errorf("pipeline: step %q: HTTP needs a valid Method", step)
	}
	tmpl, err := parseTemplate(c.URL)
	if err != nil {
		return nil, fmt.Errorf("pipeline: step %q: %w", step, err)
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	contentType := c.ContentType
	if contentType == "" {
		contentType = "application/json"
	}
	hasBody := c.Method == http.MethodPost || c.Method == http.MethodPut || c.Method == http.MethodPatch
	return func(ctx context.Context, x *Exec) error {
		target, err := tmpl.render(x)
		if err != nil {
			return retry.Permanent(err)
		}
		var body []byte
		switch {
		case c.Body != nil:
			if body, err = c.Body(x); err != nil {
				return err
			}
		case hasBody:
			body = x.Value
		}
		req, err := http.NewRequestWithContext(ctx, c.Method, target, bytes.NewReader(body))
		if err != nil {
			return retry.Permanent(err)
		}
		req.Header.Set("Idempotency-Key", x.IdempotencyKey())
		if len(body) > 0 {
			req.Header.Set("Content-Type", contentType)
		}
		if c.Prepare != nil {
			if err := c.Prepare(ctx, req, x); err != nil {
				return err
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		reply, readErr := io.ReadAll(io.LimitReader(resp.Body, maxHTTPBody+1))
		if err := retry.FromHTTP(resp, nil); err != nil {
			return err
		}
		if saveAs != "" {
			if readErr != nil {
				// The connection broke part way through the answer. Saving the piece we got would
				// hand the next step broken data, so fail this step (temporary) and try it again.
				return fmt.Errorf("pipeline: step %q: reading the answer: %w", step, readErr)
			}
			if len(reply) > maxHTTPBody {
				return retry.Permanent(fmt.Errorf("pipeline: step %q: the answer is too large to save (over %d bytes)", step, maxHTTPBody))
			}
			x.Set(saveAs, reply)
		}
		return nil
	}, nil
}

// ---- URL templates ----

type tmplPart struct {
	text string // literal text, or the reference when ref is true
	ref  bool
}

type template []tmplPart

func parseTemplate(s string) (template, error) {
	var out template
	for len(s) > 0 {
		open := strings.IndexByte(s, '{')
		if open < 0 {
			if strings.IndexByte(s, '}') >= 0 {
				return nil, errors.New(`URL has a "}" without a "{"`)
			}
			out = append(out, tmplPart{text: s})
			break
		}
		if open > 0 {
			out = append(out, tmplPart{text: s[:open]})
		}
		end := strings.IndexByte(s[open:], '}')
		if end < 0 {
			return nil, errors.New(`URL has a "{" without a "}"`)
		}
		ref := s[open+1 : open+end]
		if ref == "" || strings.ContainsAny(ref, "{ ") {
			return nil, fmt.Errorf("URL has a bad reference {%s}", ref)
		}
		out = append(out, tmplPart{text: ref, ref: true})
		s = s[open+end+1:]
	}
	return out, nil
}

func (t template) render(x *Exec) (string, error) {
	var b strings.Builder
	for _, p := range t {
		if !p.ref {
			b.WriteString(p.text)
			continue
		}
		v, err := lookup(x, p.text)
		if err != nil {
			return "", err
		}
		b.WriteString(url.PathEscape(v))
	}
	return b.String(), nil
}

// lookup resolves "key", "name" or "name.path.to.field".
func lookup(x *Exec, ref string) (string, error) {
	name, path, _ := strings.Cut(ref, ".")
	if name == "key" && path == "" {
		return string(x.Key), nil
	}
	raw, ok := x.Get(name)
	if !ok {
		return "", fmt.Errorf("pipeline: {%s}: nothing was saved under %q by an earlier step", ref, name)
	}
	if path == "" {
		return string(raw), nil
	}
	var cur any
	if err := json.Unmarshal(raw, &cur); err != nil {
		return "", fmt.Errorf("pipeline: {%s}: what was saved under %q is not JSON", ref, name)
	}
	for _, field := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", fmt.Errorf("pipeline: {%s}: %q is not an object", ref, field)
		}
		if cur, ok = m[field]; !ok {
			return "", fmt.Errorf("pipeline: {%s}: no field %q", ref, field)
		}
	}
	switch v := cur.(type) {
	case string:
		return v, nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		return "", fmt.Errorf("pipeline: {%s} is not a string, number or boolean", ref)
	}
}
