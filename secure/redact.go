package secure

import (
	"net/textproto"
	"regexp"
	"unicode/utf8"
)

// Redacted replaces every secret the Redactor removes.
const Redacted = "[REDACTED]"

const defaultMaxLength = 2048

var defaultSensitiveHeaders = []string{
	"Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie",
	"X-Api-Key", "Api-Key", "X-Auth-Token", "X-Amz-Security-Token", "X-Csrf-Token",
}

// Each rule keeps the surrounding structure (the key, the scheme) and replaces
// only the secret, so redacted text stays useful for diagnosis. The patterns
// use Go's RE2 engine, which runs in linear time on any input.
type rule struct {
	re   *regexp.Regexp
	repl string
}

var defaultRules = []rule{
	// Authorization schemes: "Bearer abc", "Basic dXNlcjpwYXNz".
	{regexp.MustCompile(`(?i)\b(bearer|basic|token)(\s+)[A-Za-z0-9\-._~+/]{8,}=*`), "${1}${2}" + Redacted},
	// JSON Web Tokens.
	{regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]*`), Redacted},
	// user:password@ inside URLs.
	{regexp.MustCompile(`(?i)([a-z][a-z0-9+.\-]*://)([^/\s:@]+):([^/\s@]+)@`), "${1}${2}:" + Redacted + "@"},
	// key=value, key: value and "key":"value" for sensitive keys.
	{regexp.MustCompile(`(?i)\b(password|passwd|pwd|passphrase|secret|token|api[_-]?key|access[_-]?key|secret[_-]?key|private[_-]?key|client[_-]?secret|credentials?|authorization|auth)\b(["']?\s*[:=]\s*["']?)([^\s"'&,;}\]]+)`), "${1}${2}" + Redacted},
}

// Redactor removes secrets from text and headers before they are stored, put in
// an error or handed to a hook. It is immutable and safe for concurrent use.
//
// It is a safety net, not a guarantee: it recognises common credential shapes.
// Do not rely on it to make arbitrary payloads safe to store; encrypt those.
type Redactor struct {
	headers   map[string]struct{}
	rules     []rule
	maxLength int
}

// RedactorOption customises a Redactor.
type RedactorOption func(*Redactor)

// WithHeaders redacts additional header names (case-insensitive).
func WithHeaders(names ...string) RedactorOption {
	return func(r *Redactor) {
		for _, n := range names {
			r.headers[textproto.CanonicalMIMEHeaderKey(n)] = struct{}{}
		}
	}
}

// WithPatterns redacts every match of the given expressions.
func WithPatterns(patterns ...*regexp.Regexp) RedactorOption {
	return func(r *Redactor) {
		for _, p := range patterns {
			r.rules = append(r.rules, rule{re: p, repl: Redacted})
		}
	}
}

// WithMaxLength truncates redacted text to n bytes (on a character boundary),
// so an error that embeds a payload cannot smuggle the payload into storage.
// n <= 0 disables truncation. The default is 2048.
func WithMaxLength(n int) RedactorOption { return func(r *Redactor) { r.maxLength = n } }

// NewRedactor builds a Redactor with sensible defaults.
func NewRedactor(opts ...RedactorOption) *Redactor {
	r := &Redactor{
		headers:   make(map[string]struct{}),
		rules:     append([]rule(nil), defaultRules...),
		maxLength: defaultMaxLength,
	}
	for _, h := range defaultSensitiveHeaders {
		r.headers[textproto.CanonicalMIMEHeaderKey(h)] = struct{}{}
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// String returns s with secrets replaced by Redacted, truncated to the maximum
// length.
func (r *Redactor) String(s string) string {
	for _, rl := range r.rules {
		s = rl.re.ReplaceAllString(s, rl.repl)
	}
	if r.maxLength > 0 && len(s) > r.maxLength {
		cut := r.maxLength
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut] + "…[truncated]"
	}
	return s
}

// Error returns the redacted text of err, or "" for nil.
func (r *Redactor) Error(err error) string {
	if err == nil {
		return ""
	}
	return r.String(err.Error())
}

// IsSensitiveHeader reports whether the header's value must not be recorded.
func (r *Redactor) IsSensitiveHeader(name string) bool {
	_, ok := r.headers[textproto.CanonicalMIMEHeaderKey(name)]
	return ok
}

// Headers returns a copy of h with the values of sensitive headers replaced by
// Redacted and the others passed through String. h is not modified.
func (r *Redactor) Headers(h map[string][]string) map[string][]string {
	if h == nil {
		return nil
	}
	out := make(map[string][]string, len(h))
	for name, values := range h {
		cp := make([]string, len(values))
		for i, v := range values {
			if r.IsSensitiveHeader(name) {
				cp[i] = Redacted
			} else {
				cp[i] = r.String(v)
			}
		}
		out[name] = cp
	}
	return out
}
