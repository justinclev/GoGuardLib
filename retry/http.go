package retry

import (
	"errors"
	"fmt"
	"net/http"
)

// FromHTTP turns the outcome of an HTTP call into the error a handler returns, so that every
// caller does not write the same switch:
//
//   - a transport error is returned as it is (the service is unreachable: temporary);
//   - a 2xx or 3xx answer is success, and FromHTTP returns nil;
//   - 408, 425, 429 and any 5xx are temporary: the call may succeed later;
//   - any other 4xx is marked Permanent: the request itself is wrong (a declined card, a missing
//     record), sending it again cannot help, and it should be set aside for a person.
//
// It does not close resp.Body; that stays with the caller. The error names the service's host and
// the status, never the URL path or query, which can carry identifiers or secrets.
func FromHTTP(resp *http.Response, err error) error {
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("retry: no response and no error")
	}
	code := resp.StatusCode
	host := ""
	if resp.Request != nil && resp.Request.URL != nil {
		host = resp.Request.URL.Host
	}
	switch {
	case code < 400:
		return nil
	case code == http.StatusRequestTimeout, code == http.StatusTooEarly, code == http.StatusTooManyRequests, code >= 500:
		return fmt.Errorf("%s answered %d", host, code)
	default:
		return Permanent(fmt.Errorf("%s answered %d", host, code))
	}
}

// IsOutage reports whether err says the dependency is unhealthy, as opposed to the request being
// wrong: any error except one marked Permanent. It is what a circuit breaker should count as a
// failure (Config.IsFailure), so that a stream of declined cards does not open the circuit.
func IsOutage(err error) bool { return err != nil && !IsPermanent(err) }
