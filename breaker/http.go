package breaker

import (
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/retry"
)

// NewHTTP is the short way to a breaker for a service that has a health endpoint. It opens when half
// of the recent calls fail (after at least 20), does not count a retry.Permanent error as a failure
// (a declined card is not an outage), and asks healthURL, an absolute http or https URL, whether the
// service is back before letting traffic through. Build the Config yourself for other settings.
// Close the breaker when you are done with it.
func NewHTTP(name, healthURL string) (*Breaker, error) {
	check, err := health.HTTP(healthURL)
	if err != nil {
		return nil, err
	}
	return New(Config{
		Name: name, FailureThreshold: 0.5, MinSamples: 20,
		IsFailure: retry.IsOutage, Health: &health.Config{Check: check},
	}), nil
}
