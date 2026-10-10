package storetest_test

import (
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/dlq/storetest"
)

func TestClockAdvances(t *testing.T) {
	c := storetest.NewClock()
	start := c.Now()
	c.Advance(90 * time.Second)
	if got := c.Now().Sub(start); got != 90*time.Second {
		t.Fatalf("advanced %v, want 90s", got)
	}
}

// The suite must accept a store that honours the contract.
func TestSuiteAcceptsReferenceStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T, now func() time.Time) dlq.Store {
		return dlq.NewMemoryStore(dlq.MemoryOptions{Clock: now})
	})
}
