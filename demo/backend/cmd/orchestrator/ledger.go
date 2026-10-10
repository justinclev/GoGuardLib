package main

import (
	"sync"
	"time"
)

// ledger follows every Kafka order from production to completion, using only the
// events the flows emit. It exists so the demo can check the claim "nothing is
// lost" instead of narrating it: the UI compares what the ledger believes is in the
// dead-letter log with what the write-ahead log itself reports.
//
// Only outstanding orders are kept, so its size follows the backlog, not the run.
type ledger struct {
	mu       sync.Mutex
	st       map[string]*ledgerEntry
	produced uint64
	done     uint64
	parked   uint64

	// adopted is how many orders were already in the log when this process started
	// (a restart after a crash); recovered is how many of them have not reappeared yet.
	adopted   uint64
	recovered int
}

// seed tells the ledger how many orders it inherited from the log at startup, so a
// restart does not look like a discrepancy.
func (l *ledger) seed(n int) {
	l.mu.Lock()
	l.adopted, l.recovered = uint64(n), n
	l.mu.Unlock()
}

type ledgerEntry struct {
	state string
	since time.Time
}

const (
	stWaiting   = "waiting"   // produced, no step has started
	stInflight  = "inflight"  // a step is running
	stStored    = "stored"    // set aside in the dead-letter log
	stRedriving = "redriving" // leased from the log by the redriver
)

// stuckAfter is how long an order may be "in flight" before it counts as unaccounted
// for: a step that started and never finished, failed or stored anything.
const stuckAfter = 60 * time.Second

func newLedger() *ledger { return &ledger{st: map[string]*ledgerEntry{}} }

func (l *ledger) set(id, state string, now time.Time) {
	if e, ok := l.st[id]; ok {
		if e.state != state {
			e.state, e.since = state, now
		}
		return
	}
	l.st[id] = &ledgerEntry{state: state, since: now}
}

// observe updates the ledger from one event. Orders it has not seen (produced before
// a restart, for instance) are adopted as they reappear.
func (l *ledger) observe(e Event) {
	if e.ID == "" || (e.Flow != "kafka" && e.Flow != "http") {
		return
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if e.Flow == "http" {
		l.observeHTTP(e, now)
		return
	}
	switch {
	case e.Type == "request" && e.Status == "new":
		l.produced++
		l.set(e.ID, stWaiting, now)
	case e.Type == "step" && e.Status == "started":
		if cur, ok := l.st[e.ID]; ok && cur.state == stRedriving {
			return // a redriven order stays "redriving" until it finishes or is set aside again
		}
		l.set(e.ID, stInflight, now)
	case e.Type == "dlq" && (e.Status == "stored" || e.Status == "requeued"):
		l.set(e.ID, stStored, now)
	case e.Type == "dlq" && e.Status == "redrive":
		if _, known := l.st[e.ID]; !known && l.recovered > 0 {
			l.recovered-- // an order from before the restart came back to life
		}
		l.set(e.ID, stRedriving, now)
	case e.Type == "request" && e.Status == "success":
		if _, ok := l.st[e.ID]; ok {
			delete(l.st, e.ID)
		}
		l.done++
	case e.Type == "request" && e.Status == "parked":
		delete(l.st, e.ID)
		l.parked++
	}
}

// observeHTTP follows the HTTP requests that were saved for later. An ordinary HTTP
// request is answered in the same breath and never enters the ledger; a saved one is
// work the store now owes, so it is counted like a Kafka message from the moment it is
// saved until its replay finishes.
func (l *ledger) observeHTTP(e Event, now time.Time) {
	switch {
	case e.Type == "dlq" && (e.Status == "stored" || e.Status == "requeued"):
		if _, known := l.st[e.ID]; !known && e.Status == "stored" {
			l.produced++
		}
		l.set(e.ID, stStored, now)
	case e.Type == "dlq" && e.Status == "redrive":
		if _, known := l.st[e.ID]; !known && l.recovered > 0 {
			l.recovered--
		}
		l.set(e.ID, stRedriving, now)
	case e.Type == "request" && e.Redriven && e.Status == "success":
		delete(l.st, e.ID)
		l.done++
	case e.Type == "request" && e.Redriven && e.Status == "parked":
		delete(l.st, e.ID)
		l.parked++
	}
}

// LedgerView is the ledger as the UI sees it.
type LedgerView struct {
	Produced uint64 `json:"produced"`
	Adopted  uint64 `json:"adopted"` // already in the log when this process started
	Done     uint64 `json:"done"`
	Parked   uint64 `json:"parked"`
	Waiting  int    `json:"waiting"`
	InFlight int    `json:"inFlight"`
	Stored   int    `json:"stored"` // set aside or being redriven
	Stuck    int    `json:"stuck"`  // in flight for too long: unaccounted for
	OnDisk   int    `json:"onDisk"` // what the write-ahead log itself reports
	Agrees   bool   `json:"agrees"` // ledger and log agree, within the events still in transit
}

func (l *ledger) view(onDisk int) LedgerView {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	v := LedgerView{Produced: l.produced, Adopted: l.adopted, Done: l.done, Parked: l.parked, OnDisk: onDisk, Stored: l.recovered}
	for _, e := range l.st {
		switch e.state {
		case stWaiting:
			v.Waiting++
		case stInflight:
			v.InFlight++
			if now.Sub(e.since) > stuckAfter {
				v.Stuck++
			}
		default:
			v.Stored++
		}
	}
	diff := v.Stored - onDisk
	if diff < 0 {
		diff = -diff
	}
	// Events and the log's own counters are read at slightly different moments, so a
	// few orders may be in transit between them.
	v.Agrees = diff <= 3+v.Stored/20
	return v
}
