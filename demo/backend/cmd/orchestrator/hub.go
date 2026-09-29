package main

import (
	"encoding/json"
	"sync"
	"time"
)

// Event is one thing that happened, streamed to the UI as it happens.
//
//	type    "request"  status: new | success | failed | rejected | deferred
//	        "step"     status: started | ok | failed
//	        "breaker"  service, from, to
//	        "probe"    service, ok
//	        "dlq"      status: stored | requeued | redrive | parked
type Event struct {
	T         int64  `json:"t"`
	Type      string `json:"type"`
	ID        string `json:"id,omitempty"`
	Flow      string `json:"flow,omitempty"` // "http" or "kafka"
	Step      string `json:"step,omitempty"`
	Status    string `json:"status,omitempty"`
	LatencyMs int64  `json:"latencyMs,omitempty"`
	Detail    string `json:"detail,omitempty"`
	Service   string `json:"service,omitempty"`
	From      string `json:"from,omitempty"`
	To        string `json:"to,omitempty"`
	OK        *bool  `json:"ok,omitempty"`
	Cause     string `json:"cause,omitempty"` // for a stored message: rejected, failed or held
	Redriven  bool   `json:"redriven,omitempty"`
}

// Hub fans events out to every connected UI without ever blocking the traffic: a
// client that cannot keep up loses events instead of slowing the demo down.
type Hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func NewHub() *Hub { return &Hub{subs: map[chan []byte]struct{}{}} }

// Subscribe returns a channel of SSE-framed messages and a function to stop.
func (h *Hub) Subscribe() (<-chan []byte, func()) {
	ch := make(chan []byte, 2048)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs, ch)
		h.mu.Unlock()
	}
}

func (h *Hub) publish(event string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	msg := append(append(append([]byte("event: "), event...), "\ndata: "...), b...)
	msg = append(msg, '\n', '\n')
	h.mu.Lock()
	for ch := range h.subs {
		select {
		case ch <- msg:
		default: // slow client: drop
		}
	}
	h.mu.Unlock()
}

// Emit publishes an Event.
func (h *Hub) Emit(e Event) {
	if e.T == 0 {
		e.T = time.Now().UnixMilli()
	}
	h.publish("e", e)
}
