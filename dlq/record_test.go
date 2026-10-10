package dlq

import (
	"errors"
	"regexp"
	"strings"
	"testing"
)

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-([78])[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewIDIsUniqueTimeOrderedUUIDv7(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewID()
		m := uuidRe.FindStringSubmatch(id)
		if m == nil || m[1] != "7" {
			t.Fatalf("%q is not a UUIDv7", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
	}
}

func TestDeterministicID(t *testing.T) {
	first, second := DeterministicID("a", "b"), DeterministicID("a", "b")
	if first != second {
		t.Fatal("not deterministic")
	}
	if DeterministicID("ab", "c") == DeterministicID("a", "bc") {
		t.Fatal("different groupings collided")
	}
	if !uuidRe.MatchString(DeterministicID("x")) {
		t.Fatal("not UUID formatted")
	}
	k1, k2 := KafkaID("t", 1, 2), KafkaID("t", 1, 2)
	if k1 != k2 || k1 == KafkaID("t", 2, 1) || k1 == KafkaID("u", 1, 2) {
		t.Fatal("KafkaID must be stable and distinguish topic, partition and offset")
	}
}

func TestValidate(t *testing.T) {
	ok := Record{ID: "x"}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, r := range map[string]Record{
		"empty id": {},
		"long id":  {ID: strings.Repeat("x", 257)},
		"attempts": {ID: "x", Attempts: -1},
		"leased":   {ID: "x", State: Leased},
	} {
		if err := r.Validate(); !errors.Is(err, ErrInvalidRecord) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestCloneIsDeepAndKeepsNil(t *testing.T) {
	r := Record{ID: "x", Key: []byte("k"), Value: nil, Checkpoint: []byte{}, Headers: []Header{{Key: "h", Value: []byte("v")}}}
	c := r.Clone()
	c.Key[0], c.Headers[0].Value[0] = 'X', 'X'
	if string(r.Key) != "k" || string(r.Headers[0].Value) != "v" {
		t.Fatal("Clone aliased memory")
	}
	if c.Value != nil || c.Checkpoint == nil {
		t.Fatal("Clone must keep nil and empty distinct")
	}
	if (Record{}).Clone().Headers != nil {
		t.Fatal("nil headers must stay nil")
	}
}

func TestStateStringAndSize(t *testing.T) {
	for s, want := range map[State]string{Pending: "pending", Leased: "leased", Parked: "parked", State(9): "unknown"} {
		if s.String() != want {
			t.Errorf("%d -> %q", int(s), s.String())
		}
	}
	small := Record{ID: "x"}
	big := Record{ID: "x", Value: make([]byte, 1000), Headers: []Header{{Key: "h", Value: make([]byte, 500)}}}
	if big.Size() < small.Size()+1500 {
		t.Fatalf("Size does not account for payload: %d vs %d", big.Size(), small.Size())
	}
}
