package pipeline

import (
	"testing"

	"github.com/justinclev/GoGuardLib/retry"
)

type account struct {
	ID    string `json:"id"`
	Limit int    `json:"limit"`
}

func TestSetJSONAndGetJSONRoundTripAStruct(t *testing.T) {
	x := &Exec{}
	if err := SetJSON(x, "account", account{ID: "a-1", Limit: 50}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := GetJSON[account](x, "account")
	if err != nil || !ok || got != (account{ID: "a-1", Limit: 50}) {
		t.Fatalf("got %+v ok=%v err=%v", got, ok, err)
	}
	if raw, _ := x.Get("account"); string(raw) != `{"id":"a-1","limit":50}` {
		t.Fatalf("stored %s; Set and Get must see the same JSON", raw)
	}
}

func TestGetJSONReportsAMissingKeyWithoutAnError(t *testing.T) {
	got, ok, err := GetJSON[account](&Exec{}, "nope")
	if ok || err != nil || got != (account{}) {
		t.Fatalf("got %+v ok=%v err=%v", got, ok, err)
	}
}

func TestJSONErrorsArePermanent(t *testing.T) {
	x := &Exec{}
	if err := SetJSON(x, "bad", make(chan int)); !retry.IsPermanent(err) {
		t.Fatalf("an unmarshalable value: %v, want a permanent error", err)
	}
	x.Set("old", []byte(`{"id": 5}`)) // saved by older code: id was a number
	if _, ok, err := GetJSON[account](x, "old"); ok || !retry.IsPermanent(err) {
		t.Fatalf("data of the wrong shape: ok=%v err=%v, want a permanent error", ok, err)
	}
}
