package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/justinclev/GoGuardLib/dlq"
)

func seed(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "log")
	s, err := dlq.OpenWAL(dir, dlq.WALOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, r := range []dlq.Record{
		{ID: "p1", State: dlq.Parked, OrderKey: "cust-7", Value: []byte("CARD-4111-PAYLOAD"), LastError: "gave up after 8 attempts"},
		{ID: "w1", OrderKey: "cust-7", BlockedOn: "payments", Value: []byte("CARD-4111-PAYLOAD")},
	} {
		if err := s.Append(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func do(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestInspectShowsWhatBlocksAndNeverThePayload(t *testing.T) {
	dir := seed(t)
	code, out, errs := do(t, "inspect", dir)
	if code != 0 {
		t.Fatalf("inspect = %d: %s", code, errs)
	}
	for _, want := range []string{`"blockedKeys":1`, `"id":"p1"`, "gave up after 8 attempts", `"payments":1`} {
		if !strings.Contains(out, want) {
			t.Errorf("inspect output lacks %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, "CARD-4111") || strings.Contains(out, "cust-7") {
		t.Fatalf("inspect printed message data or a key:\n%s", out)
	}
}

func TestVerifyBackupRequeueAndDiscard(t *testing.T) {
	dir := seed(t)
	if code, out, errs := do(t, "verify", dir); code != 0 || !strings.Contains(out, `"records":2`) {
		t.Fatalf("verify = %d %s %s", code, out, errs)
	}
	dest := filepath.Join(t.TempDir(), "bk")
	if code, _, errs := do(t, "backup", dir, dest); code != 0 {
		t.Fatalf("backup = %d %s", code, errs)
	}
	if code, _, errs := do(t, "verify", dest); code != 0 {
		t.Fatalf("the backup does not verify: %s", errs)
	}
	code, out, errs := do(t, "requeue", "-actor", "alice", dir, "p1")
	if code != 0 || !strings.Contains(out, `"actor":"alice"`) || !strings.Contains(out, `"action":"requeue"`) {
		t.Fatalf("requeue = %d %s %s", code, out, errs)
	}
	if code, _, _ := do(t, "requeue", dir, "nope"); code == 0 {
		t.Fatal("requeueing a missing record must fail")
	}
	if code, out, _ := do(t, "inspect", dir); code != 0 || !strings.Contains(out, `"parked":0`) {
		t.Fatalf("after requeue: %d %s", code, out)
	}
}
