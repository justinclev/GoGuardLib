// Command dlqctl inspects and maintains a dead-letter log written by dlq.OpenWAL.
//
//	dlqctl verify  [flags] DIR          check every frame and the sequence, changing nothing
//	dlqctl inspect [flags] DIR          counts, blocked keys, oldest parked, the parked records
//	dlqctl backup  [flags] DIR DEST     copy the log to an empty directory
//	dlqctl requeue [flags] DIR ID...    send parked records back to pending
//	dlqctl discard [flags] DIR ID...    delete parked records
//
// verify and inspect work on a private copy: they never write to DIR and do not
// need the directory lock, so they are safe beside a running service. backup,
// requeue and discard open the store, so a service that holds the lock must be
// stopped first (a live store can be backed up in code with WALStore.Backup). Output is JSON,
// one document per line. It never prints message keys, values or headers: only IDs,
// sources, counts, timings and the (redacted) failure reason.
//
// A log written with signing needs -signing-key-file (the raw key bytes) and
// -signing-key-id.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/secure"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

type common struct {
	keyFile, keyID string
	acceptLegacy   bool
	actor          string
}

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.keyFile, "signing-key-file", "", "file holding the raw signing key, for logs written with WALOptions.Signer")
	fs.StringVar(&c.keyID, "signing-key-id", "default", "ID of that key")
	fs.BoolVar(&c.acceptLegacy, "accept-unsigned-legacy", false, "read files written before signing was on")
	fs.StringVar(&c.actor, "actor", "", "who is acting, recorded with requeue and discard")
}

func (c *common) options() (dlq.WALOptions, error) {
	o := dlq.WALOptions{AcceptUnsignedLegacy: c.acceptLegacy}
	if c.keyFile != "" {
		b, err := os.ReadFile(c.keyFile)
		if err != nil {
			return o, err
		}
		s, err := secure.NewHMAC(secure.Key{ID: c.keyID, Material: b})
		if err != nil {
			return o, err
		}
		o.Signer = s
	}
	return o, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return usage(stderr)
	}
	cmd, rest := args[0], args[1:]
	var c common
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	c.register(fs)
	if err := fs.Parse(rest); err != nil {
		return 2
	}
	pos := fs.Args()
	out := json.NewEncoder(stdout)
	fail := func(err error) int {
		_, _ = fmt.Fprintln(stderr, "dlqctl:", err)
		if errors.Is(err, dlq.ErrLocked) {
			_, _ = fmt.Fprintln(stderr, "dlqctl: stop the service that owns the directory first, or use verify/inspect, which do not need the lock")
		}
		return 1
	}
	opts, err := c.options()
	if err != nil {
		return fail(err)
	}
	ctx := context.Background()

	switch cmd {
	case "verify":
		if len(pos) != 1 {
			return usage(stderr)
		}
		rep, err := dlq.VerifyWAL(pos[0], opts)
		if err != nil {
			return fail(err)
		}
		_ = out.Encode(map[string]any{"ok": true, "records": rep.Stats.Total(), "entries": rep.Recovery.Entries, "segments": rep.Recovery.Segments, "snapshotLSN": rep.Recovery.SnapshotLSN})
	case "inspect":
		if len(pos) != 1 {
			return usage(stderr)
		}
		return inspect(ctx, pos[0], opts, out, fail)
	case "backup":
		if len(pos) != 2 {
			return usage(stderr)
		}
		s, err := dlq.OpenWAL(pos[0], opts)
		if err != nil {
			return fail(err)
		}
		defer func() { _ = s.Close() }()
		if err := s.Backup(ctx, pos[1]); err != nil {
			return fail(err)
		}
		_ = out.Encode(map[string]any{"ok": true, "backup": pos[1]})
	case "requeue", "discard":
		if len(pos) < 2 {
			return usage(stderr)
		}
		return act(ctx, cmd, pos[0], pos[1:], c.actor, opts, out, fail)
	default:
		return usage(stderr)
	}
	return 0
}

func usage(w io.Writer) int {
	_, _ = fmt.Fprintln(w, "usage: dlqctl verify|inspect|backup|requeue|discard [flags] DIR [DEST|ID...]")
	return 2
}

func inspect(ctx context.Context, dir string, opts dlq.WALOptions, out *json.Encoder, fail func(error) int) int {
	// A private copy: a running service is not disturbed and DIR is not written.
	src, cleanup, err := dlq.OpenWALCopy(dir, opts)
	if err != nil {
		return fail(err)
	}
	defer cleanup()
	st, err := src.Stats(ctx)
	if err != nil {
		return fail(err)
	}
	_ = out.Encode(map[string]any{
		"pending": st.Pending, "leased": st.Leased, "parked": st.Parked, "bytes": st.Bytes,
		"oldestPending": st.OldestPending.String(), "oldestParked": st.OldestParked.String(),
		"blockedKeys": st.BlockedKeys, "byDependency": st.ByDependency,
	})
	var after uint64
	for {
		page, err := src.Parked(ctx, dlq.ParkedQuery{After: after, Limit: 500})
		if err != nil {
			return fail(err)
		}
		if len(page) == 0 {
			return 0
		}
		for _, r := range page {
			after = r.Seq
			_ = out.Encode(map[string]any{
				"id": r.ID, "source": r.Source, "attempts": r.Attempts, "blockedOn": r.BlockedOn,
				"orderKeyed": r.OrderKey != "", "firstFailed": r.FirstFailed.Format(time.RFC3339), "reason": r.LastError,
			})
		}
	}
}

func act(ctx context.Context, cmd, dir string, ids []string, actor string, opts dlq.WALOptions, out *json.Encoder, fail func(error) int) int {
	s, err := dlq.OpenWAL(dir, opts)
	if err != nil {
		return fail(err)
	}
	defer func() { _ = s.Close() }()
	st := dlq.Audited(s, dlq.AuditOptions{Events: obs.SinkFunc(func(e obs.Event) {
		if a, ok := e.(obs.OperatorAction); ok {
			_ = out.Encode(map[string]any{"action": a.Action, "id": a.RecordID, "actor": a.Actor, "ok": a.OK, "at": a.At.Format(time.RFC3339)})
		}
	})})
	ctx = dlq.WithActor(ctx, actor)
	code := 0
	for _, id := range ids {
		var err error
		if cmd == "requeue" {
			err = st.Requeue(ctx, id)
		} else {
			err = st.Discard(ctx, id)
		}
		if err != nil {
			code = fail(fmt.Errorf("%s %s: %w", cmd, id, err))
		}
	}
	return code
}
