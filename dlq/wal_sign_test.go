package dlq

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/secure"
)

func hmacSigner(t testing.TB, id string, fill byte, retired ...secure.Key) *secure.HMACSigner {
	t.Helper()
	sg, err := secure.NewHMAC(secure.Key{ID: id, Material: bytes.Repeat([]byte{fill}, 32)}, retired...)
	if err != nil {
		t.Fatal(err)
	}
	return sg
}

// forgeFrames rewrites a log file, letting mutate change the on-disk body of any
// frame; the frame's CRC is recomputed, as an attacker with write access would.
func forgeFrames(t testing.TB, path string, mutate func(i int, lsn uint64, typ byte, body []byte) (uint64, byte, []byte)) {
	t.Helper()
	raw, err := os.ReadFile(path)
	mustNil(t, err)
	out := append([]byte{}, raw[:headerSize]...)
	r := bufio.NewReader(bytes.NewReader(raw[headerSize:]))
	remaining := int64(len(raw) - headerSize)
	for i := 0; ; i++ {
		fr, err := readFrame(r, remaining, 64<<20)
		if errors.Is(err, io.EOF) {
			break
		}
		mustNil(t, err)
		remaining -= fr.size
		lsn, typ, body := mutate(i, fr.lsn, fr.typ, append([]byte{}, fr.body...))
		out = appendFrame(out, lsn, typ, body)
	}
	mustNil(t, os.WriteFile(path, out, 0o600))
}

func signedOpts(sg secure.Signer) WALOptions {
	return WALOptions{Signer: sg, Sync: SyncNone, DisableAutoCompact: true}
}

func writeSigned(t testing.TB, dir string, sg secure.Signer, n int) {
	t.Helper()
	ctx := context.Background()
	s := openInternal(t, dir, signedOpts(sg))
	appendN(t, s, n)
	ls, err := s.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Hour})
	mustNil(t, err)
	mustNil(t, s.Checkpoint(ctx, ls[0].Record.ID, ls[0].Token, []byte("progress")))
	mustNil(t, s.Close())
}

func lastSegment(t testing.TB, dir string) string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(m) == 0 {
		t.Fatal("no segments")
	}
	return m[len(m)-1]
}

func TestSignedStoresPassTheConformanceSuiteAndRoundTrip(t *testing.T) {
	sg := hmacSigner(t, "k1", 1)
	dir := t.TempDir()
	writeSigned(t, dir, sg, 5)
	s := openInternal(t, dir, signedOpts(sg))
	defer func() { _ = s.Close() }()
	g, err := s.Get(context.Background(), "r000")
	mustNil(t, err)
	if !bytes.Equal(g.Value, payload(0)) || string(g.Checkpoint) != "progress" {
		t.Fatalf("after reopen: %q %q", g.Value, g.Checkpoint)
	}
	if rr := s.Recovery(); rr.UnsignedFiles != 0 || rr.Entries == 0 {
		t.Fatalf("recovery %+v", rr)
	}
}

func TestOpeningASignedLogNeedsTheSignerAndTheRightKey(t *testing.T) {
	dir := t.TempDir()
	writeSigned(t, dir, hmacSigner(t, "k1", 1), 3)

	_, err := OpenWAL(dir, WALOptions{Sync: SyncNone})
	if !errors.Is(err, ErrSignerRequired) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("no signer: %v, want ErrSignerRequired", err)
	}
	_, err = OpenWAL(dir, signedOpts(hmacSigner(t, "k1", 2))) // same id, different key
	if !errors.Is(err, ErrTampered) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("wrong key: %v, want ErrTampered", err)
	}
	_, err = OpenWAL(dir, signedOpts(hmacSigner(t, "other", 1))) // unknown key id
	if !errors.Is(err, ErrTampered) {
		t.Fatalf("unknown key id: %v, want ErrTampered", err)
	}
}

func TestKeyRotationKeepsOldEntriesReadable(t *testing.T) {
	dir := t.TempDir()
	old := secure.Key{ID: "k1", Material: bytes.Repeat([]byte{1}, 32)}
	writeSigned(t, dir, hmacSigner(t, "k1", 1), 3)
	rotated := hmacSigner(t, "k2", 2, old)
	s := openInternal(t, dir, signedOpts(rotated))
	mustNil(t, s.Append(context.Background(), Record{ID: "new", Value: []byte("n")}))
	mustNil(t, s.Close())
	s = openInternal(t, dir, signedOpts(rotated))
	defer func() { _ = s.Close() }()
	if _, err := s.Get(context.Background(), "new"); err != nil {
		t.Fatal(err)
	}
}

// Recomputing the CRC is trivial; the signature is what stops a forged entry.
func TestAForgedEntryIsRefusedEvenInTheLastSegment(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(i int, lsn uint64, typ byte, body []byte) (uint64, byte, []byte)
	}{
		{"body byte flipped", func(i int, lsn uint64, typ byte, body []byte) (uint64, byte, []byte) {
			if i == 1 {
				body[0] ^= 1
			}
			return lsn, typ, body
		}},
		{"last entry flipped", func(i int, lsn uint64, typ byte, body []byte) (uint64, byte, []byte) {
			if i == 4 {
				body[len(body)/2] ^= 1
			}
			return lsn, typ, body
		}},
		{"type changed", func(i int, lsn uint64, typ byte, body []byte) (uint64, byte, []byte) {
			if i == 2 {
				typ = entPark
			}
			return lsn, typ, body
		}},
		{"signature removed", func(i int, lsn uint64, typ byte, body []byte) (uint64, byte, []byte) {
			if i == 1 {
				n := int(binary.LittleEndian.Uint16(body[len(body)-2:]))
				body = body[:len(body)-2-n]
				body = binary.LittleEndian.AppendUint16(body, 0)
			}
			return lsn, typ, body
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sg := hmacSigner(t, "k1", 1)
			dir := t.TempDir()
			writeSigned(t, dir, sg, 4)
			forgeFrames(t, lastSegment(t, dir), tc.mutate)
			_, err := OpenWAL(dir, signedOpts(sg))
			if !errors.Is(err, ErrTampered) || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("open = %v, want ErrTampered", err)
			}
			if m, _ := filepath.Glob(filepath.Join(dir, "*.quarantine-*")); len(m) != 0 {
				t.Fatalf("a forged entry was repaired away (quarantine %v): tampering must never be repaired", m)
			}
		})
	}
}

// An entry copied over another one has a valid signature of its own; binding the
// LSN and type into the signature is what makes the copy fail.
func TestAnEntryCopiedOverAnotherIsRefused(t *testing.T) {
	sg := hmacSigner(t, "k1", 1)
	dir := t.TempDir()
	writeSigned(t, dir, sg, 4)
	var keepBody []byte
	var keepTyp byte
	forgeFrames(t, lastSegment(t, dir), func(i int, lsn uint64, typ byte, body []byte) (uint64, byte, []byte) {
		switch i {
		case 1:
			keepBody, keepTyp = append([]byte{}, body...), typ
		case 2:
			return lsn, keepTyp, keepBody // the LSN stays right; the content is another entry's
		}
		return lsn, typ, body
	})
	if _, err := OpenWAL(dir, signedOpts(sg)); !errors.Is(err, ErrTampered) {
		t.Fatalf("open = %v, want ErrTampered", err)
	}
}

func TestAForgedSnapshotIsRefused(t *testing.T) {
	ctx := context.Background()
	sg := hmacSigner(t, "k1", 1)
	dir := t.TempDir()
	s := openInternal(t, dir, signedOpts(sg))
	appendN(t, s, 5)
	mustNil(t, s.Compact(ctx))
	mustNil(t, s.Close())
	snaps, _ := filepath.Glob(filepath.Join(dir, "snapshot-*.snap"))
	if len(snaps) != 1 {
		t.Fatalf("snapshots %v", snaps)
	}
	forgeFrames(t, snaps[0], func(i int, lsn uint64, typ byte, body []byte) (uint64, byte, []byte) {
		if i == 2 {
			body[3] ^= 1
		}
		return lsn, typ, body
	})
	if _, err := OpenWAL(dir, signedOpts(sg)); !errors.Is(err, ErrTampered) {
		t.Fatalf("open = %v, want ErrTampered", err)
	}
}

// A payload changed on disk after the store opened is not handed out.
func TestAForgedPayloadIsNeverHandedOut(t *testing.T) {
	ctx := context.Background()
	sg := hmacSigner(t, "k1", 1)
	dir := t.TempDir()
	s := openInternal(t, dir, signedOpts(sg))
	defer func() { _ = s.Close() }()
	appendN(t, s, 3)
	forgeFrames(t, lastSegment(t, dir), func(i int, lsn uint64, typ byte, body []byte) (uint64, byte, []byte) {
		if i == 1 {
			body[len(body)/3] ^= 1 // valid CRC, wrong signature
		}
		return lsn, typ, body
	})
	s.files.closeAll() // forget cached handles: the file was replaced
	s.files = newFileCache(dir, 8)
	if _, err := s.Get(ctx, "r001"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get = %v, want ErrCorrupt", err)
	}
	ls, err := s.Lease(ctx, LeaseRequest{Max: 3, TTL: time.Hour})
	mustNil(t, err)
	if len(ls) != 2 {
		t.Fatalf("leased %d records, want the two untouched ones", len(ls))
	}
	if s.WALStats().UnreadablePayloads != 1 {
		t.Fatal("the forged record was not parked as unreadable")
	}
}

func TestMovingAnExistingLogToSigning(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	plain := openInternal(t, dir, WALOptions{Sync: SyncNone, DisableAutoCompact: true})
	appendN(t, plain, 10)
	mustNil(t, plain.Close())

	sg := hmacSigner(t, "k1", 1)
	if _, err := OpenWAL(dir, signedOpts(sg)); !errors.Is(err, ErrUnsignedLog) {
		t.Fatalf("open = %v, want ErrUnsignedLog without AcceptUnsignedLegacy", err)
	}

	o := signedOpts(sg)
	o.AcceptUnsignedLegacy = true
	s := openInternal(t, dir, o)
	if rr := s.Recovery(); rr.UnsignedFiles == 0 || rr.Records != 10 {
		t.Fatalf("recovery %+v: want the legacy files counted and every record kept", rr)
	}
	mustNil(t, s.Append(ctx, Record{ID: "signed-new", Value: []byte("n")}))
	mustNil(t, s.Close())

	// The new entry went to a fresh signed segment, not into a legacy one.
	raw, err := os.ReadFile(lastSegment(t, dir))
	mustNil(t, err)
	if _, flags, err := decodeHeaderFlags(raw[:headerSize], walMagic); err != nil || flags&flagSigned == 0 {
		t.Fatalf("the newest segment is not signed (flags %#x, %v)", flags, err)
	}

	s = openInternal(t, dir, o)
	mustNil(t, s.Compact(ctx)) // writes a signed snapshot and retires the legacy files
	mustNil(t, s.Close())
	s = openInternal(t, dir, signedOpts(sg)) // legacy files no longer accepted, and none remain
	defer func() { _ = s.Close() }()
	if rr := s.Recovery(); rr.UnsignedFiles != 0 || rr.Records != 11 {
		t.Fatalf("recovery %+v: want everything signed and 11 records", rr)
	}
	for i := 0; i < 10; i++ {
		if g, err := s.Get(ctx, fmt.Sprintf("r%03d", i)); err != nil || !bytes.Equal(g.Value, payload(i)) {
			t.Fatalf("r%03d after migration: %v %q", i, err, g.Value)
		}
	}
}

func TestSignedFramesAreLargerByExactlyTheSignature(t *testing.T) {
	sg := hmacSigner(t, "k1", 1)
	a := openInternal(t, t.TempDir(), WALOptions{Sync: SyncNone, DisableAutoCompact: true})
	b := openInternal(t, t.TempDir(), signedOpts(sg))
	defer func() { _ = a.Close(); _ = b.Close() }()
	appendN(t, a, 1)
	appendN(t, b, 1)
	sigLen := len(sg.Sign([]byte("x")))
	if d := b.logBytes - a.logBytes; d != int64(sigLen+2) {
		t.Fatalf("a signed entry is %d bytes larger, want %d", d, sigLen+2)
	}
}
