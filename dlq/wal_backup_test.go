package dlq

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestBackupIsAConsistentVerifiableCopy(t *testing.T) {
	src := t.TempDir()
	s := openF(t, src, nil, WALOptions{SegmentBytes: 2048}) // several segments
	defer func() { _ = s.Close() }()
	for i := 0; i < 40; i++ {
		addRecords(t, s, "rec-"+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	dest := filepath.Join(t.TempDir(), "bk")
	if err := s.Backup(context.Background(), dest); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if err := s.Backup(context.Background(), dest); err == nil {
		t.Fatal("a second backup into a non-empty directory must be refused")
	}
	addRecords(t, s, "after-backup") // the live store carries on

	rep, err := VerifyWAL(dest, WALOptions{})
	if err != nil {
		t.Fatalf("VerifyWAL(backup): %v", err)
	}
	if rep.Stats.Total() != 40 {
		t.Fatalf("backup holds %d records, want the 40 acknowledged before it", rep.Stats.Total())
	}
	b, err := OpenWAL(dest, WALOptions{})
	if err != nil {
		t.Fatalf("the backup does not open as a store: %v", err)
	}
	_ = b.Close()
}

func TestVerifyWALDetectsDamageAndDoesNotTouchTheOriginal(t *testing.T) {
	src := t.TempDir()
	s := openF(t, src, nil, WALOptions{})
	addRecords(t, s, "a", "b", "c", "d")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadDir(src)
	if _, err := VerifyWAL(src, WALOptions{}); err != nil {
		t.Fatalf("a healthy log failed verification: %v", err)
	}
	after, _ := os.ReadDir(src)
	if len(before) != len(after) {
		t.Fatalf("verification changed the directory: %d files before, %d after", len(before), len(after))
	}

	seg := lastSeg(t, src)
	data, _ := os.ReadFile(seg)
	data[len(data)/2] ^= 0xff // damage in the middle: bit rot, not a torn tail
	if err := os.WriteFile(seg, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyWAL(src, WALOptions{}); err == nil {
		t.Fatal("a damaged log passed verification")
	}
}
