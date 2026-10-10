package dlq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// isLogFile reports whether name is a segment or snapshot: the files that make up
// a log. LOCK and unfinished snapshots are not part of it.
func isLogFile(name string) bool {
	if _, ok := parseName(name, "wal-", ".log"); ok {
		return true
	}
	_, ok := parseName(name, "snapshot-", ".snap")
	return ok
}

// copyPrefix copies the first n bytes of src to dst (0600) and syncs it. n < 0
// copies the whole file.
func copyPrefix(src, dst string, n int64) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	var r io.Reader = in
	if n >= 0 {
		r = io.LimitReader(in, n)
	}
	if _, err := io.Copy(out, r); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// Backup writes a consistent copy of the log into dest, which must be empty or not
// yet exist, while the store keeps serving. Compaction is held off for the
// duration; appends and leases continue, and whatever they add after Backup began
// is simply not in the copy (the copy is the log as it stood, a valid prefix).
// Everything acknowledged before Backup was called is in it. The files are copied
// as they are: still sealed or signed, so a backup is as protected as the log and
// can be checked with VerifyWAL and opened with the same keys.
func (w *WALStore) Backup(ctx context.Context, dest string) error {
	if err := os.MkdirAll(dest, 0o700); err != nil {
		return err
	}
	if entries, err := os.ReadDir(dest); err != nil {
		return err
	} else if len(entries) > 0 {
		return fmt.Errorf("dlq: backup destination %s is not empty", dest)
	}

	w.compactMu.Lock() // no segment may be deleted while we copy
	defer w.compactMu.Unlock()

	type file struct {
		name string
		size int64
	}
	w.mu.Lock()
	if err := w.begin(ctx); err != nil {
		w.mu.Unlock()
		return err
	}
	if err := w.active.Sync(); err != nil {
		w.mu.Unlock()
		return w.failAndWake(fmt.Errorf("fsync for backup: %w", err))
	}
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	activeName := filepath.Base(w.segments[len(w.segments)-1].path)
	var files []file
	for _, e := range entries {
		if e.IsDir() || !isLogFile(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			w.mu.Unlock()
			return err
		}
		size := info.Size()
		if e.Name() == activeName {
			size = w.activeSize // later appends are not part of this copy
		}
		files = append(files, file{e.Name(), size})
	}
	w.mu.Unlock()

	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := copyPrefix(filepath.Join(w.dir, f.name), filepath.Join(dest, f.name), f.size); err != nil {
			return fmt.Errorf("dlq: copying %s: %w", f.name, err)
		}
	}
	return syncDir(dest)
}

// VerifyReport is what VerifyWAL found.
type VerifyReport struct {
	Recovery RecoveryReport
	Stats    StoreStats
}

// VerifyWAL checks that the log in dir can be opened and read from end to end:
// every frame's checksum (and signature, if opts.Signer is set) holds, the
// sequence has no gap, and the snapshot loads. It works on a private copy, so it
// never modifies dir, takes no lock on it, and is safe to run beside a live store,
// though a store being written may show a torn tail that a stopped one would not;
// verify a Backup, or a stopped store, for a definitive answer. A damaged tail is
// reported as an error rather than repaired. Sealed payloads are not decrypted.
func VerifyWAL(dir string, opts WALOptions) (VerifyReport, error) {
	s, cleanup, err := OpenWALCopy(dir, opts)
	if err != nil {
		return VerifyReport{}, err
	}
	defer cleanup()
	st, err := s.Stats(context.Background())
	if err != nil {
		return VerifyReport{}, err
	}
	return VerifyReport{Recovery: s.Recovery(), Stats: st}, nil
}

// OpenWALCopy opens a private copy of the log in dir, for inspection: dir is only
// read, its lock is not taken (so a running service is undisturbed), and a damaged
// log is an error, not repaired. The copy lives in a temporary directory that the
// returned cleanup function closes the store and deletes; call it when done. The
// same caveat as VerifyWAL applies to a store being written.
func OpenWALCopy(dir string, opts WALOptions) (*WALStore, func(), error) {
	tmp, err := os.MkdirTemp("", "goguard-copy-")
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (*WALStore, func(), error) {
		_ = os.RemoveAll(tmp)
		return nil, nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fail(err)
	}
	found := false
	for _, e := range entries {
		if e.IsDir() || !isLogFile(e.Name()) {
			continue
		}
		found = true
		if err := copyPrefix(filepath.Join(dir, e.Name()), filepath.Join(tmp, e.Name()), -1); err != nil {
			return fail(err)
		}
	}
	if !found {
		return fail(errors.New("dlq: no log files in " + dir))
	}
	opts.FailOnTruncation = true
	opts.DisableAutoCompact = true
	opts.AllowInsecurePermissions = true // the copy is ours; the original's permissions are not under test
	opts.AllowNetworkFilesystem = true
	opts.Events = nil
	s, err := OpenWAL(tmp, opts)
	if err != nil {
		return fail(err)
	}
	return s, func() { _ = s.Close(); _ = os.RemoveAll(tmp) }, nil
}
