//go:build unix

package dlq

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// dirLock keeps two processes (or two stores) from writing the same directory,
// which would interleave and corrupt the log. It is an flock on a LOCK file, so
// the OS releases it if the process dies.
type dirLock struct{ f *os.File }

func lockDir(dir string) (*dirLock, error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
		}
		return nil, err
	}
	return &dirLock{f: f}, nil
}

func (l *dirLock) unlock() {
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
}
