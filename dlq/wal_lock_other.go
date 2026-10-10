//go:build !unix

package dlq

import "errors"

type dirLock struct{}

// The durable store needs an exclusive directory lock, which is only implemented
// on Unix. Refusing to open is safer than running without one.
func lockDir(string) (*dirLock, error) {
	return nil, errors.New("dlq: the WAL store requires a Unix platform (directory locking is not implemented here)")
}

func (l *dirLock) unlock() {}
