//go:build unix

package dlq

import "syscall"

// freeBytes reports the space available to an unprivileged writer on the volume
// holding dir.
func freeBytes(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil //nolint:unconvert // the field types differ between platforms
}
