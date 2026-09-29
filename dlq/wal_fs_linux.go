//go:build linux

package dlq

import "syscall"

// networkFS names the filesystem holding dir if it is one whose locking or
// durability semantics the log cannot rely on (network and user-space mounts).
func networkFS(dir string) (string, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return "", false // cannot tell: do not block on a measurement that failed
	}
	switch int64(st.Type) { //nolint:unconvert // the field type differs between architectures
	case 0x6969:
		return "nfs", true
	case 0x517B:
		return "smb", true
	case 0xFF534D42:
		return "cifs", true
	case 0x65735546:
		return "fuse", true
	case 0x5346414F:
		return "afs", true
	case 0x00C36400:
		return "ceph", true
	case 0x01021997:
		return "9p", true
	}
	return "", false
}
