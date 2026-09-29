//go:build unix

package dlq

import (
	"os"
	"syscall"
)

// ownedByTrustedUser reports whether the file belongs to the user running the
// process or to root. A file owned by anyone else can be rewritten by that user
// whatever its mode says now, so its contents cannot be trusted.
func ownedByTrustedUser(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return true
	}
	uid := uint32(os.Geteuid()) //nolint:gosec // a uid is never negative
	return st.Uid == uid || st.Uid == 0
}
