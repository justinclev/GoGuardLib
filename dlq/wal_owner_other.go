//go:build !unix

package dlq

import "os"

func ownedByTrustedUser(os.FileInfo) bool { return true }
