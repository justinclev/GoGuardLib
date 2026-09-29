//go:build !unix

package dlq

import "errors"

func freeBytes(string) (uint64, error) {
	return 0, errors.New("dlq: free space is not available on this platform")
}
