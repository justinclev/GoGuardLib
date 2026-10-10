//go:build !linux

package dlq

func networkFS(string) (string, bool) { return "", false }
