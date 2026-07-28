//go:build !windows

package goannotationcleanup

import "os"

func replaceFile(source, target string) error {
	return os.Rename(source, target)
}
