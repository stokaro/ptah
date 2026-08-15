//go:build !windows

package testutils

import "fmt"

// FailingHookCommand renders a pre-flight hook that prints message and exits
// with code, in the shell Ptah runs hooks through on this platform.
//
// internal/preflight picks the shell per platform -- `/bin/sh -c` here, `cmd
// /C` on Windows -- so the feature is portable and only the command's grammar
// is not. A fixture written as `echo m; exit 9` runs under cmd without the
// separator or the status meaning what it says, and the hook then succeeds,
// which is the opposite of what such a test asserts.
func FailingHookCommand(message string, code int) string {
	return fmt.Sprintf("echo %s; exit %d", message, code)
}
