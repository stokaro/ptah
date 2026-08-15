//go:build windows

package testutils

import "fmt"

// FailingHookCommand renders the same hook for `cmd /C`: "&" separates the
// commands, and `exit /b` sets the process status rather than closing the
// shell's parent.
func FailingHookCommand(message string, code int) string {
	return fmt.Sprintf("echo %s& exit /b %d", message, code)
}
