//go:build !windows

package testutils

import "testing"

// SkipWithoutPOSIXShell does nothing here: /bin/sh is available, so a fixture
// written as a shell script runs.
func SkipWithoutPOSIXShell(testing.TB) {}
