//go:build !windows

package testutils

// ExecutableSuffix is empty: every operating system but Windows decides
// executability from the file's mode rather than from its name.
const ExecutableSuffix = ""
