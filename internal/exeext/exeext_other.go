//go:build !windows

package exeext

// Suffix is empty: every operating system but Windows decides executability
// from the file's mode rather than from its name.
const Suffix = ""
