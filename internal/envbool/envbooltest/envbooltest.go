// Package envbooltest carries the environment manipulation that every boolean
// `PTAH_*` contract test needs, so the four states the contract distinguishes
// are spelled the same way wherever they are exercised.
//
// It exists because the states are not symmetric and the asymmetry is the whole
// point. `t.Setenv(name, "")` EXPORTS an empty value, which the contract refuses;
// "absent" is a different state that a table row cannot reach by passing an
// empty string. Writing that difference out per package invited exactly the row
// that means to test "unset" and tests "" instead.
//
// The steps are values a table row carries, which is what keeps the test bodies
// free of the branching the repository's test-style gate rejects.
package envbooltest

import (
	"os"
	"testing"
)

// Set exports name with value for the duration of the test.
func Set(name, value string) func(testing.TB) {
	return func(t testing.TB) {
		t.Helper()
		t.Setenv(name, value)
	}
}

// Unset removes name for the duration of the test.
//
// The Setenv call before the Unsetenv call is load-bearing twice over: it is
// what registers the restore of whatever the ambient environment held, and it is
// what makes the test fail loudly rather than silently skip when it is run in
// parallel with another test.
func Unset(name string) func(testing.TB) {
	return func(t testing.TB) {
		t.Helper()
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
}
