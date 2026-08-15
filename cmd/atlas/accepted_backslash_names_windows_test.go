//go:build windows

package atlas_test

// acceptedBackslashNames is empty here. On Windows a backslash IS a path
// separator, so `a\b` names two elements and the rule that refuses a path
// separator in a migration name refuses it -- correctly, and for the same
// reason the rest of this file exists.
//
// The acceptance measurement was taken on a platform where the character is
// ordinary, so asserting it here would assert something the oracle never said.
func acceptedBackslashNames() []acceptedNameCase { return nil }
