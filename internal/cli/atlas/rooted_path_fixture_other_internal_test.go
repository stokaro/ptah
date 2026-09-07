//go:build !windows

package atlas

// White-box testing required: the rooted-path tests in this package are
// internal, and this is their fixture.

import "strings"

// rootedFixturePath renders an absolute path for the platform.
//
// A literal "/project/missing" is absolute here and is NOT on Windows, where
// filepath.IsAbs wants a volume name -- so a test written with one asserts the
// relative branch there and never reaches the behavior it was written for.
func rootedFixturePath(elements ...string) string {
	return "/" + strings.Join(elements, "/")
}
