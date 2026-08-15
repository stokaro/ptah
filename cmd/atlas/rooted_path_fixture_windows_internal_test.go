//go:build windows

package atlas

// White-box testing required: the rooted-path tests in this package are
// internal, and this is their fixture.

import "strings"

// rootedFixturePath renders an absolute path for the platform. On Windows that
// needs a volume name: "/project" is rooted on the current drive but
// filepath.IsAbs answers false for it.
func rootedFixturePath(elements ...string) string {
	return `C:\` + strings.Join(elements, `\`)
}
