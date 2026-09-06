//go:build unix

package migratetests_test

import "ptah.run/cmd/atlas/internal/atlastest"

// acceptedBackslashNames carries the row that only holds where a backslash is
// an ordinary character in a file name. The pinned binary was measured here:
// `migrate new 'a\b'` wrote a file and exited 0.
func acceptedBackslashNames() []atlastest.AcceptedNameCase {
	return []atlastest.AcceptedNameCase{{Name: "backslash", Given: `a\b`}}
}
