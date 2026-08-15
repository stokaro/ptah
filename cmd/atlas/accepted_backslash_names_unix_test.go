//go:build unix

package atlas_test

// acceptedBackslashNames carries the row that only holds where a backslash is
// an ordinary character in a file name. The pinned binary was measured here:
// `migrate new 'a\b'` wrote a file and exited 0.
func acceptedBackslashNames() []acceptedNameCase {
	return []acceptedNameCase{{name: "backslash", given: `a\b`}}
}
