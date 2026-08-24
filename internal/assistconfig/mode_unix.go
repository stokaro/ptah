//go:build !windows

package assistconfig

// modeBitsAreMeaningful reports whether a file's permission bits say who can
// read it.
//
// They do here, so the group and other bits are checked.
func modeBitsAreMeaningful() bool { return true }
