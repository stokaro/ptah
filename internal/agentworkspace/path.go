package agentworkspace

import (
	"fmt"
	"path"
	"slices"
	"strings"
)

// reservedWindowsNames are the device names Win32 resolves before it ever
// reaches the filesystem, with or without an extension.
//
// They are refused on every platform, not only on Windows, for the reason
// AGENTS.md states about the `atlas.hcl` confinement rule: a rule about what a
// file is allowed to be called must not depend on the machine reading the file.
// A migration named `CON.sql` written on Linux is a repository that cannot be
// checked out on Windows, and the agent that wrote it will not be there when
// that is discovered.
var reservedWindowsNames = []string{
	"CON", "PRN", "AUX", "NUL",
	"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
	"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
}

// maxSegmentBytes is the longest single path component. 255 is the limit almost
// every filesystem shares; a longer name fails at write time on most machines
// and at checkout time on the rest.
const maxSegmentBytes = 255

// validateRelativePath refuses every spelling that means something other than
// it looks like, and returns the cleaned path for the ones that do not.
//
// The list is not defensive programming. Each rule below refuses a spelling
// that reached a real caller somewhere: an absolute path pasted from an error
// message, a `..` composed by string concatenation, a Windows path handed to a
// slash-separated API, a drive letter that turns out to be a directory name on
// Linux, a trailing space that Win32 silently strips so two names become one.
//
// The kernel-level containment is [pathguard.OpenedDirectory]'s and stands on
// its own. This exists because a refusal naming the rule is a better answer than
// a rooted open failing with a system error, and because two of these -- the
// device names and the trailing separators -- are legal to the kernel and still
// wrong.
func validateRelativePath(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("path is empty: %w", ErrUnsafePath)
	}
	if err := refuseUnsafeSpelling(raw); err != nil {
		return "", err
	}
	for segment := range strings.SplitSeq(raw, "/") {
		if err := validateSegment(raw, segment); err != nil {
			return "", err
		}
	}
	clean := path.Clean(raw)
	if clean != raw {
		// Clean is used as a comparison rather than as a repair. A caller whose
		// path needed cleaning wrote something other than what it meant, and
		// accepting the cleaned form would be Ptah deciding which of the two
		// readings was intended.
		return "", fmt.Errorf("path %q is not in its plain form (%q): %w", raw, clean, ErrUnsafePath)
	}
	return clean, nil
}

// refuseUnsafeSpelling catches the whole-string properties.
func refuseUnsafeSpelling(raw string) error {
	if strings.HasPrefix(raw, "/") {
		return fmt.Errorf("path %q is absolute: %w", raw, ErrUnsafePath)
	}
	if strings.ContainsRune(raw, '\\') {
		return fmt.Errorf("path %q contains a backslash: %w", raw, ErrUnsafePath)
	}
	if hasDriveLetter(raw) {
		return fmt.Errorf("path %q names a drive: %w", raw, ErrUnsafePath)
	}
	for _, char := range raw {
		if char < 0x20 || char == 0x7f {
			return fmt.Errorf("path %q contains a control character: %w", raw, ErrUnsafePath)
		}
	}
	return nil
}

// hasDriveLetter reports a `C:` prefix, which is a drive on Windows and an
// ordinary directory name on Linux -- so a repository written on one is
// unusable on the other.
func hasDriveLetter(raw string) bool {
	if len(raw) < 2 || raw[1] != ':' {
		return false
	}
	letter := raw[0]
	return (letter >= 'a' && letter <= 'z') || (letter >= 'A' && letter <= 'Z')
}

// validateSegment catches the per-component properties.
func validateSegment(raw, segment string) error {
	switch segment {
	case "":
		return fmt.Errorf("path %q has an empty component: %w", raw, ErrUnsafePath)
	case ".":
		return fmt.Errorf("path %q has a %q component: %w", raw, ".", ErrUnsafePath)
	case "..":
		return fmt.Errorf("path %q leaves the artifact scope: %w", raw, ErrUnsafePath)
	}
	if len(segment) > maxSegmentBytes {
		return fmt.Errorf("path %q has a component longer than %d bytes: %w",
			raw, maxSegmentBytes, ErrUnsafePath)
	}
	if strings.HasSuffix(segment, " ") || strings.HasSuffix(segment, ".") {
		return fmt.Errorf("path %q has a component ending in a space or a dot: %w", raw, ErrUnsafePath)
	}
	if isReservedDeviceName(segment) {
		return fmt.Errorf("path %q names a reserved device: %w", raw, ErrUnsafePath)
	}
	return nil
}

// isReservedDeviceName reports a Win32 device name, extension or not.
func isReservedDeviceName(segment string) bool {
	stem, _, _ := strings.Cut(segment, ".")
	return slices.Contains(reservedWindowsNames, strings.ToUpper(stem))
}

// FoldKey is the name two paths share when a case-insensitive filesystem cannot
// tell them apart.
//
// It exists because pathguard folds nothing -- its containment check is a byte
// comparison -- and because the failure that gap produces is quiet: on APFS or
// NTFS a patch naming `Users.sql` and `users.sql` is two changes and one file,
// so the second publication finds a destination that does not match what it
// expected and the patch half-applies. Detecting the collision while planning
// turns that into a refusal naming both paths.
//
// Simple Unicode case folding, which is what those filesystems approximate. It
// is not exact -- NTFS folds by a stored table and APFS by a normalization form
// -- and it does not have to be: the check refuses a collision, so being
// stricter than the filesystem costs a rejected patch and being looser would
// cost a half-applied one.
func FoldKey(relative string) string {
	return strings.ToLower(relative)
}
