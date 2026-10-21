package backend

import "strings"

// NormalizeCommonIdentifier follows the fixture's declared ASCII-only folding.
func NormalizeCommonIdentifier(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, name)
}
