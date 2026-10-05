package ydbstream

import "slices"

// LosesCheckpoint identifies SQL operations that discard streaming-query state.
// Safety assessment and migration lint must recognize the same operations.
// Words are uppercase SQL tokens with comments and literals excluded.
func LosesCheckpoint(words []string) bool {
	if startsWith(words, "DROP", "STREAMING", "QUERY") || startsWith(words, "CREATE", "OR", "REPLACE", "STREAMING", "QUERY") {
		return true
	}
	if !startsWith(words, "ALTER", "STREAMING", "QUERY") {
		return false
	}
	for i := 3; i < len(words); i++ {
		if startsWith(words[i:], "AS", "DO", "BEGIN") {
			return true
		}
	}
	return false
}

func startsWith(words []string, prefix ...string) bool {
	return len(words) >= len(prefix) && slices.Equal(words[:len(prefix)], prefix)
}
