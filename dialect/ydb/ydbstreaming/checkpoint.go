package ydbstreaming

import "slices"

// ExecutionChange explains the risk of changing continuous execution settings.
const ExecutionChange = "ALTER STREAMING QUERY changes continuous query execution"

// CheckpointLoss explains why declaration rollback cannot recover query state.
const CheckpointLoss = "the operation removes or resets streaming-query checkpoint state; a rollback restores the declaration, not the discarded state"

// LosesCheckpoint identifies SQL operations that discard streaming-query state.
// Safety assessment and migration lint must recognize the same operations.
// Words are uppercase SQL tokens with comments and literals excluded.
func LosesCheckpoint(words []string) bool {
	if startsWith(words, "DROP", "STREAMING", "QUERY") {
		return true
	}
	if startsWith(words, "CREATE", "OR", "REPLACE", "STREAMING", "QUERY") {
		return (CreateOptions{OrReplace: true, IfNotExists: startsWith(words[5:], "IF", "NOT", "EXISTS")}).ReplacesExisting()
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
