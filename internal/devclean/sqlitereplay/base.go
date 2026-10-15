package sqlitereplay

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

func validateSQLiteReplayStatement(tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}

	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "ATTACH", "DETACH":
		return replay.Refuse(platform.SQLite, first)
	case "CREATE":
		if createsSQLiteTemporaryObject(tokens) {
			return replay.Refuse(platform.SQLite, "TEMP object")
		}
	case "PRAGMA":
		if !isRestorableSQLitePragma(tokens) {
			return replay.Refuse(platform.SQLite, "state-changing or unsupported PRAGMA")
		}
	case "VACUUM":
		if replay.ContainsIdentifier(tokens[1:], "INTO") {
			return replay.Refuse(platform.SQLite, "VACUUM INTO")
		}
	}

	if replay.ContainsIdentifier(tokens, "LOAD_EXTENSION") {
		return replay.Refuse(platform.SQLite, "load_extension")
	}
	if mutatesSQLiteRealm(first) && containsQualifiedIdentifier(tokens, "temp") {
		return replay.Refuse(platform.SQLite, "TEMP schema target")
	}
	return nil
}

func createsSQLiteTemporaryObject(tokens []lexer.Token) bool {
	if len(tokens) < 2 {
		return false
	}
	modifier := replay.NormalizedIdentifier(tokens[1])
	return modifier == "TEMP" || modifier == "TEMPORARY"
}

func isRestorableSQLitePragma(tokens []lexer.Token) bool {
	if len(tokens) < 2 {
		return true
	}
	return replay.NormalizedIdentifier(tokens[1]) == "FOREIGN_KEYS"
}

func containsQualifiedIdentifier(tokens []lexer.Token, value string) bool {
	for index := range len(tokens) - 1 {
		if replay.NormalizedIdentifier(tokens[index]) == strings.ToUpper(value) &&
			tokens[index+1].MatchOperatorValue(".") {
			return true
		}
	}
	return false
}

func mutatesSQLiteRealm(first string) bool {
	switch first {
	case "ALTER", "CREATE", "DELETE", "DROP", "INSERT", "PRAGMA", "REINDEX",
		"REPLACE", "UPDATE", "VACUUM":
		return true
	default:
		return false
	}
}
