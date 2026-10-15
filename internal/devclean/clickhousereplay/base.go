package clickhousereplay

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

func validateClickHouseReplayStatementBase(database string, tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "USE", "SYSTEM":
		return replay.Refuse(platform.ClickHouse, first)
	case "SET":
		if replay.TokenSequenceAt(tokens, 1, "GLOBAL") {
			return replay.Refuse(platform.ClickHouse, "global SET")
		}
	}
	if replay.ContainsTokenSequence(tokens, "ON", "CLUSTER") {
		return replay.Refuse(platform.ClickHouse, "ON CLUSTER")
	}
	if isGlobalClickHouseDDL(tokens) {
		return replay.Refuse(platform.ClickHouse, replay.DescribeStatementObject(tokens))
	}
	if replay.UsesTemporaryObject(tokens) {
		return replay.Refuse(platform.ClickHouse, "TEMP object")
	}
	if replay.ContainsIdentifier(tokens, "INTO") &&
		(replay.ContainsIdentifier(tokens, "OUTFILE") || replay.ContainsIdentifier(tokens, "DUMPFILE")) {
		return replay.Refuse(platform.ClickHouse, "external file operation")
	}
	return rejectCrossDatabaseTargets(platform.ClickHouse, database, tokens)
}

func isGlobalClickHouseDDL(tokens []lexer.Token) bool {
	if !replay.IsDDLAction(tokens) {
		return false
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return false
	}
	switch replay.NormalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE", "USER", "ROLE", "QUOTA", "FUNCTION", "WORKLOAD", "RESOURCE":
		return true
	case "ROW":
		return replay.TokenSequenceAt(tokens, kindIndex, "ROW", "POLICY")
	case "SETTINGS":
		return replay.TokenSequenceAt(tokens, kindIndex, "SETTINGS", "PROFILE")
	case "NAMED":
		return replay.TokenSequenceAt(tokens, kindIndex, "NAMED", "COLLECTION")
	default:
		return false
	}
}

func rejectCrossDatabaseTargets(dialect, database string, tokens []lexer.Token) error {
	if strings.TrimSpace(database) == "" {
		return replay.Refuse(dialect, "qualified target without a configured database")
	}
	for _, target := range replay.MutationTargets(tokens) {
		if len(target) < 2 || target[0] == database {
			continue
		}
		return replay.Refuse(
			dialect,
			fmt.Sprintf("cross-database target %q", strings.Join(target, ".")),
		)
	}
	return nil
}
