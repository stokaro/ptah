package clickhousereplay

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

func validateClickHouseReplayStatementBase(tokens []lexer.Token, scope replay.Scope) error {
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
	if replay.ContainsTokenSequence(tokens, "ON", "CLUSTER") && !scope.TrustedSQL {
		return replay.Refuse(platform.ClickHouse, "ON CLUSTER")
	}
	if isGlobalClickHouseDDL(tokens) && (!scope.TrustedSQL || !clickHouseUnconfinedServerObject(tokens)) {
		return replay.Refuse(platform.ClickHouse, replay.DescribeStatementObject(tokens))
	}
	if replay.UsesTemporaryObject(tokens) {
		return replay.Refuse(platform.ClickHouse, "TEMP object")
	}
	if scope.TrustedSQL {
		return nil
	}
	if replay.ContainsIdentifier(tokens, "INTO") &&
		(replay.ContainsIdentifier(tokens, "OUTFILE") || replay.ContainsIdentifier(tokens, "DUMPFILE")) {
		return replay.Refuse(platform.ClickHouse, "external file operation")
	}
	return rejectCrossDatabaseTargets(platform.ClickHouse, scope.Database, tokens)
}

// clickHouseUnconfinedServerObject reports a statement [isGlobalClickHouseDDL]
// names whose only reason to be refused is that its object is the server's
// rather than the dev database's: a SQL function, a row policy or a named
// collection in any form, and CREATE or DROP of a database, a user or a role.
// ALTER of a database, a user or a role is not, since it can change the dev
// database or the identity the run connects as, and neither is a quota, a
// settings profile, a workload or a resource, which limit or schedule the run's
// own work.
func clickHouseUnconfinedServerObject(tokens []lexer.Token) bool {
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return false
	}
	first := replay.NormalizedIdentifier(tokens[0])
	switch replay.NormalizedIdentifier(tokens[kindIndex]) {
	case "FUNCTION":
		return true
	case "ROW":
		return replay.TokenSequenceAt(tokens, kindIndex, "ROW", "POLICY")
	case "NAMED":
		return replay.TokenSequenceAt(tokens, kindIndex, "NAMED", "COLLECTION")
	case "DATABASE", "USER", "ROLE":
		return first == "CREATE" || first == "DROP"
	default:
		return false
	}
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
