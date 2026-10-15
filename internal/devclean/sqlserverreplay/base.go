package sqlserverreplay

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

func validateSQLServerReplayStatementBase(tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}
	first := replay.NormalizedIdentifier(tokens[0])
	if first == "USE" {
		return replay.Refuse(platform.SQLServer, first)
	}
	if first == "EXEC" || first == "EXECUTE" {
		if replay.ContainsIdentifier(tokens, "AT") {
			return replay.Refuse(platform.SQLServer, "remote EXECUTE")
		}
	}
	if isGlobalSQLServerDDL(tokens) {
		return replay.Refuse(platform.SQLServer, replay.DescribeStatementObject(tokens))
	}
	if replay.UsesTemporaryObject(tokens) || containsTemporaryTarget(tokens) {
		return replay.Refuse(platform.SQLServer, "temporary object")
	}
	for _, target := range sqlServerMutationTargets(tokens) {
		if len(target) >= 3 {
			return replay.Refuse(
				platform.SQLServer,
				fmt.Sprintf("cross-database target %q", strings.Join(target, ".")),
			)
		}
	}
	return nil
}

func isGlobalSQLServerDDL(tokens []lexer.Token) bool {
	if !replay.IsDDLAction(tokens) {
		return false
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return false
	}
	switch replay.NormalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE", "LOGIN", "CREDENTIAL", "ENDPOINT", "AVAILABILITY":
		return true
	case "SERVER":
		return true
	case "SYNONYM":
		return replay.NormalizedIdentifier(tokens[0]) != "DROP"
	case "EXTERNAL":
		return !replay.TokenSequenceAt(tokens, kindIndex, "EXTERNAL", "TABLE")
	default:
		return false
	}
}

func containsTemporaryTarget(tokens []lexer.Token) bool {
	for _, target := range replay.MutationTargets(tokens) {
		for _, part := range target {
			if strings.HasPrefix(part, "#") {
				return true
			}
		}
	}
	return false
}

func sqlServerMutationTargets(tokens []lexer.Token) [][]string {
	if replay.NormalizedIdentifier(tokens[0]) != "DROP" {
		return replay.MutationTargets(tokens)
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return replay.MutationTargets(tokens)
	}
	if replay.NormalizedIdentifier(tokens[kindIndex]) == "INDEX" {
		return sqlServerDropIndexTargets(tokens, kindIndex+1)
	}
	return replay.CommaSeparatedQualifiedTargets(
		tokens,
		replay.ObjectNameIndex(tokens, kindIndex),
		"",
	)
}

func sqlServerDropIndexTargets(tokens []lexer.Token, start int) [][]string {
	var targets [][]string
	for index := start; index < len(tokens); index++ {
		if !tokens[index].MatchIdentifierValue("ON") {
			continue
		}
		target, next := replay.QualifiedIdentifierAt(tokens, index+1)
		if len(target) != 0 {
			targets = append(targets, target)
		}
		index = next - 1
	}
	return targets
}
