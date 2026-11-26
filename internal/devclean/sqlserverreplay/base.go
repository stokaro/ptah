package sqlserverreplay

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

func validateSQLServerReplayStatementBase(tokens []lexer.Token, scope replay.Scope) error {
	if len(tokens) == 0 {
		return nil
	}
	first := replay.NormalizedIdentifier(tokens[0])
	if first == "USE" {
		return replay.Refuse(platform.SQLServer, first)
	}
	if first == "EXEC" || first == "EXECUTE" {
		if sqlServerRemoteExecute(tokens) && !scope.TrustedSQL {
			return replay.Refuse(platform.SQLServer, "remote EXECUTE")
		}
	}
	if isGlobalSQLServerDDL(tokens) && (!scope.TrustedSQL || !sqlServerUnconfinedServerObject(tokens)) {
		return replay.Refuse(platform.SQLServer, replay.DescribeStatementObject(tokens))
	}
	if replay.UsesTemporaryObject(tokens) || containsTemporaryTarget(tokens) {
		return replay.Refuse(platform.SQLServer, "temporary object")
	}
	if scope.TrustedSQL {
		return nil
	}
	if operation := sqlServerOutsideRead(tokens); operation != "" {
		return replay.Refuse(platform.SQLServer, operation)
	}
	if sqlServerKeywordSequenceAt(tokens, 0, "ADD", "SIGNATURE") || sqlServerKeywordSequenceAt(tokens, 0, "ADD", "COUNTER", "SIGNATURE") {
		return replay.Refuse(platform.SQLServer, "ADD SIGNATURE")
	}
	targets := sqlServerMutationTargets(tokens)
	targets = append(targets, sqlServerWriteTargets(tokens, sqlServerExecutableTokens(tokens))...)
	for _, target := range targets {
		if len(target) >= 3 {
			return replay.Refuse(
				platform.SQLServer,
				fmt.Sprintf("cross-database target %q", strings.Join(target, ".")),
			)
		}
	}
	return nil
}

// sqlServerRemoteExecute reports the AT of EXECUTE ... AT linked_server: the
// word AT, unquoted, that is not part of a name or a variable. A string or a
// bracketed name that spells at, such as the new name N'at', is not it.
func sqlServerRemoteExecute(tokens []lexer.Token) bool {
	for index, token := range tokens {
		previous := replay.TokenAt(tokens, index-1)
		if token.MatchIdentifierValue("AT") && !previous.MatchOperatorValue("@") && !previous.MatchOperatorValue(".") {
			return true
		}
	}
	return false
}

// sqlServerOutsideRead returns the rowset function in tokens that reads
// outside the dev database, wherever it stands: OPENROWSET reads a file on the
// server, as OPENROWSET(BULK '/etc/hostname', SINGLE_CLOB) does, or a remote
// source, and OPENDATASOURCE and OPENQUERY read a remote source. It refuses a
// write into one of them too, as INSERT OPENQUERY(...) VALUES (1) is. A
// bracketed or quoted name is a name: SQL Server 2025 refuses
// [OPENROWSET](BULK ...) with Msg 156.
func sqlServerOutsideRead(tokens []lexer.Token) string {
	for index := range tokens {
		next := replay.TokenAt(tokens, index+1)
		if !next.MatchOperatorValue("(") {
			continue
		}
		for _, name := range []string{"OPENROWSET", "OPENDATASOURCE", "OPENQUERY"} {
			if tokens[index].MatchIdentifierValue(name) {
				return name
			}
		}
	}
	return ""
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

// sqlServerUnconfinedServerObject reports a statement [isGlobalSQLServerDDL]
// names whose only reason to be refused is that its object is not the dev
// database's: CREATE or DROP of a database or a login, a server credential in
// any form, and CREATE or ALTER of a synonym, which the realm cleanup removes
// and which can name an object outside the dev database.
//
// ALTER DATABASE and a database's own scoped objects -- DATABASE SCOPED
// CREDENTIAL or CONFIGURATION, DATABASE AUDIT SPECIFICATION, DATABASE
// ENCRYPTION KEY -- are not: they configure the dev database itself, or leave
// in it what the cleanup does not remove. Nor is ALTER LOGIN, which can change
// the credentials the run connects with, nor an ENDPOINT, an AVAILABILITY
// GROUP, a SERVER object or an EXTERNAL object other than a table, which
// configure the server or stay in the dev database after the cleanup.
func sqlServerUnconfinedServerObject(tokens []lexer.Token) bool {
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return false
	}
	first := replay.NormalizedIdentifier(tokens[0])
	switch replay.NormalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE":
		switch replay.NormalizedIdentifier(replay.TokenAt(tokens, kindIndex+1)) {
		case "SCOPED", "AUDIT", "ENCRYPTION":
			return false
		}
		return first == "CREATE" || first == "DROP"
	case "LOGIN":
		return first == "CREATE" || first == "DROP"
	case "CREDENTIAL", "SYNONYM":
		return true
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
