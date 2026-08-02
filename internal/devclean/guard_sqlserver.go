package devclean

import (
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/lexer"
)

// validateSQLServerReplayStatement rejects SQL Server mutations whose
// effects cannot be removed by the database-realm cleanup.
func validateSQLServerReplayStatement(tokens []lexer.Token) error {
	if err := validateSQLServerReplayStatementBase(tokens); err != nil {
		return err
	}
	if len(tokens) == 0 {
		return nil
	}
	replayTokens := tokens
	tokens = sqlServerExecutableTokens(tokens)

	first := normalizedIdentifier(tokens[0])
	if first == "EXEC" || first == "EXECUTE" {
		return unsafeReplayStatement(platform.SQLServer, "EXEC/EXECUTE sublanguage")
	}
	if operation := sqlServerExternalOrServerOperation(tokens); operation != "" {
		return unsafeReplayStatement(platform.SQLServer, operation)
	}
	if first == "GRANT" || first == "DENY" || first == "REVOKE" {
		return unsafeReplayStatement(platform.SQLServer, "permission mutation")
	}
	if sqlServerHasGlobalScope(tokens) {
		return unsafeReplayStatement(platform.SQLServer, "ON SERVER or ON ALL SERVER")
	}
	if sqlServerHasDatabaseTriggerScope(tokens) {
		return unsafeReplayStatement(
			platform.SQLServer,
			first+" DATABASE DDL TRIGGER",
		)
	}
	if sqlServerDefinesExecutableBody(tokens) {
		return unsafeReplayStatement(platform.SQLServer, first+" executable stored body")
	}
	if sqlServerSelectIntoEscapesRealm(tokens) {
		return unsafeReplayStatement(platform.SQLServer, "cross-database SELECT INTO")
	}
	if sqlServerSelectIntoTemporaryObject(tokens) {
		return unsafeReplayStatement(platform.SQLServer, "temporary SELECT INTO target")
	}
	destination := sqlServerMutationRowsetDestination(tokens)
	if destination == "" {
		destination = sqlServerMutationRowsetCTEDestination(replayTokens, tokens)
	}
	if destination != "" {
		return unsafeReplayStatement(
			platform.SQLServer,
			destination+" mutation destination",
		)
	}
	if sqlServerExternalTableWritesData(tokens) {
		return unsafeReplayStatement(
			platform.SQLServer,
			"CREATE EXTERNAL TABLE AS SELECT",
		)
	}
	if sqlServerRejectsExternalTableDDL(tokens) {
		return unsafeReplayStatement(platform.SQLServer, first+" EXTERNAL TABLE")
	}
	if sqlServerEnablesLedger(tokens) {
		return unsafeReplayStatement(platform.SQLServer, "LEDGER table")
	}
	if operation := sqlServerUnsupportedRealmOperation(tokens); operation != "" {
		return unsafeReplayStatement(platform.SQLServer, operation)
	}
	return nil
}

func sqlServerRejectsExternalTableDDL(tokens []lexer.Token) bool {
	return normalizedIdentifier(tokens[0]) != "DROP" &&
		sqlServerDDLStartsWith(tokens, "EXTERNAL", "TABLE")
}

func sqlServerExternalOrServerOperation(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "BACKUP", "RESTORE":
		return first + " external storage operation"
	case "BULK":
		if sqlServerKeywordSequenceAt(tokens, 1, "INSERT") {
			return "BULK INSERT external data operation"
		}
	case "DBCC":
		if !sqlServerDatabaseLocalDBCC(tokens) {
			return first + " server operation"
		}
	case "KILL", "RECONFIGURE", "SHUTDOWN":
		return first + " server operation"
	}
	return ""
}

func sqlServerDefinesExecutableBody(tokens []lexer.Token) bool {
	if normalizedIdentifier(tokens[0]) == "DROP" &&
		sqlServerDDLStartsWith(tokens, "FUNCTION") {
		return false
	}
	return sqlServerDDLStartsWith(tokens, "FUNCTION") ||
		sqlServerDDLStartsWith(tokens, "PROCEDURE") ||
		sqlServerDDLStartsWith(tokens, "PROC") ||
		sqlServerDDLStartsWith(tokens, "TRIGGER")
}

func sqlServerDatabaseLocalDBCC(tokens []lexer.Token) bool {
	return sqlServerKeywordSequenceAt(tokens, 1, "CHECKIDENT") ||
		sqlServerKeywordSequenceAt(tokens, 1, "CHECKTABLE") ||
		sqlServerKeywordSequenceAt(tokens, 1, "CHECKCONSTRAINTS")
}

func sqlServerHasGlobalScope(tokens []lexer.Token) bool {
	if !sqlServerTriggerStatement(tokens) &&
		!sqlServerDDLStartsWith(tokens, "EVENT", "SESSION") &&
		!sqlServerDDLStartsWith(tokens, "EVENT", "NOTIFICATION") {
		return false
	}
	return sqlServerContainsKeywordSequence(tokens, "ON", "SERVER") ||
		sqlServerContainsKeywordSequence(tokens, "ON", "ALL", "SERVER")
}

func sqlServerHasDatabaseTriggerScope(tokens []lexer.Token) bool {
	return sqlServerTriggerStatement(tokens) &&
		sqlServerContainsKeywordSequence(tokens, "ON", "DATABASE")
}

func sqlServerTriggerStatement(tokens []lexer.Token) bool {
	if sqlServerDDLStartsWith(tokens, "TRIGGER") {
		return true
	}
	if len(tokens) < 2 {
		return false
	}
	first := normalizedIdentifier(tokens[0])
	return (first == "ENABLE" || first == "DISABLE") &&
		sqlServerKeywordSequenceAt(tokens, 1, "TRIGGER")
}

func sqlServerExecutableTokens(tokens []lexer.Token) []lexer.Token {
	if len(tokens) == 0 || !tokens[0].MatchIdentifierValue("WITH") {
		return tokens
	}
	depth := 0
	closedDefinition := false
	for index := 1; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth--
			closedDefinition = depth == 0
		case depth == 0 && closedDefinition &&
			sqlServerDMLStatementAt(tokens, index):
			return tokens[index:]
		}
	}
	return tokens
}

func sqlServerDMLStatementAt(tokens []lexer.Token, index int) bool {
	return sqlServerKeywordSequenceAt(tokens, index, "SELECT") ||
		sqlServerKeywordSequenceAt(tokens, index, "INSERT") ||
		sqlServerKeywordSequenceAt(tokens, index, "UPDATE") ||
		sqlServerKeywordSequenceAt(tokens, index, "DELETE") ||
		sqlServerKeywordSequenceAt(tokens, index, "MERGE")
}

func sqlServerSelectIntoEscapesRealm(tokens []lexer.Token) bool {
	targetIndex := sqlServerSelectIntoTargetIndex(tokens)
	return sqlServerQualifiedNameArityAt(tokens, targetIndex) >= 3
}

func sqlServerSelectIntoTemporaryObject(tokens []lexer.Token) bool {
	targetIndex := sqlServerSelectIntoTargetIndex(tokens)
	if targetIndex < 0 || targetIndex >= len(tokens) {
		return false
	}
	target := normalizedIdentifier(tokens[targetIndex])
	return len(target) > 0 && target[0] == '#'
}

func sqlServerSelectIntoTargetIndex(tokens []lexer.Token) int {
	if len(tokens) == 0 {
		return mutationTargetNotFound
	}
	first := normalizedIdentifier(tokens[0])
	if first != "SELECT" && first != "WITH" {
		return mutationTargetNotFound
	}
	intoIndex := sqlServerFindKeyword(tokens, "INTO", 1)
	if intoIndex == mutationTargetNotFound {
		return mutationTargetNotFound
	}
	return intoIndex + 1
}

func sqlServerQualifiedNameArityAt(tokens []lexer.Token, index int) int {
	if index < 0 || index >= len(tokens) || identifierValue(tokens[index]) == "" {
		return 0
	}
	arity := 1
	index++
	for index < len(tokens) && tokens[index].MatchOperatorValue(".") {
		arity++
		index++
		if index < len(tokens) && identifierValue(tokens[index]) != "" {
			index++
		}
	}
	return arity
}

func sqlServerMutationRowsetDestination(tokens []lexer.Token) string {
	index := sqlServerMutationDestinationIndex(tokens)
	if index == mutationTargetNotFound {
		return ""
	}
	if destination := sqlServerRowsetFunctionAt(tokens, index); destination != "" {
		return destination
	}
	return sqlServerMutationRowsetAliasDestination(tokens, index)
}

func sqlServerRowsetFunctionAt(tokens []lexer.Token, index int) string {
	if index < 0 ||
		index+1 >= len(tokens) ||
		!tokens[index+1].MatchOperatorValue("(") {
		return ""
	}
	switch {
	case tokens[index].MatchIdentifierValue("OPENQUERY"):
		return "OPENQUERY"
	case tokens[index].MatchIdentifierValue("OPENROWSET"):
		return "OPENROWSET"
	default:
		return ""
	}
}

func sqlServerMutationRowsetAliasDestination(tokens []lexer.Token, targetIndex int) string {
	first := normalizedIdentifier(tokens[0])
	if first != "UPDATE" && first != "DELETE" {
		return ""
	}
	if targetIndex+1 < len(tokens) && tokens[targetIndex+1].MatchOperatorValue(".") {
		return ""
	}
	target := normalizedIdentifier(tokens[targetIndex])
	fromIndex := sqlServerFindKeyword(tokens, "FROM", targetIndex+1)
	if target == "" || fromIndex == mutationTargetNotFound {
		return ""
	}
	for index := fromIndex + 1; index < len(tokens); index++ {
		destination := sqlServerRowsetFunctionAt(tokens, index)
		if destination == "" {
			continue
		}
		aliasIndex := skipSQLServerParenthesized(tokens, index+1)
		if sqlServerKeywordSequenceAt(tokens, aliasIndex, "AS") {
			aliasIndex++
		}
		if aliasIndex < len(tokens) &&
			normalizedIdentifier(tokens[aliasIndex]) == target {
			return destination
		}
	}
	return ""
}

func sqlServerMutationRowsetCTEDestination(
	replayTokens,
	executableTokens []lexer.Token,
) string {
	if len(replayTokens) == 0 ||
		!replayTokens[0].MatchIdentifierValue("WITH") ||
		len(executableTokens) >= len(replayTokens) {
		return ""
	}
	targetIndex := sqlServerMutationDestinationIndex(executableTokens)
	if targetIndex == mutationTargetNotFound ||
		targetIndex+1 < len(executableTokens) &&
			executableTokens[targetIndex+1].MatchOperatorValue(".") {
		return ""
	}
	target := normalizedIdentifier(executableTokens[targetIndex])
	definitionTokens := replayTokens[:len(replayTokens)-len(executableTokens)]
	return sqlServerCTERowsetDefinition(definitionTokens, target)
}

func sqlServerCTERowsetDefinition(tokens []lexer.Token, target string) string {
	for index := 1; index < len(tokens); {
		name := normalizedIdentifier(tokens[index])
		index++
		if index < len(tokens) && tokens[index].MatchOperatorValue("(") {
			index = skipSQLServerParenthesized(tokens, index)
		}
		if !sqlServerKeywordSequenceAt(tokens, index, "AS") {
			return ""
		}
		index++
		if index >= len(tokens) || !tokens[index].MatchOperatorValue("(") {
			return ""
		}
		bodyEnd := skipSQLServerParenthesized(tokens, index)
		if name == target {
			return sqlServerRowsetFunctionInRange(tokens, index+1, bodyEnd-1)
		}
		index = bodyEnd
		if index >= len(tokens) || !tokens[index].MatchOperatorValue(",") {
			return ""
		}
		index++
	}
	return ""
}

func sqlServerRowsetFunctionInRange(tokens []lexer.Token, start, end int) string {
	for index := max(start, 0); index < min(end, len(tokens)); index++ {
		if destination := sqlServerRowsetFunctionAt(tokens, index); destination != "" {
			return destination
		}
	}
	return ""
}

func sqlServerFindKeyword(tokens []lexer.Token, value string, start int) int {
	for index := max(start, 0); index < len(tokens); index++ {
		if tokens[index].MatchIdentifierValue(value) {
			return index
		}
	}
	return mutationTargetNotFound
}

func sqlServerContainsKeywordSequence(tokens []lexer.Token, values ...string) bool {
	for index := range len(tokens) {
		if sqlServerKeywordSequenceAt(tokens, index, values...) {
			return true
		}
	}
	return false
}

func sqlServerKeywordSequenceAt(tokens []lexer.Token, index int, values ...string) bool {
	if index < 0 || index+len(values) > len(tokens) {
		return false
	}
	for offset, value := range values {
		if !tokens[index+offset].MatchIdentifierValue(value) {
			return false
		}
	}
	return true
}

func sqlServerDDLStartsWith(tokens []lexer.Token, values ...string) bool {
	if !isDDLAction(tokens) {
		return false
	}
	index := 1
	if sqlServerKeywordSequenceAt(tokens, index, "OR", "ALTER") {
		index += 2
	}
	return sqlServerKeywordSequenceAt(tokens, index, values...)
}

func sqlServerMutationDestinationIndex(tokens []lexer.Token) int {
	if len(tokens) == 0 {
		return mutationTargetNotFound
	}
	index := skipSQLServerTopClause(tokens, 1)
	switch normalizedIdentifier(tokens[0]) {
	case "INSERT", "MERGE":
		if sqlServerKeywordSequenceAt(tokens, index, "INTO") {
			index++
		}
	case "UPDATE":
	case "DELETE":
		if sqlServerKeywordSequenceAt(tokens, index, "FROM") {
			index++
		}
	default:
		return mutationTargetNotFound
	}
	if index >= len(tokens) {
		return mutationTargetNotFound
	}
	return index
}

func skipSQLServerTopClause(tokens []lexer.Token, index int) int {
	if !sqlServerKeywordSequenceAt(tokens, index, "TOP") {
		return index
	}
	index++
	if index < len(tokens) && tokens[index].MatchOperatorValue("(") {
		index = skipSQLServerParenthesized(tokens, index)
	} else if index < len(tokens) {
		index++
	}
	if sqlServerKeywordSequenceAt(tokens, index, "PERCENT") {
		index++
	}
	return index
}

func skipSQLServerParenthesized(tokens []lexer.Token, index int) int {
	depth := 0
	for index < len(tokens) {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return index + 1
			}
		}
		index++
	}
	return index
}

func sqlServerExternalTableWritesData(tokens []lexer.Token) bool {
	if !tokens[0].MatchIdentifierValue("CREATE") ||
		!sqlServerDDLStartsWith(tokens, "EXTERNAL", "TABLE") {
		return false
	}
	depth := 0
	for index := 3; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth--
		case depth == 0 && tokens[index].MatchIdentifierValue("AS"):
			return true
		}
	}
	return false
}

func sqlServerEnablesLedger(tokens []lexer.Token) bool {
	if !sqlServerDDLStartsWith(tokens, "TABLE") {
		return false
	}
	for index := 0; index+2 < len(tokens); index++ {
		if tokens[index].MatchIdentifierValue("LEDGER") &&
			tokens[index+1].MatchOperatorValue("=") &&
			tokens[index+2].MatchIdentifierValue("ON") {
			return true
		}
	}
	return false
}

func sqlServerUnsupportedRealmOperation(tokens []lexer.Token) string {
	if !isDDLAction(tokens) {
		return ""
	}
	first := normalizedIdentifier(tokens[0])
	if sqlServerDDLStartsWith(tokens, "USER") ||
		sqlServerDDLStartsWith(tokens, "ROLE") ||
		sqlServerDDLStartsWith(tokens, "APPLICATION", "ROLE") {
		return first + " DATABASE PRINCIPAL"
	}

	patterns := [...]struct {
		sequence  []string
		operation string
	}{
		{sequence: []string{"AUTHORIZATION"}, operation: first + " AUTHORIZATION"},
		{sequence: []string{"ASSEMBLY"}, operation: first + " ASSEMBLY"},
		{sequence: []string{"PARTITION", "FUNCTION"}, operation: first + " PARTITION FUNCTION"},
		{sequence: []string{"PARTITION", "SCHEME"}, operation: first + " PARTITION SCHEME"},
		{sequence: []string{"FULLTEXT", "CATALOG"}, operation: first + " FULLTEXT CATALOG"},
		{sequence: []string{"FULLTEXT", "STOPLIST"}, operation: first + " FULLTEXT STOPLIST"},
		{sequence: []string{"SEARCH", "PROPERTY", "LIST"}, operation: first + " SEARCH PROPERTY LIST"},
		{sequence: []string{"CERTIFICATE"}, operation: first + " CERTIFICATE"},
		{sequence: []string{"COLUMN", "MASTER", "KEY"}, operation: first + " COLUMN MASTER KEY"},
		{sequence: []string{"COLUMN", "ENCRYPTION", "KEY"}, operation: first + " COLUMN ENCRYPTION KEY"},
		{sequence: []string{"MASTER", "KEY"}, operation: first + " MASTER KEY"},
		{sequence: []string{"SYMMETRIC", "KEY"}, operation: first + " SYMMETRIC KEY"},
		{sequence: []string{"ASYMMETRIC", "KEY"}, operation: first + " ASYMMETRIC KEY"},
		{sequence: []string{"EVENT", "NOTIFICATION"}, operation: first + " EVENT NOTIFICATION"},
		{sequence: []string{"EVENT", "SESSION"}, operation: first + " EVENT SESSION"},
		{sequence: []string{"MESSAGE", "TYPE"}, operation: first + " MESSAGE TYPE"},
		{sequence: []string{"REMOTE", "SERVICE", "BINDING"}, operation: first + " REMOTE SERVICE BINDING"},
		{sequence: []string{"BROKER", "PRIORITY"}, operation: first + " BROKER PRIORITY"},
		{sequence: []string{"CONTRACT"}, operation: first + " CONTRACT"},
		{sequence: []string{"QUEUE"}, operation: first + " QUEUE"},
		{sequence: []string{"SERVICE"}, operation: first + " SERVICE"},
		{sequence: []string{"ROUTE"}, operation: first + " ROUTE"},
		{sequence: []string{"RESOURCE", "POOL"}, operation: first + " RESOURCE POOL"},
		{sequence: []string{"RESOURCE", "GOVERNOR"}, operation: first + " RESOURCE GOVERNOR"},
		{sequence: []string{"WORKLOAD", "GROUP"}, operation: first + " WORKLOAD GROUP"},
		{sequence: []string{"CRYPTOGRAPHIC", "PROVIDER"}, operation: first + " CRYPTOGRAPHIC PROVIDER"},
	}
	for _, pattern := range patterns {
		if sqlServerDDLStartsWith(tokens, pattern.sequence...) {
			return pattern.operation
		}
	}
	return ""
}
