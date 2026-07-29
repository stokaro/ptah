package devclean

import (
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/lexer"
)

const mutationTargetNotFound = -1

func replayLexerOptions(dialect string) lexer.Options {
	options := lexer.Options{StandardStrings: true}
	switch dialect {
	case platform.MySQL, platform.MariaDB, platform.ClickHouse:
		options.BackslashEscapes = true
	case platform.SQLServer:
		options.BracketIdentifiers = true
		options.DisableHashComments = true
	case platform.SQLite:
		options.BracketIdentifiers = true
	}
	return options
}

func validatePostgresReplayStatementBase(dialect string, tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}
	first := normalizedIdentifier(tokens[0])
	if first == "COPY" && isExternalPostgresCopy(tokens) {
		return unsafeReplayStatement(dialect, "external COPY")
	}
	if first == "ALTER" && tokenSequenceAt(tokens, 1, "SYSTEM") {
		return unsafeReplayStatement(dialect, "ALTER SYSTEM")
	}
	if first == "SET" && tokenSequenceAt(tokens, 1, "ROLE") {
		return unsafeReplayStatement(dialect, "SET ROLE")
	}
	if first == "SET" && tokenSequenceAt(tokens, 1, "SESSION", "AUTHORIZATION") {
		return unsafeReplayStatement(dialect, "SET SESSION AUTHORIZATION")
	}
	if isGlobalPostgresDDL(tokens) {
		return unsafeReplayStatement(dialect, describeStatementObject(tokens))
	}
	if usesTemporaryObject(tokens) {
		return unsafeReplayStatement(dialect, "TEMP object")
	}
	return nil
}

func validateMySQLReplayStatement(dialect, database string, tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "USE", "INSTALL", "UNINSTALL", "FLUSH", "RESET":
		return unsafeReplayStatement(dialect, first)
	case "SET":
		if tokenSequenceAt(tokens, 1, "GLOBAL") ||
			tokenSequenceAt(tokens, 1, "PERSIST") ||
			tokenSequenceAt(tokens, 1, "PERSIST_ONLY") {
			return unsafeReplayStatement(dialect, "global or persistent SET")
		}
	case "GRANT", "REVOKE":
		return unsafeReplayStatement(dialect, "privilege or role mutation")
	case "PREPARE", "EXECUTE", "DEALLOCATE":
		return unsafeReplayStatement(dialect, "prepared statement")
	case "LOCK":
		if tokenSequenceAt(tokens, 1, "TABLES") {
			return unsafeReplayStatement(dialect, "LOCK TABLES")
		}
	case "ALTER":
		if tokenSequenceAt(tokens, 1, "INSTANCE") {
			return unsafeReplayStatement(dialect, "ALTER INSTANCE")
		}
	}
	if isGlobalMySQLDDL(tokens) {
		return unsafeReplayStatement(dialect, describeStatementObject(tokens))
	}
	if usesTemporaryObject(tokens) {
		return unsafeReplayStatement(dialect, "TEMP object")
	}
	if containsIdentifier(tokens, "OUTFILE") ||
		containsIdentifier(tokens, "DUMPFILE") ||
		containsIdentifier(tokens, "SONAME") {
		return unsafeReplayStatement(dialect, "external file operation")
	}
	if err := rejectMySQLRenameDestinations(dialect, database, tokens); err != nil {
		return err
	}
	return rejectCrossDatabaseTargets(dialect, database, tokens)
}

func rejectMySQLRenameDestinations(dialect, database string, tokens []lexer.Token) error {
	if tokenSequenceAt(tokens, 0, "RENAME", "TABLE") ||
		tokenSequenceAt(tokens, 0, "ALTER", "TABLE") && containsIdentifier(tokens, "RENAME") {
		return rejectTargetsAfterKeyword(dialect, database, tokens, "TO")
	}
	return nil
}

func validateSQLServerReplayStatementBase(tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}
	first := normalizedIdentifier(tokens[0])
	if first == "USE" {
		return unsafeReplayStatement(platform.SQLServer, first)
	}
	if first == "EXEC" || first == "EXECUTE" {
		if containsIdentifier(tokens, "AT") {
			return unsafeReplayStatement(platform.SQLServer, "remote EXECUTE")
		}
	}
	if isGlobalSQLServerDDL(tokens) {
		return unsafeReplayStatement(platform.SQLServer, describeStatementObject(tokens))
	}
	if usesTemporaryObject(tokens) || containsTemporaryTarget(tokens) {
		return unsafeReplayStatement(platform.SQLServer, "temporary object")
	}
	for _, target := range mutationTargets(tokens) {
		if len(target) >= 3 {
			return unsafeReplayStatement(
				platform.SQLServer,
				fmt.Sprintf("cross-database target %q", strings.Join(target, ".")),
			)
		}
	}
	return nil
}

func validateClickHouseReplayStatementBase(database string, tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "USE", "SYSTEM":
		return unsafeReplayStatement(platform.ClickHouse, first)
	case "SET":
		if tokenSequenceAt(tokens, 1, "GLOBAL") {
			return unsafeReplayStatement(platform.ClickHouse, "global SET")
		}
	}
	if containsTokenSequence(tokens, "ON", "CLUSTER") {
		return unsafeReplayStatement(platform.ClickHouse, "ON CLUSTER")
	}
	if isGlobalClickHouseDDL(tokens) {
		return unsafeReplayStatement(platform.ClickHouse, describeStatementObject(tokens))
	}
	if usesTemporaryObject(tokens) {
		return unsafeReplayStatement(platform.ClickHouse, "TEMP object")
	}
	if containsIdentifier(tokens, "INTO") &&
		(containsIdentifier(tokens, "OUTFILE") || containsIdentifier(tokens, "DUMPFILE")) {
		return unsafeReplayStatement(platform.ClickHouse, "external file operation")
	}
	return rejectCrossDatabaseTargets(platform.ClickHouse, database, tokens)
}

func isGlobalPostgresDDL(tokens []lexer.Token) bool {
	if !isDDLAction(tokens) {
		return false
	}
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound {
		return false
	}
	switch normalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE", "ROLE", "USER", "TABLESPACE", "SUBSCRIPTION",
		"PUBLICATION", "CAST", "LANGUAGE", "TRANSFORM":
		return true
	case "EVENT":
		return tokenSequenceAt(tokens, kindIndex, "EVENT", "TRIGGER")
	case "FOREIGN":
		return !tokenSequenceAt(tokens, kindIndex, "FOREIGN", "TABLE")
	case "SERVER":
		return true
	case "TEXT":
		return tokenSequenceAt(tokens, kindIndex, "TEXT", "SEARCH")
	case "ACCESS":
		return tokenSequenceAt(tokens, kindIndex, "ACCESS", "METHOD")
	default:
		return false
	}
}

func isGlobalMySQLDDL(tokens []lexer.Token) bool {
	if !isDDLAction(tokens) {
		return false
	}
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound {
		return false
	}
	switch normalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE", "SCHEMA", "USER", "ROLE", "SERVER", "TABLESPACE",
		"UNDO", "LOGFILE", "RESOURCE":
		return true
	case "SPATIAL":
		return tokenSequenceAt(tokens, kindIndex, "SPATIAL", "REFERENCE", "SYSTEM")
	default:
		return false
	}
}

func isGlobalSQLServerDDL(tokens []lexer.Token) bool {
	if !isDDLAction(tokens) {
		return false
	}
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound {
		return false
	}
	switch normalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE", "LOGIN", "CREDENTIAL", "ENDPOINT", "AVAILABILITY":
		return true
	case "SERVER":
		return true
	case "EXTERNAL":
		return !tokenSequenceAt(tokens, kindIndex, "EXTERNAL", "TABLE")
	default:
		return false
	}
}

func isGlobalClickHouseDDL(tokens []lexer.Token) bool {
	if !isDDLAction(tokens) {
		return false
	}
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound {
		return false
	}
	switch normalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE", "USER", "ROLE", "QUOTA", "FUNCTION", "WORKLOAD", "RESOURCE":
		return true
	case "ROW":
		return tokenSequenceAt(tokens, kindIndex, "ROW", "POLICY")
	case "SETTINGS":
		return tokenSequenceAt(tokens, kindIndex, "SETTINGS", "PROFILE")
	case "NAMED":
		return tokenSequenceAt(tokens, kindIndex, "NAMED", "COLLECTION")
	default:
		return false
	}
}

func isExternalPostgresCopy(tokens []lexer.Token) bool {
	if containsIdentifier(tokens, "PROGRAM") {
		return true
	}
	toIndex := findIdentifier(tokens, "TO", 1)
	if toIndex == mutationTargetNotFound || toIndex+1 >= len(tokens) {
		return false
	}
	return tokens[toIndex+1].Type == lexer.TokenString &&
		normalizedIdentifier(tokens[toIndex+1]) != "STDOUT"
}

func isDDLAction(tokens []lexer.Token) bool {
	if len(tokens) == 0 {
		return false
	}
	switch normalizedIdentifier(tokens[0]) {
	case "CREATE", "ALTER", "DROP", "RENAME":
		return true
	default:
		return false
	}
}

func usesTemporaryObject(tokens []lexer.Token) bool {
	if !isDDLAction(tokens) {
		return false
	}
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound {
		return false
	}
	return containsIdentifier(tokens[1:kindIndex], "TEMP") ||
		containsIdentifier(tokens[1:kindIndex], "TEMPORARY")
}

func containsTemporaryTarget(tokens []lexer.Token) bool {
	for _, target := range mutationTargets(tokens) {
		for _, part := range target {
			if strings.HasPrefix(part, "#") {
				return true
			}
		}
	}
	return false
}

func rejectCrossDatabaseTargets(dialect, database string, tokens []lexer.Token) error {
	if strings.TrimSpace(database) == "" {
		return unsafeReplayStatement(dialect, "qualified target without a configured database")
	}
	for _, target := range mutationTargets(tokens) {
		if len(target) < 2 || target[0] == database {
			continue
		}
		return unsafeReplayStatement(
			dialect,
			fmt.Sprintf("cross-database target %q", strings.Join(target, ".")),
		)
	}
	return nil
}

func rejectTargetsAfterKeyword(
	dialect,
	database string,
	tokens []lexer.Token,
	keyword string,
) error {
	for index := 1; index < len(tokens); index++ {
		if normalizedIdentifier(tokens[index]) != keyword {
			continue
		}
		target, _ := qualifiedIdentifierAt(tokens, skipNameModifiers(tokens, index+1))
		if len(target) < 2 || target[0] == database {
			continue
		}
		return unsafeReplayStatement(
			dialect,
			fmt.Sprintf("cross-database target %q", strings.Join(target, ".")),
		)
	}
	return nil
}

func mutationTargets(tokens []lexer.Token) [][]string {
	if len(tokens) == 0 {
		return nil
	}
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "INSERT", "REPLACE", "MERGE":
		return targetAfterKeywordWithModifiers(tokens, "INTO", 1, "TABLE")
	case "UPDATE":
		return targetAfterModifiers(tokens, 1, "LOW_PRIORITY", "IGNORE", "ONLY")
	case "DELETE":
		return targetAfterKeyword(tokens, "FROM", 1)
	case "TRUNCATE":
		return targetAfterModifiers(tokens, 1, "TABLE", "ONLY")
	case "OPTIMIZE", "ANALYZE", "REPAIR":
		return targetAfterKeyword(tokens, "TABLE", 1)
	case "CREATE", "ALTER", "DROP":
		return ddlMutationTargets(tokens)
	case "RENAME":
		return targetAfterKeyword(tokens, "TABLE", 1)
	case "COMMENT":
		return targetAfterKeyword(tokens, "ON", 1)
	default:
		return nil
	}
}

func targetAfterKeywordWithModifiers(
	tokens []lexer.Token,
	keyword string,
	start int,
	modifiers ...string,
) [][]string {
	index := findIdentifier(tokens, keyword, start)
	if index == mutationTargetNotFound {
		return nil
	}
	return targetAfterModifiers(tokens, skipNameModifiers(tokens, index+1), modifiers...)
}

func ddlMutationTargets(tokens []lexer.Token) [][]string {
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound {
		return nil
	}
	kind := normalizedIdentifier(tokens[kindIndex])
	if kind == "INDEX" || tokenSequenceAt(tokens, kindIndex, "UNIQUE", "INDEX") ||
		tokenSequenceAt(tokens, kindIndex, "FULLTEXT", "INDEX") ||
		tokenSequenceAt(tokens, kindIndex, "SPATIAL", "INDEX") ||
		tokenSequenceAt(tokens, kindIndex, "CLUSTERED", "INDEX") ||
		tokenSequenceAt(tokens, kindIndex, "NONCLUSTERED", "INDEX") {
		return targetAfterKeyword(tokens, "ON", kindIndex+1)
	}

	nameIndex := objectNameIndex(tokens, kindIndex)
	targets := qualifiedTargetAt(tokens, nameIndex)
	if kind == "TRIGGER" {
		targets = append(targets, targetAfterKeyword(tokens, "ON", nameIndex)...)
	}
	return targets
}

func statementObjectKindIndex(tokens []lexer.Token) int {
	limit := min(len(tokens), 16)
	for index := 1; index < limit; index++ {
		if isStatementObjectKind(normalizedIdentifier(tokens[index])) {
			return index
		}
	}
	return mutationTargetNotFound
}

func isStatementObjectKind(value string) bool {
	switch value {
	case "ACCESS", "AVAILABILITY", "CAST", "CREDENTIAL", "DATABASE",
		"DICTIONARY", "DOMAIN", "ENDPOINT", "EVENT", "EXTERNAL", "EXTENSION",
		"FOREIGN", "FUNCTION", "FULLTEXT", "INDEX", "LANGUAGE", "LIVE",
		"LOGIN", "LOGFILE", "MATERIALIZED", "NAMED", "NONCLUSTERED",
		"PROCEDURE", "PUBLICATION", "QUOTA", "RESOURCE", "ROLE", "ROW",
		"SCHEMA", "SEQUENCE", "SERVER", "SETTINGS", "SPATIAL", "SUBSCRIPTION",
		"TABLE", "TABLESPACE", "TEXT", "TRANSFORM", "TRIGGER", "TYPE",
		"UNDO", "UNIQUE", "USER", "VIEW", "WINDOW", "WORKLOAD":
		return true
	default:
		return false
	}
}

func objectNameIndex(tokens []lexer.Token, kindIndex int) int {
	index := kindIndex + 1
	switch normalizedIdentifier(tokens[kindIndex]) {
	case "ACCESS":
		index = skipSequence(tokens, index, "METHOD")
	case "AVAILABILITY":
		index = skipSequence(tokens, index, "GROUP")
	case "EVENT":
		index = skipSequence(tokens, index, "TRIGGER")
	case "EXTERNAL":
		index = skipSequence(tokens, index, "TABLE")
	case "FOREIGN":
		index = skipSequence(tokens, index, "TABLE", "DATA", "WRAPPER")
	case "FULLTEXT", "UNIQUE", "CLUSTERED", "NONCLUSTERED":
		index = skipSequence(tokens, index, "INDEX")
	case "LIVE", "MATERIALIZED", "WINDOW":
		index = skipSequence(tokens, index, "VIEW")
	case "LOGFILE":
		index = skipSequence(tokens, index, "GROUP")
	case "NAMED":
		index = skipSequence(tokens, index, "COLLECTION")
	case "RESOURCE":
		index = skipSequence(tokens, index, "GROUP")
	case "ROW":
		index = skipSequence(tokens, index, "POLICY")
	case "SERVER":
		index = skipSequence(tokens, index, "ROLE", "AUDIT")
	case "SETTINGS":
		index = skipSequence(tokens, index, "PROFILE")
	case "SPATIAL":
		if tokenSequenceAt(tokens, index, "INDEX") {
			index++
		} else {
			index = skipSequence(tokens, index, "REFERENCE", "SYSTEM")
		}
	case "TEXT":
		index = skipSequence(tokens, index, "SEARCH")
	case "UNDO":
		index = skipSequence(tokens, index, "TABLESPACE")
	}
	return skipNameModifiers(tokens, index)
}

func skipSequence(tokens []lexer.Token, index int, alternatives ...string) int {
	for index < len(tokens) && containsString(alternatives, normalizedIdentifier(tokens[index])) {
		index++
	}
	return index
}

func skipNameModifiers(tokens []lexer.Token, index int) int {
	for index < len(tokens) {
		switch normalizedIdentifier(tokens[index]) {
		case "IF", "NOT", "EXISTS", "ONLY", "CONCURRENTLY":
			index++
		default:
			return index
		}
	}
	return index
}

func targetAfterKeyword(tokens []lexer.Token, keyword string, start int) [][]string {
	index := findIdentifier(tokens, keyword, start)
	if index == mutationTargetNotFound {
		return nil
	}
	return qualifiedTargetAt(tokens, skipNameModifiers(tokens, index+1))
}

func targetAfterModifiers(tokens []lexer.Token, index int, modifiers ...string) [][]string {
	for index < len(tokens) && containsString(modifiers, normalizedIdentifier(tokens[index])) {
		index++
	}
	return qualifiedTargetAt(tokens, index)
}

func qualifiedTargetAt(tokens []lexer.Token, index int) [][]string {
	parts, _ := qualifiedIdentifierAt(tokens, index)
	if len(parts) == 0 {
		return nil
	}
	return [][]string{parts}
}

func qualifiedIdentifierAt(tokens []lexer.Token, index int) ([]string, int) {
	if index < 0 || index >= len(tokens) || normalizedIdentifier(tokens[index]) == "" {
		return nil, index
	}
	parts := []string{identifierValue(tokens[index])}
	index++
	for index+1 < len(tokens) && tokens[index].MatchOperatorValue(".") {
		value := identifierValue(tokens[index+1])
		if value == "" {
			break
		}
		parts = append(parts, value)
		index += 2
	}
	return parts, index
}

func identifierValue(token lexer.Token) string {
	value := token.Value
	switch token.Type {
	case lexer.TokenIdentifier, lexer.TokenString:
	default:
		return ""
	}
	value = strings.TrimPrefix(value, "`")
	value = strings.TrimSuffix(value, "`")
	value = strings.TrimPrefix(value, `"`)
	value = strings.TrimSuffix(value, `"`)
	value = strings.TrimPrefix(value, "'")
	value = strings.TrimSuffix(value, "'")
	value = strings.TrimPrefix(value, "[")
	value = strings.TrimSuffix(value, "]")
	value = strings.ReplaceAll(value, "``", "`")
	value = strings.ReplaceAll(value, `""`, `"`)
	value = strings.ReplaceAll(value, "''", "'")
	value = strings.ReplaceAll(value, "]]", "]")
	return value
}

func tokenSequenceAt(tokens []lexer.Token, index int, values ...string) bool {
	if index < 0 || index+len(values) > len(tokens) {
		return false
	}
	for offset, value := range values {
		if normalizedIdentifier(tokens[index+offset]) != value {
			return false
		}
	}
	return true
}

func containsTokenSequence(tokens []lexer.Token, values ...string) bool {
	for index := range len(tokens) {
		if tokenSequenceAt(tokens, index, values...) {
			return true
		}
	}
	return false
}

func findIdentifier(tokens []lexer.Token, value string, start int) int {
	for index := max(start, 0); index < len(tokens); index++ {
		if normalizedIdentifier(tokens[index]) == value {
			return index
		}
	}
	return mutationTargetNotFound
}

func containsString(values []string, target string) bool {
	return slices.Contains(values, target)
}

func describeStatementObject(tokens []lexer.Token) string {
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound {
		return normalizedIdentifier(tokens[0])
	}
	return normalizedIdentifier(tokens[0]) + " " + normalizedIdentifier(tokens[kindIndex])
}
