package devclean

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/lexer"
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
	tokens = leadingCTEExecutableTokens(tokens)
	first := normalizedIdentifier(tokens[0])
	if err := rejectMySQLExecutableObjects(dialect, first, tokens); err != nil {
		return err
	}
	if operation := unsafeMySQLReplayOperation(tokens); operation != "" {
		return unsafeReplayStatement(dialect, operation)
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
	return rejectMySQLCrossDatabaseTargets(dialect, database, tokens)
}

func unsafeMySQLReplayOperation(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "USE", "INSTALL", "UNINSTALL", "FLUSH", "RESET":
		return first
	case "CALL":
		return "CALL sublanguage"
	case "LOAD":
		if tokenSequenceAt(tokens, 1, "DATA") || tokenSequenceAt(tokens, 1, "XML") {
			return first + " external data operation"
		}
	case "SET":
		if tokenSequenceAt(tokens, 1, "GLOBAL") ||
			tokenSequenceAt(tokens, 1, "PERSIST") ||
			tokenSequenceAt(tokens, 1, "PERSIST_ONLY") {
			return "global or persistent SET"
		}
	case "GRANT", "REVOKE":
		return "privilege or role mutation"
	case "PREPARE", "EXECUTE", "DEALLOCATE":
		return "prepared statement"
	case "LOCK":
		if tokenSequenceAt(tokens, 1, "TABLES") {
			return "LOCK TABLES"
		}
	case "ALTER":
		if tokenSequenceAt(tokens, 1, "INSTANCE") {
			return "ALTER INSTANCE"
		}
	}
	return ""
}

func rejectMySQLExecutableObjects(dialect, first string, tokens []lexer.Token) error {
	if definesMySQLExecutableBody(tokens) {
		return unsafeReplayStatement(dialect, first+" executable stored body")
	}
	if engine := unconfinedMySQLStorageEngine(tokens); engine != "" {
		return unsafeReplayStatement(dialect, engine+" storage engine")
	}
	return nil
}

func rejectMySQLRenameDestinations(dialect, database string, tokens []lexer.Token) error {
	if tokenSequenceAt(tokens, 0, "ALTER", "TABLE") && containsIdentifier(tokens, "RENAME") {
		return rejectTargetsAfterKeyword(dialect, database, tokens, "TO")
	}
	return nil
}

func rejectMySQLCrossDatabaseTargets(dialect, database string, tokens []lexer.Token) error {
	if strings.TrimSpace(database) == "" {
		return unsafeReplayStatement(dialect, "qualified target without a configured database")
	}
	for _, target := range mysqlMutationTargets(tokens) {
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

func mysqlMutationTargets(tokens []lexer.Token) [][]string {
	switch normalizedIdentifier(tokens[0]) {
	case "RENAME":
		return mysqlRenameTargets(tokens)
	case "DROP":
		kindIndex := statementObjectKindIndex(tokens)
		if kindIndex != mutationTargetNotFound &&
			(normalizedIdentifier(tokens[kindIndex]) == "TABLE" ||
				normalizedIdentifier(tokens[kindIndex]) == "VIEW") {
			return commaSeparatedQualifiedTargets(
				tokens,
				objectNameIndex(tokens, kindIndex),
				"",
			)
		}
	case "UPDATE":
		return mysqlUpdateTargets(tokens)
	}
	return mutationTargets(tokens)
}

func mysqlUpdateTargets(tokens []lexer.Token) [][]string {
	index := 1
	for index < len(tokens) {
		switch normalizedIdentifier(tokens[index]) {
		case "LOW_PRIORITY", "IGNORE", "ONLY":
			index++
		default:
			return mysqlTableReferenceTargets(tokens, index)
		}
	}
	return nil
}

func mysqlTableReferenceTargets(tokens []lexer.Token, start int) [][]string {
	var targets [][]string
	expectTarget := true
	depth := 0
	for index := start; index < len(tokens); index++ {
		if depth == 0 && tokens[index].MatchIdentifierValue("SET") {
			return targets
		}
		if expectTarget {
			target, next := qualifiedIdentifierAt(tokens, index)
			if len(target) == 0 {
				continue
			}
			targets = append(targets, target)
			expectTarget = false
			index = next - 1
			continue
		}
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && tokens[index].MatchOperatorValue(","):
			expectTarget = true
		case depth == 0 && (tokens[index].MatchIdentifierValue("JOIN") ||
			tokens[index].MatchIdentifierValue("STRAIGHT_JOIN")):
			expectTarget = true
		}
	}
	return targets
}

func mysqlRenameTargets(tokens []lexer.Token) [][]string {
	index := 1
	if index < len(tokens) && tokens[index].MatchIdentifierValue("TABLE") {
		index++
	}
	var targets [][]string
	for index < len(tokens) {
		source, next := qualifiedIdentifierAt(tokens, index)
		if len(source) == 0 {
			return targets
		}
		targets = append(targets, source)
		if next >= len(tokens) || !tokens[next].MatchIdentifierValue("TO") {
			return targets
		}
		destination, next := qualifiedIdentifierAt(tokens, next+1)
		if len(destination) == 0 {
			return targets
		}
		targets = append(targets, destination)
		if next >= len(tokens) || !tokens[next].MatchOperatorValue(",") {
			return targets
		}
		index = next + 1
	}
	return targets
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
	for _, target := range sqlServerMutationTargets(tokens) {
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
		return normalizedIdentifier(tokens[0]) != "DROP" ||
			!tokenSequenceAt(tokens, kindIndex, "FOREIGN", "TABLE")
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
	case "SYNONYM":
		return normalizedIdentifier(tokens[0]) != "DROP"
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

func definesMySQLExecutableBody(tokens []lexer.Token) bool {
	if !isDDLAction(tokens) {
		return false
	}
	first := normalizedIdentifier(tokens[0])
	if first != "CREATE" && first != "ALTER" {
		return false
	}
	index := 1
	if first == "CREATE" && tokenSequenceAt(tokens, index, "OR", "REPLACE") {
		index += 2
	}
	if first == "CREATE" && tokenSequenceAt(tokens, index, "DEFINER") {
		index = skipMySQLDefiner(tokens, index+1)
	}
	if tokenSequenceAt(tokens, index, "AGGREGATE", "FUNCTION") {
		return true
	}
	return tokenSequenceAt(tokens, index, "EVENT") ||
		tokenSequenceAt(tokens, index, "FUNCTION") ||
		tokenSequenceAt(tokens, index, "PROCEDURE") ||
		tokenSequenceAt(tokens, index, "TRIGGER")
}

func skipMySQLDefiner(tokens []lexer.Token, index int) int {
	if index < len(tokens) && tokens[index].MatchOperatorValue("=") {
		index++
	}
	if index >= len(tokens) {
		return index
	}
	index++
	if index < len(tokens) && tokens[index].MatchOperatorValue("(") {
		index = skipBalancedParentheses(tokens, index)
	}
	if index < len(tokens) && tokens[index].MatchOperatorValue("@") {
		index += min(2, len(tokens)-index)
	}
	return index
}

func skipBalancedParentheses(tokens []lexer.Token, index int) int {
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

func unconfinedMySQLStorageEngine(tokens []lexer.Token) string {
	if !isDDLAction(tokens) {
		return ""
	}
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound ||
		normalizedIdentifier(tokens[kindIndex]) != "TABLE" {
		return ""
	}
	engine, _ := topLevelValueAfterKeyword(tokens, "ENGINE", objectNameIndex(tokens, kindIndex)+1)
	if engine == "FEDERATED" || engine == "CONNECT" {
		return engine
	}
	return ""
}

func topLevelValueAfterKeyword(tokens []lexer.Token, keyword string, start int) (string, bool) {
	depth := 0
	for index := max(start, 0); index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && normalizedIdentifier(tokens[index]) == "SELECT":
			return "", false
		case depth == 0 && normalizedIdentifier(tokens[index]) == keyword:
			index++
			assigned := false
			if index < len(tokens) && tokens[index].MatchOperatorValue("=") {
				index++
				assigned = true
			}
			if index < len(tokens) {
				return normalizedIdentifier(tokens[index]), assigned
			}
			return "", assigned
		}
	}
	return "", false
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

func sqlServerMutationTargets(tokens []lexer.Token) [][]string {
	if normalizedIdentifier(tokens[0]) != "DROP" {
		return mutationTargets(tokens)
	}
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound {
		return mutationTargets(tokens)
	}
	if normalizedIdentifier(tokens[kindIndex]) == "INDEX" {
		return sqlServerDropIndexTargets(tokens, kindIndex+1)
	}
	return commaSeparatedQualifiedTargets(
		tokens,
		objectNameIndex(tokens, kindIndex),
		"",
	)
}

func sqlServerDropIndexTargets(tokens []lexer.Token, start int) [][]string {
	var targets [][]string
	for index := start; index < len(tokens); index++ {
		if !tokens[index].MatchIdentifierValue("ON") {
			continue
		}
		target, next := qualifiedIdentifierAt(tokens, index+1)
		if len(target) != 0 {
			targets = append(targets, target)
		}
		index = next - 1
	}
	return targets
}

func commaSeparatedQualifiedTargets(
	tokens []lexer.Token,
	start int,
	stopKeyword string,
) [][]string {
	var targets [][]string
	expectTarget := true
	depth := 0
	for index := start; index < len(tokens); index++ {
		if depth == 0 && stopKeyword != "" &&
			tokens[index].MatchIdentifierValue(stopKeyword) {
			return targets
		}
		if expectTarget {
			target, next := qualifiedIdentifierAt(tokens, index)
			if len(target) == 0 {
				continue
			}
			targets = append(targets, target)
			expectTarget = false
			index = next - 1
			continue
		}
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && tokens[index].MatchOperatorValue(","):
			expectTarget = true
		}
	}
	return targets
}

func leadingCTEExecutableTokens(tokens []lexer.Token) []lexer.Token {
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
			depth = max(depth-1, 0)
			closedDefinition = depth == 0
		case depth == 0 && closedDefinition && isDMLStatementToken(tokens[index]):
			return tokens[index:]
		}
	}
	return tokens
}

func isDMLStatementToken(token lexer.Token) bool {
	switch {
	case token.MatchIdentifierValue("SELECT"),
		token.MatchIdentifierValue("INSERT"),
		token.MatchIdentifierValue("UPDATE"),
		token.MatchIdentifierValue("DELETE"),
		token.MatchIdentifierValue("MERGE"):
		return true
	default:
		return false
	}
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
		"SYNONYM", "TABLE", "TABLESPACE", "TEXT", "TRANSFORM", "TRIGGER", "TYPE",
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
