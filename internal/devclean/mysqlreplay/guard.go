package mysqlreplay

import (
	"fmt"
	"strings"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

func rejectMySQLExecutableComments(dialect, stmt string) error {
	lex := lexer.NewLexerWithOptions(stmt, lexerOptions())
	for {
		token := lex.NextToken()
		if token.Type == lexer.TokenEOF {
			return nil
		}
		if token.Type != lexer.TokenComment {
			continue
		}
		comment := strings.ToUpper(token.Value)
		if strings.HasPrefix(comment, "/*!") || strings.HasPrefix(comment, "/*M!") {
			return replay.Refuse(dialect, "executable comment")
		}
	}
}

func validateMySQLReplayStatement(dialect, database string, tokens []lexer.Token, scope replay.Scope) error {
	if len(tokens) == 0 {
		return nil
	}
	tokens = replay.LeadingCTEExecutableTokens(tokens)
	first := replay.NormalizedIdentifier(tokens[0])
	if err := rejectMySQLExecutableObjects(dialect, first, tokens); err != nil {
		return err
	}
	if operation := unsafeMySQLReplayOperation(tokens); operation != "" {
		return replay.Refuse(dialect, operation)
	}
	if isGlobalMySQLDDL(tokens) {
		return replay.Refuse(dialect, replay.DescribeStatementObject(tokens))
	}
	if replay.UsesTemporaryObject(tokens) {
		return replay.Refuse(dialect, "TEMP object")
	}
	if replay.ContainsIdentifier(tokens, "OUTFILE") ||
		replay.ContainsIdentifier(tokens, "DUMPFILE") ||
		replay.ContainsIdentifier(tokens, "SONAME") {
		return replay.Refuse(dialect, "external file operation")
	}
	// Another database is the realm's own concern: on a server the run owns,
	// a table written in another database is removed with the server, and on
	// a server named as a whole every user database is the run's.
	if scope.Server || scope.Databases {
		return nil
	}
	if err := rejectMySQLRenameDestinations(dialect, database, tokens); err != nil {
		return err
	}
	return rejectMySQLCrossDatabaseTargets(dialect, database, tokens)
}

func unsafeMySQLReplayOperation(tokens []lexer.Token) string {
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "USE", "INSTALL", "UNINSTALL", "FLUSH", "RESET":
		return first
	case "CALL":
		return "CALL sublanguage"
	case "LOAD":
		if replay.TokenSequenceAt(tokens, 1, "DATA") || replay.TokenSequenceAt(tokens, 1, "XML") {
			return first + " external data operation"
		}
	case "SET":
		if replay.TokenSequenceAt(tokens, 1, "GLOBAL") ||
			replay.TokenSequenceAt(tokens, 1, "PERSIST") ||
			replay.TokenSequenceAt(tokens, 1, "PERSIST_ONLY") {
			return "global or persistent SET"
		}
	case "GRANT", "REVOKE":
		return "privilege or role mutation"
	case "PREPARE", "EXECUTE", "DEALLOCATE":
		return "prepared statement"
	case "ALTER":
		if replay.TokenSequenceAt(tokens, 1, "INSTANCE") {
			return "ALTER INSTANCE"
		}
	}
	return ""
}

func rejectMySQLExecutableObjects(dialect, first string, tokens []lexer.Token) error {
	if definesMySQLExecutableBody(tokens) {
		return replay.Refuse(dialect, first+" executable stored body")
	}
	if engine := unconfinedMySQLStorageEngine(tokens); engine != "" {
		return replay.Refuse(dialect, engine+" storage engine")
	}
	return nil
}

func rejectMySQLRenameDestinations(dialect, database string, tokens []lexer.Token) error {
	if replay.TokenSequenceAt(tokens, 0, "ALTER", "TABLE") && replay.ContainsIdentifier(tokens, "RENAME") {
		return rejectTargetsAfterKeyword(dialect, database, tokens, "TO")
	}
	return nil
}

func rejectMySQLCrossDatabaseTargets(dialect, database string, tokens []lexer.Token) error {
	if strings.TrimSpace(database) == "" {
		return replay.Refuse(dialect, "qualified target without a configured database")
	}
	for _, target := range mysqlMutationTargets(tokens) {
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

func mysqlMutationTargets(tokens []lexer.Token) [][]string {
	switch replay.NormalizedIdentifier(tokens[0]) {
	case "RENAME":
		return mysqlRenameTargets(tokens)
	case "DROP":
		kindIndex := replay.StatementObjectKindIndex(tokens)
		if kindIndex != replay.MutationTargetNotFound &&
			(replay.NormalizedIdentifier(tokens[kindIndex]) == "TABLE" ||
				replay.NormalizedIdentifier(tokens[kindIndex]) == "VIEW") {
			return replay.CommaSeparatedQualifiedTargets(
				tokens,
				replay.ObjectNameIndex(tokens, kindIndex),
				"",
			)
		}
	case "UPDATE":
		return mysqlUpdateTargets(tokens)
	case "LOCK":
		// A planned trigger swap runs under LOCK TABLES. The lock ends with
		// the replay session, so only a table outside the realm makes it
		// unsafe: that would hold writes off a database the run does not own.
		if replay.TokenSequenceAt(tokens, 1, "TABLES") || replay.TokenSequenceAt(tokens, 1, "TABLE") {
			return replay.CommaSeparatedQualifiedTargets(tokens, 2, "")
		}
	}
	return replay.MutationTargets(tokens)
}

func mysqlUpdateTargets(tokens []lexer.Token) [][]string {
	index := 1
	for index < len(tokens) {
		switch replay.NormalizedIdentifier(tokens[index]) {
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
			target, next := replay.QualifiedIdentifierAt(tokens, index)
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
		source, next := replay.QualifiedIdentifierAt(tokens, index)
		if len(source) == 0 {
			return targets
		}
		targets = append(targets, source)
		if next >= len(tokens) || !tokens[next].MatchIdentifierValue("TO") {
			return targets
		}
		destination, next := replay.QualifiedIdentifierAt(tokens, next+1)
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

func isGlobalMySQLDDL(tokens []lexer.Token) bool {
	if !replay.IsDDLAction(tokens) {
		return false
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return false
	}
	switch replay.NormalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE", "SCHEMA", "USER", "ROLE", "SERVER", "TABLESPACE",
		"UNDO", "LOGFILE", "RESOURCE":
		return true
	case "SPATIAL":
		return replay.TokenSequenceAt(tokens, kindIndex, "SPATIAL", "REFERENCE", "SYSTEM")
	default:
		return false
	}
}

func definesMySQLExecutableBody(tokens []lexer.Token) bool {
	return mysqlExecutableBodyKind(tokens) != ""
}

// mysqlExecutableBodyKind names the kind of executable object a CREATE or
// ALTER defines -- FUNCTION, PROCEDURE, TRIGGER or EVENT -- or returns "" for
// any other statement. It is the one reading of that shape: the database realm
// refuses every kind it names, and the server realm lifts every kind but EVENT.
func mysqlExecutableBodyKind(tokens []lexer.Token) string {
	if !replay.IsDDLAction(tokens) {
		return ""
	}
	first := replay.NormalizedIdentifier(tokens[0])
	if first != "CREATE" && first != "ALTER" {
		return ""
	}
	index := 1
	if first == "CREATE" && replay.TokenSequenceAt(tokens, index, "OR", "REPLACE") {
		index += 2
	}
	if first == "CREATE" && replay.TokenSequenceAt(tokens, index, "DEFINER") {
		index = skipMySQLDefiner(tokens, index+1)
	}
	if replay.TokenSequenceAt(tokens, index, "AGGREGATE", "FUNCTION") {
		return "FUNCTION"
	}
	for _, kind := range []string{"EVENT", "FUNCTION", "PROCEDURE", "TRIGGER"} {
		if replay.TokenSequenceAt(tokens, index, kind) {
			return kind
		}
	}
	return ""
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
	if !replay.IsDDLAction(tokens) {
		return ""
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound ||
		replay.NormalizedIdentifier(tokens[kindIndex]) != "TABLE" {
		return ""
	}
	engine, _ := replay.TopLevelValueAfterKeyword(tokens, "ENGINE", replay.ObjectNameIndex(tokens, kindIndex)+1)
	if engine == "FEDERATED" || engine == "CONNECT" {
		return engine
	}
	return ""
}

func rejectTargetsAfterKeyword(
	dialect,
	database string,
	tokens []lexer.Token,
	keyword string,
) error {
	for index := 1; index < len(tokens); index++ {
		if replay.NormalizedIdentifier(tokens[index]) != keyword {
			continue
		}
		target, _ := replay.QualifiedIdentifierAt(tokens, replay.SkipNameModifiers(tokens, index+1))
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
