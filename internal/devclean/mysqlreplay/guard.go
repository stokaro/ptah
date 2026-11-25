package mysqlreplay

import (
	"fmt"
	"strings"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/internal/mysqlstored"
)

// rejectMySQLExecutableComments refuses a statement holding a comment
// written as an executable comment of either server, read by reading's rule
// for a backslash. The guard does not know the version of the server the
// replay reaches, so it refuses each one, run or not.
func rejectMySQLExecutableComments(dialect, stmt string, reading mysqlstored.Server) error {
	lex := lexer.NewLexerWithOptions(stmt, mysqlstored.LexerOptions(reading))
	for {
		token := lex.NextToken()
		if token.Type == lexer.TokenEOF {
			return nil
		}
		if token.Type == lexer.TokenComment && mysqlstored.WrittenAsExecutable(token.Value) {
			return replay.Refuse(dialect, "executable comment")
		}
	}
}

// validateMySQLReplayStatement refuses a statement whose effects cannot be
// confined to scope.
//
// Under [replay.Scope.TrustedSQL] the caller has already accepted the
// statements a server the run owns lifts ([mysqlServerWideOperation]): a stored
// body, a CALL, a privilege, a user, a role and another database. A loadable
// function is one of them, since CREATE FUNCTION ... SONAME is a definition of
// a stored function to that reading: its library runs in the server process,
// as COPY TO PROGRAM runs a command on PostgreSQL's host, and trust lifts both.
// Here it lifts, beyond them: a prepared statement, which is the dynamic SQL a
// MySQL history writes where PostgreSQL writes a DO block; a FEDERATED or
// CONNECT table, LOAD DATA or LOAD XML, and SELECT ... INTO OUTFILE or
// DUMPFILE, which move data across the server's edge and leave nothing in it
// that the cleanup does not remove; and a write to another database. Each
// one's only reason to be refused is that its effect reaches past the dev
// database.
//
// It keeps every refusal that protects the run: an executable comment, which
// hides SQL from every check here; an EVENT, which the scheduler runs on its own
// timetable during the rest of the run; ALTER USER and ALTER ROLE, which can
// change the credentials the run connects with; USE, a global or persistent
// SET, INSTALL, UNINSTALL, FLUSH, RESET and ALTER INSTANCE, which change the
// session or the server the run uses; a TABLESPACE, LOGFILE GROUP, SERVER,
// RESOURCE GROUP or SPATIAL REFERENCE SYSTEM, which configure the server, as a
// server the run owns keeps them refused; ALTER DATABASE; and a TEMP object.
func validateMySQLReplayStatement(dialect, database, stmt string, tokens []lexer.Token, scope replay.Scope) error {
	if len(tokens) == 0 {
		return nil
	}
	tokens = replay.LeadingCTEExecutableTokens(tokens)
	first := replay.NormalizedIdentifier(tokens[0])
	if err := rejectMySQLExecutableObjects(dialect, stmt, first, tokens, scope); err != nil {
		return err
	}
	if operation := unsafeMySQLReplayOperation(tokens, scope); operation != "" {
		return replay.Refuse(dialect, operation)
	}
	if isGlobalMySQLDDL(tokens) {
		return replay.Refuse(dialect, replay.DescribeStatementObject(tokens))
	}
	if replay.UsesTemporaryObject(tokens) {
		return replay.Refuse(dialect, "TEMP object")
	}
	if replay.ContainsIdentifier(tokens, "SONAME") ||
		!scope.TrustedSQL && (replay.ContainsIdentifier(tokens, "OUTFILE") || replay.ContainsIdentifier(tokens, "DUMPFILE")) {
		return replay.Refuse(dialect, "external file operation")
	}
	// Another database is the realm's own concern: on a server the run owns,
	// a table written in another database is removed with the server, and on
	// a server named as a whole every user database is the run's. A run that
	// trusts its SQL accepts the write without owning that database, and its
	// cleanup does not reach it.
	if scope.Server || scope.Databases || scope.TrustedSQL {
		return nil
	}
	if err := rejectMySQLRenameDestinations(dialect, database, tokens); err != nil {
		return err
	}
	return rejectMySQLCrossDatabaseTargets(dialect, database, tokens)
}

// unsafeMySQLReplayOperation names a statement the replay refuses by its first
// words. Under [replay.Scope.TrustedSQL], a prepared statement and LOAD DATA or LOAD XML are
// not refused; see [validateMySQLReplayStatement].
func unsafeMySQLReplayOperation(tokens []lexer.Token, scope replay.Scope) string {
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "USE", "INSTALL", "UNINSTALL", "FLUSH", "RESET":
		return first
	case "CALL":
		return "CALL sublanguage"
	case "LOAD":
		if !scope.TrustedSQL && (replay.TokenSequenceAt(tokens, 1, "DATA") || replay.TokenSequenceAt(tokens, 1, "XML")) {
			return first + " external data operation"
		}
	case "SET":
		if setsGlobalVariable(tokens) {
			return "global or persistent SET"
		}
		if setsClientCharacterSet(tokens) {
			return "client character set change"
		}
	case "GRANT", "REVOKE":
		return "privilege or role mutation"
	case "PREPARE", "EXECUTE", "DEALLOCATE":
		if !scope.TrustedSQL {
			return "prepared statement"
		}
	case "ALTER":
		if replay.TokenSequenceAt(tokens, 1, "INSTANCE") {
			return "ALTER INSTANCE"
		}
	}
	return ""
}

func rejectMySQLExecutableObjects(dialect, stmt, first string, tokens []lexer.Token, scope replay.Scope) error {
	if definesMySQLExecutableBody(stmt, tokens) {
		return replay.Refuse(dialect, first+" executable stored body")
	}
	if engine := unconfinedMySQLStorageEngine(tokens); engine != "" && !scope.TrustedSQL {
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

// definesMySQLExecutableBody reports a CREATE or ALTER of an object with a
// body, as [mysqlstored.Read] reads it: the database realm refuses every such
// statement, and the server realm lifts every one but an EVENT's. Read reads
// one that defines nothing with a body, such as CREATE TABLE, as such. A
// statement Read cannot read reports false here, because
// [rejectUnreadableDefinition] refused it before any realm was consulted.
func definesMySQLExecutableBody(stmt string, tokens []lexer.Token) bool {
	definition, ok, _ := mysqlstored.Read(stmt, tokens)
	return ok && definition.Body()
}

// rejectUnreadableDefinition refuses a CREATE or ALTER whose head
// [mysqlstored.Read] cannot follow to a kind of object it knows. It runs
// before any realm or trust lifts a statement, since such a statement may
// define an EVENT, which every realm keeps refused.
func rejectUnreadableDefinition(dialect, stmt string, tokens []lexer.Token) error {
	if _, _, err := mysqlstored.Read(stmt, tokens); err != nil {
		return replay.Refuse(dialect, replay.NormalizedIdentifier(tokens[0])+" statement whose object kind cannot be read")
	}
	return nil
}

// setItems returns the index at which each item of a SET's list begins: the
// token after SET and each token after a comma outside parentheses.
func setItems(tokens []lexer.Token) []int {
	items := []int{1}
	depth := 0
	for index := 1; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && tokens[index].MatchOperatorValue(","):
			items = append(items, index+1)
		}
	}
	return items
}

// setsGlobalVariable reports a SET with an item that assigns a global or
// persisted variable: GLOBAL, PERSIST or PERSIST_ONLY in front of the name,
// or @@GLOBAL., @@PERSIST. or @@PERSIST_ONLY. as its prefix. Each item names
// its own scope, so measured on MySQL 8.4.13 and MariaDB 11.8.9,
// SET @@global.max_connections = 152 changes the server for every session, as
// SET GLOBAL does.
func setsGlobalVariable(tokens []lexer.Token) bool {
	for _, item := range setItems(tokens) {
		scope := item
		if at, next := replay.TokenAt(tokens, item), replay.TokenAt(tokens, item+1); at.MatchOperatorValue("@") && next.MatchOperatorValue("@") {
			scope = item + 2
		}
		switch replay.NormalizedIdentifier(replay.TokenAt(tokens, scope)) {
		case "GLOBAL", "PERSIST", "PERSIST_ONLY":
			if scope == item || scope+1 < len(tokens) && tokens[scope+1].MatchOperatorValue(".") {
				return true
			}
		}
	}
	return false
}

// setsClientCharacterSet reports a SET that gives the session a client
// character set outside the UTF-8 family: NAMES, CHARACTER SET or CHARSET as
// an item of the SET's list, or an assignment to character_set_client in any
// spelling, whose value is not utf8mb4, utf8mb3 or utf8. DEFAULT and an
// expression are refused, since the guard cannot tell what they name.
//
// The guard reads a statement's bytes as UTF-8, which is how a session whose
// client character set is in that family reads them. Measured on MySQL
// 8.4.13 and MariaDB 11.8.9: under latin1 the byte 0xA0 is whitespace, so
// CREATE<0xA0>TRIGGER defines a trigger; and under gbk the last byte of 中
// and the backtick after it are one character, so the server ends a quoted
// name where the guard reads a doubled backtick, and runs the executable
// comment that follows. That statement is valid UTF-8, so refusing bytes that
// are not UTF-8 would not catch it; refusing the change does.
func setsClientCharacterSet(tokens []lexer.Token) bool {
	for _, at := range setItems(tokens) {
		switch {
		case replay.TokenSequenceAt(tokens, at, "NAMES") || replay.TokenSequenceAt(tokens, at, "CHARSET"):
			if !namesUTF8(tokens, at+1) {
				return true
			}
		case replay.TokenSequenceAt(tokens, at, "CHARACTER", "SET"):
			if !namesUTF8(tokens, at+2) {
				return true
			}
		}
	}
	for index, token := range tokens {
		if replay.NormalizedIdentifier(token) != "CHARACTER_SET_CLIENT" {
			continue
		}
		if value := assignedValue(tokens, index+1); value >= 0 && !namesUTF8(tokens, value) {
			return true
		}
	}
	return false
}

// assignedValue returns the index of the value an = or := at at assigns, and
// -1 where no assignment stands at at.
func assignedValue(tokens []lexer.Token, at int) int {
	switch {
	case at < len(tokens) && tokens[at].MatchOperatorValue("="):
		return at + 1
	case at+1 < len(tokens) && tokens[at].MatchOperatorValue(":") && tokens[at+1].MatchOperatorValue("="):
		return at + 2
	default:
		return -1
	}
}

// namesUTF8 reports whether the token at at names a character set of the
// UTF-8 family, quoted or not.
func namesUTF8(tokens []lexer.Token, at int) bool {
	if at >= len(tokens) {
		return false
	}
	switch replay.NormalizedIdentifier(tokens[at]) {
	case "UTF8MB4", "UTF8MB3", "UTF8":
		return true
	default:
		return false
	}
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
