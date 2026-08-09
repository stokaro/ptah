package migrator

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/lexer"
)

// implicitCommitDialect reports whether the server commits an open transaction
// on its own, without the client asking, before certain statements run.
//
// Only the MySQL family does. Everywhere else a migration body that ran inside
// one transaction either survives whole or leaves nothing behind, so "did the
// rollback undo this statement?" has a single answer for the whole file. On
// MySQL and MariaDB the answer differs statement by statement, and a resume
// that assumes otherwise either repeats committed SQL or skips SQL that never
// ran.
func implicitCommitDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

func (m *Migrator) validateTransactionalProgressSQL(
	migration *Migration,
	direction MigrationDirection,
) error {
	if !implicitCommitDialect(m.connectionDialect()) {
		return nil
	}
	statements := splitSQLStatementsForConnection(m.conn, migrationSQLForDirection(migration, direction))
	for i, statement := range statements {
		tokens := significantSQLTokens(statement, m.connectionDialect())
		if len(tokens) > 0 && tokens[0].MatchIdentifierValue("USE") {
			return fmt.Errorf(
				"migration %d cannot run tx-mode file statement %d because %q changes the target database; "+
					"select the database in the connection URL so Ptah can verify its storage engines",
				migration.Version,
				i+1,
				strings.TrimSpace(statement),
			)
		}
		if mysqlUnwitnessedStateChange(tokens) {
			return fmt.Errorf(
				"migration %d cannot run tx-mode file statement %d because %q changes state outside "+
					"Ptah's migration transaction; use a session-scoped setting or tx-mode none",
				migration.Version,
				i+1,
				strings.TrimSpace(statement),
			)
		}
		if isTransactionControlStatement(statement, m.connectionDialect()) {
			return fmt.Errorf(
				"migration %d cannot run tx-mode file statement %d because %q controls transaction state; "+
					"remove the transaction control and let Ptah manage the file transaction",
				migration.Version,
				i+1,
				strings.TrimSpace(statement),
			)
		}
		if mysqlCreateTableLike(tokens) {
			return fmt.Errorf(
				"migration %d cannot run tx-mode file statement %d because CREATE TABLE LIKE inherits "+
					"a storage engine that Ptah cannot prove is InnoDB; declare the table explicitly or use tx-mode none",
				migration.Version,
				i+1,
			)
		}
		engine, selected := mysqlStorageEngineSelection(statement, m.connectionDialect())
		if !selected || strings.EqualFold(engine, "InnoDB") || strings.EqualFold(engine, "DEFAULT") {
			continue
		}
		return fmt.Errorf(
			"migration %d statement %d selects non-transactional storage engine %s; "+
				"tx-mode file requires InnoDB on MySQL-family databases",
			migration.Version,
			i+1,
			engine,
		)
	}
	return nil
}

// mysqlUnwitnessedStateChange identifies MySQL-family statements whose effects
// are not session-local and are not tied to the InnoDB transaction containing
// the revision witness. Replaying one after the witness rolls back can repeat a
// durable server change, while skipping it can lose a change that never took
// effect. File mode therefore refuses these statements before any body SQL.
func mysqlUnwitnessedStateChange(tokens []lexer.Token) bool {
	if len(tokens) == 0 {
		return false
	}
	if tokens[0].MatchIdentifierValue("RESET") {
		return true
	}
	if !tokens[0].MatchIdentifierValue("SET") {
		return false
	}

	assignmentStart := true
	for _, token := range tokens[1:] {
		if token.Value == "," {
			assignmentStart = true
			continue
		}
		if !assignmentStart || token.Type != lexer.TokenIdentifier {
			continue
		}
		assignmentStart = false
		if matchesAnyKeyword(
			token,
			"GLOBAL",
			"PERSIST",
			"PERSIST_ONLY",
			"PASSWORD",
			"DEFAULT",
			"RESOURCE",
			"STATEMENT",
		) {
			return true
		}
	}
	return false
}

func mysqlCreateTableLike(tokens []lexer.Token) bool {
	if len(tokens) < 3 || !tokens[0].MatchIdentifierValue("CREATE") {
		return false
	}
	tableKeyword := mysqlTableKeyword(tokens)
	if tableKeyword < 0 {
		return false
	}

	depth := 0
	for _, token := range tokens[tableKeyword+1:] {
		switch token.Value {
		case "(":
			depth++
			continue
		case ")":
			depth = max(depth-1, 0)
			continue
		}
		if depth == 0 && token.MatchIdentifierValue("LIKE") {
			return true
		}
	}
	return false
}

func mysqlStorageEngineSelection(statement, dialect string) (string, bool) {
	tokens := significantSQLTokens(statement, dialect)
	if len(tokens) == 0 {
		return "", false
	}
	if tokens[0].MatchIdentifierValue("SET") {
		return mysqlStorageEngineSetting(tokens)
	}
	if tokens[0].MatchIdentifierValue("CREATE") {
		return mysqlCreateTableStorageEngine(tokens)
	}
	if tokens[0].MatchIdentifierValue("ALTER") {
		return mysqlAlterTableStorageEngine(tokens)
	}
	return "", false
}

func mysqlStorageEngineSetting(tokens []lexer.Token) (string, bool) {
	assignmentStart := 1
	for assignmentStart < len(tokens) {
		assignmentEnd := len(tokens)
		for i := assignmentStart; i < len(tokens); i++ {
			if tokens[i].Value == "," {
				assignmentEnd = i
				break
			}
		}
		if engine, selected := mysqlStorageEngineAssignment(tokens[assignmentStart:assignmentEnd]); selected {
			return engine, true
		}
		assignmentStart = assignmentEnd + 1
	}
	return "", false
}

func mysqlStorageEngineAssignment(tokens []lexer.Token) (string, bool) {
	equals := -1
	storageEngineTarget := false
	for i, token := range tokens {
		if token.Value == "=" {
			equals = i
			break
		}
		if matchesAnyKeyword(token, "STORAGE_ENGINE", "DEFAULT_STORAGE_ENGINE") {
			storageEngineTarget = true
		}
	}
	if !storageEngineTarget || equals < 0 {
		return "", false
	}
	return mysqlStorageEngineValue(tokens, equals+1)
}

func mysqlCreateTableStorageEngine(tokens []lexer.Token) (string, bool) {
	tableKeyword := mysqlTableKeyword(tokens)
	if tableKeyword < 0 {
		return "", false
	}
	optionStart := mysqlTableNameEnd(tokens, tableKeyword+1)
	if optionStart < 0 {
		return "", false
	}
	depth := 0
	for i := optionStart; i < len(tokens); i++ {
		token := tokens[i]
		switch token.Value {
		case "(":
			depth++
			continue
		case ")":
			depth = max(depth-1, 0)
			continue
		}
		if depth != 0 {
			continue
		}
		if matchesAnyKeyword(token, "AS", "SELECT") {
			return "", false
		}
		if token.MatchIdentifierValue("ENGINE") {
			return mysqlStorageEngineValue(tokens, i+1)
		}
	}
	return "", false
}

func mysqlAlterTableStorageEngine(tokens []lexer.Token) (string, bool) {
	tableKeyword := mysqlTableKeyword(tokens)
	if tableKeyword < 0 {
		return "", false
	}
	optionStart := mysqlTableNameEnd(tokens, tableKeyword+1)
	if optionStart < 0 {
		return "", false
	}
	depth := 0
	atOptionStart := true
	for i := optionStart; i < len(tokens); i++ {
		token := tokens[i]
		switch token.Value {
		case "(":
			depth++
			continue
		case ")":
			depth = max(depth-1, 0)
			continue
		case ",":
			if depth == 0 {
				atOptionStart = true
			}
			continue
		}
		if depth != 0 || !atOptionStart {
			continue
		}
		if token.MatchIdentifierValue("ENGINE") {
			return mysqlStorageEngineValue(tokens, i+1)
		}
		atOptionStart = false
	}
	return "", false
}

func mysqlTableKeyword(tokens []lexer.Token) int {
	for i, token := range tokens[1:min(len(tokens), 6)] {
		if token.MatchIdentifierValue("TABLE") {
			return i + 1
		}
	}
	return -1
}

func mysqlTableNameEnd(tokens []lexer.Token, start int) int {
	for start < len(tokens) && matchesAnyKeyword(tokens[start], "IF", "NOT", "EXISTS") {
		start++
	}
	if start >= len(tokens) || tokens[start].Type != lexer.TokenIdentifier {
		return -1
	}
	start++
	if start+1 < len(tokens) && tokens[start].Value == "." && tokens[start+1].Type == lexer.TokenIdentifier {
		start += 2
	}
	return start
}

func mysqlStorageEngineValue(tokens []lexer.Token, start int) (string, bool) {
	for _, token := range tokens[start:] {
		if token.Value == "=" {
			continue
		}
		if token.Value == "," || token.Type == lexer.TokenSemicolon {
			return "", false
		}
		return strings.Trim(token.Value, "`\"'"), true
	}
	return "", false
}
