package sqlutil

import (
	"strings"

	"github.com/stokaro/ptah/core/lexer"
	"github.com/stokaro/ptah/core/platform"
)

// StripComments removes all SQL comments from the input string using lexer-based parsing.
// This properly handles comments within string literals and preserves the structure of the SQL.
// Both line comments (-- comment) and block comments (/* comment */) are removed.
func StripComments(sql string) string {
	if strings.TrimSpace(sql) == "" {
		return sql
	}

	lexr := lexer.NewLexer(sql)
	var result strings.Builder

	for {
		token := lexr.NextToken()

		if token.Type == lexer.TokenEOF {
			break
		}

		// Skip comment tokens, include everything else
		if token.Type != lexer.TokenComment {
			result.WriteString(token.Value)
		}
	}

	return result.String()
}

// SplitSQLStatements splits a SQL string into individual statements using AST-based parsing.
// This properly handles semicolons within string literals and comments, unlike simple string splitting.
func SplitSQLStatements(sql string) []string {
	return splitSQLStatements(sql, "")
}

// SplitSQLStatementsForDialect splits SQL using dialect-specific client
// statement boundaries where the generic splitter would be too aggressive.
func SplitSQLStatementsForDialect(sql string, dialect string) []string {
	return splitSQLStatements(sql, platform.NormalizeDialect(dialect))
}

func splitSQLStatements(sql string, dialect string) []string {
	if strings.TrimSpace(sql) == "" {
		return []string{}
	}

	sql = NormalizeClientDelimiters(sql)
	lexr := lexer.NewLexer(sql)
	var statements []string
	var currentStatement strings.Builder
	state := statementSplitState{dialect: dialect}
	skippingGoBatchLine := false

	for {
		token := lexr.NextToken()

		if token.Type == lexer.TokenEOF {
			break
		}
		if skippingGoBatchLine {
			if token.Type == lexer.TokenWhitespace && strings.ContainsAny(token.Value, "\r\n") {
				skippingGoBatchLine = false
			}
			continue
		}
		if state.isSQLServer() && token.MatchIdentifierValue("GO") && IsSQLServerGoBatchSeparatorAt(sql, token.Start, token.End) {
			stmt := strings.TrimSpace(currentStatement.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			currentStatement.Reset()
			state.reset()
			state.dialect = dialect
			skippingGoBatchLine = true
			continue
		}

		if token.Type == lexer.TokenSemicolon {
			if state.keepSemicolonInsideStatement() {
				currentStatement.WriteString(token.Value)
				continue
			}

			// Found a statement terminator - add current statement if not empty
			stmt := strings.TrimSpace(currentStatement.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			currentStatement.Reset()
			state.reset()
		} else {
			state.observe(token)
			// Add token to current statement
			currentStatement.WriteString(token.Value)
		}
	}

	// Add any remaining statement
	stmt := strings.TrimSpace(currentStatement.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	// Ensure we always return a non-nil slice
	if statements == nil {
		return []string{}
	}

	return statements
}

type statementSplitState struct {
	dialect               string
	createPrefix          createStatementPrefix
	createObject          string
	inCompoundCreate      bool
	sqlServerRoutine      bool
	compoundDepth         int
	caseDepth             int
	pendingEndKeyword     bool
	pendingCaseEndKeyword bool
}

func (s *statementSplitState) reset() {
	dialect := s.dialect
	*s = statementSplitState{dialect: dialect}
}

func (s *statementSplitState) observe(token lexer.Token) {
	if token.Type != lexer.TokenIdentifier {
		return
	}

	value := strings.ToUpper(token.Value)
	if !s.inCompoundCreate {
		s.observeCreatePrefix(value)
		return
	}

	switch value {
	case "CASE":
		if s.pendingEndKeyword || s.pendingCaseEndKeyword {
			s.pendingEndKeyword = false
			s.pendingCaseEndKeyword = false
			return
		}
		s.caseDepth++
	case "BEGIN":
		s.compoundDepth++
		s.pendingEndKeyword = false
		s.pendingCaseEndKeyword = false
	case "END":
		if s.caseDepth > 0 {
			s.caseDepth--
			s.pendingEndKeyword = false
			s.pendingCaseEndKeyword = true
			return
		}
		s.pendingEndKeyword = true
		s.pendingCaseEndKeyword = false
	default:
		if s.pendingEndKeyword && isEndContinuationKeyword(value) {
			s.pendingEndKeyword = false
		}
		s.pendingCaseEndKeyword = false
	}
}

type createStatementPrefix int

const (
	createPrefixNone createStatementPrefix = iota
	createPrefixCreate
	createPrefixCreateObject
	createPrefixCreateObjectBeforeBody
)

func (s *statementSplitState) observeCreatePrefix(value string) {
	switch s.createPrefix {
	case createPrefixNone:
		if value == "CREATE" {
			s.createPrefix = createPrefixCreate
		}
		return

	case createPrefixCreate:
		if s.isCompoundCreateObject(value) {
			s.createPrefix = createPrefixCreateObject
			s.createObject = value
			return
		}
		if value == "OR" || value == "ALTER" || value == "REPLACE" || value == "DEFINER" {
			return
		}
		s.createPrefix = createPrefixNone
		return

	case createPrefixCreateObject:
		s.createPrefix = createPrefixCreateObjectBeforeBody
		return

	case createPrefixCreateObjectBeforeBody:
		if s.isSQLServerRoutineObject() && value == "AS" {
			s.inCompoundCreate = true
			s.sqlServerRoutine = true
			s.pendingEndKeyword = false
			return
		}
		if value == "BEGIN" {
			s.inCompoundCreate = true
			s.compoundDepth = 1
			s.pendingEndKeyword = false
		}
		return
	}
}

func (s statementSplitState) isCompoundCreateObject(value string) bool {
	switch value {
	case "FUNCTION", "PROCEDURE", "TRIGGER":
		return true
	case "PROC":
		return s.isSQLServer()
	default:
		return false
	}
}

func (s statementSplitState) isSQLServerRoutineObject() bool {
	return s.isSQLServer() && (s.createObject == "FUNCTION" || s.createObject == "PROC" || s.createObject == "PROCEDURE")
}

func (s statementSplitState) isSQLServer() bool {
	return s.dialect == platform.SQLServer
}

func isEndContinuationKeyword(value string) bool {
	switch value {
	case "IF", "LOOP", "REPEAT", "WHILE", "CASE":
		return true
	default:
		return false
	}
}

func (s *statementSplitState) keepSemicolonInsideStatement() bool {
	if !s.inCompoundCreate {
		return false
	}
	if s.sqlServerRoutine && !s.pendingEndKeyword {
		return true
	}
	if !s.pendingEndKeyword {
		return true
	}
	if s.compoundDepth > 0 {
		s.compoundDepth--
	}
	s.pendingEndKeyword = false
	if s.compoundDepth == 0 {
		s.inCompoundCreate = false
		return false
	}
	return true
}

// IsSQLServerGoBatchSeparatorAt reports whether a GO token is a SQL Server
// utility batch separator command on its own line. Identifiers such as
// "AS go" or variables such as "@go" are ordinary T-SQL tokens.
func IsSQLServerGoBatchSeparatorAt(input string, start, end int) bool {
	for i := start - 1; i >= 0 && input[i] != '\n' && input[i] != '\r'; i-- {
		if input[i] != ' ' && input[i] != '\t' {
			return false
		}
	}

	i := end
	for i < len(input) && (input[i] == ' ' || input[i] == '\t') {
		i++
	}
	for i < len(input) && input[i] >= '0' && input[i] <= '9' {
		i++
	}
	for i < len(input) && (input[i] == ' ' || input[i] == '\t') {
		i++
	}
	return i >= len(input) || input[i] == '\n' || input[i] == '\r' || strings.HasPrefix(input[i:], "--")
}
