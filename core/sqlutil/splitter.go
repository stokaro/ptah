package sqlutil

import (
	"slices"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/lexer"
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
	sqlServerBatchStart := 0

	for {
		token := lexr.NextToken()

		if token.Type == lexer.TokenEOF {
			break
		}
		if skippingGoBatchLine {
			if (token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment) &&
				strings.ContainsAny(token.Value, "\r\n") {
				skippingGoBatchLine = false
			}
			continue
		}
		if state.isSQLServer() && token.MatchIdentifierValue("GO") {
			var handled bool
			statements, sqlServerBatchStart, handled = handleSQLServerGoBatchSeparator(
				sql,
				token,
				statements,
				&currentStatement,
				&state,
				sqlServerBatchStart,
			)
			if !handled {
				state.observe(token)
				currentStatement.WriteString(token.Value)
				continue
			}
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

func handleSQLServerGoBatchSeparator(
	sql string,
	token lexer.Token,
	statements []string,
	currentStatement *strings.Builder,
	state *statementSplitState,
	batchStart int,
) ([]string, int, bool) {
	repeatCount, ok := sqlServerGoBatchSeparatorRepeatCountAt(sql, token.Start, token.End)
	if !ok {
		return statements, batchStart, false
	}
	statements, batchStart = appendSQLServerBatch(statements, batchStart, currentStatement.String(), repeatCount)
	currentStatement.Reset()
	state.reset()
	return statements, batchStart, true
}

func appendSQLServerBatch(statements []string, batchStart int, currentStatement string, repeatCount int) ([]string, int) {
	stmt := strings.TrimSpace(currentStatement)
	if stmt != "" {
		statements = append(statements, stmt)
	}
	batch := slices.Clone(statements[batchStart:])
	switch {
	case repeatCount == 0:
		statements = statements[:batchStart]
	case repeatCount > 1:
		for range repeatCount - 1 {
			statements = append(statements, batch...)
		}
	}
	return statements, len(statements)
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
	return s.isSQLServer() &&
		(s.createObject == "FUNCTION" || s.createObject == "PROC" || s.createObject == "PROCEDURE" || s.createObject == "TRIGGER")
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
	_, ok := sqlServerGoBatchSeparatorRepeatCountAt(input, start, end)
	return ok
}

// sqlServerGoBatchSeparatorRepeatCountAt reports whether a GO token is a SQL
// Server utility batch separator and returns the optional GO count. Plain GO
// has count 1; GO 0 discards the pending batch just like SQL Server client
// tooling.
func sqlServerGoBatchSeparatorRepeatCountAt(input string, start, end int) (int, bool) {
	if !sqlServerGoLinePrefixIsEmpty(input, start) {
		return 0, false
	}
	return sqlServerGoTrailerRepeatCount(input, end)
}

func sqlServerGoLinePrefixIsEmpty(input string, start int) bool {
	for i := start - 1; i >= 0 && input[i] != '\n' && input[i] != '\r'; i-- {
		if input[i] != ' ' && input[i] != '\t' {
			return false
		}
	}
	return true
}

func sqlServerGoTrailerRepeatCount(input string, pos int) (int, bool) {
	i := pos
	count := 1
	consumedCount := false
	for {
		i = skipSQLServerHorizontalSpace(input, i)
		if !consumedCount && i < len(input) && input[i] >= '0' && input[i] <= '9' {
			consumedCount = true
			countStart := i
			i = skipSQLServerDigits(input, i)
			parsedCount, err := strconv.Atoi(input[countStart:i])
			if err != nil {
				return 0, false
			}
			count = parsedCount
			continue
		}
		switch {
		case i >= len(input) || input[i] == '\n' || input[i] == '\r':
			return count, true
		case strings.HasPrefix(input[i:], "--"):
			return count, true
		case strings.HasPrefix(input[i:], "/*"):
			next, ok := skipSQLServerBlockComment(input, i)
			if !ok {
				return 0, false
			}
			i = next
		default:
			return 0, false
		}
	}
}

func skipSQLServerHorizontalSpace(input string, pos int) int {
	for pos < len(input) && (input[pos] == ' ' || input[pos] == '\t') {
		pos++
	}
	return pos
}

func skipSQLServerDigits(input string, pos int) int {
	for pos < len(input) && input[pos] >= '0' && input[pos] <= '9' {
		pos++
	}
	return pos
}

func skipSQLServerBlockComment(input string, pos int) (int, bool) {
	commentEnd := strings.Index(input[pos+2:], "*/")
	if commentEnd == -1 {
		return pos, false
	}
	return pos + commentEnd + len("/**/"), true
}
