package sqlutil

import (
	"strings"

	"github.com/stokaro/ptah/core/lexer"
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
	if strings.TrimSpace(sql) == "" {
		return []string{}
	}

	sql = NormalizeClientDelimiters(sql)
	lexr := lexer.NewLexer(sql)
	var statements []string
	var currentStatement strings.Builder
	var state statementSplitState

	for {
		token := lexr.NextToken()

		if token.Type == lexer.TokenEOF {
			break
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
	createPrefix          createStatementPrefix
	inCompoundCreate      bool
	compoundDepth         int
	caseDepth             int
	pendingEndKeyword     bool
	pendingCaseEndKeyword bool
}

func (s *statementSplitState) reset() {
	*s = statementSplitState{}
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
		if isCompoundCreateObject(value) {
			s.createPrefix = createPrefixCreateObject
			return
		}
		if value == "OR" || value == "REPLACE" || value == "DEFINER" {
			return
		}
		s.createPrefix = createPrefixNone
		return

	case createPrefixCreateObject:
		s.createPrefix = createPrefixCreateObjectBeforeBody
		return

	case createPrefixCreateObjectBeforeBody:
		if value == "BEGIN" {
			s.inCompoundCreate = true
			s.compoundDepth = 1
			s.pendingEndKeyword = false
		}
		return
	}
}

func isCompoundCreateObject(value string) bool {
	switch value {
	case "FUNCTION", "PROCEDURE", "TRIGGER":
		return true
	default:
		return false
	}
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
