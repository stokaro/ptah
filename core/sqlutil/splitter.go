package sqlutil

import (
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/sqlcompound"
)

// StripComments removes all SQL comments from the input string, scanning it as
// SQL rather than as text, so a comment marker inside a string literal is not
// mistaken for a comment and everything that is not a comment is preserved as
// written, whitespace included. Both line comments (-- comment) and block
// comments (/* comment */) are removed.
func StripComments(sql string) string {
	return stripComments(sql, "")
}

// StripCommentsForDialect removes SQL comments while preserving constructs
// whose lexical meaning depends on dialect, including SQL Server bracketed
// identifiers containing comment markers. The dialect is folded through
// [platform.NormalizeDialect]; a blank or unrecognized one is not an error and
// strips exactly as [StripComments] does.
func StripCommentsForDialect(sql, dialect string) string {
	return stripComments(sql, platform.NormalizeDialect(dialect))
}

func stripComments(sql, dialect string) string {
	if strings.TrimSpace(sql) == "" {
		return sql
	}

	options := lexer.Options{}
	if dialect != "" {
		options = dialectlexer.Options(dialect)
	}
	lexr := lexer.NewLexerWithOptions(sql, options)
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

// SplitSQLStatements splits a SQL string into individual statements, scanning
// it as SQL rather than as text, so a semicolon inside a string literal, a
// comment, or a compound routine body does not start a new statement. MySQL
// DELIMITER and `-- atlas:delimiter` directives are honored first (see
// [NormalizeClientDelimiters]), so a script that declares its own terminator
// splits at that terminator. Each returned statement is whitespace-trimmed
// with its terminating semicolon dropped — anything that needs the statements
// as they were written, byte for byte, uses [SplitSourceStatements] instead —
// and statements with no content are not returned. Comments stay in the
// statements that carry them; [SplitStatements] is the composition that also
// strips them. The result is never nil: input carrying no statement at all —
// empty, whitespace, or bare semicolons — yields an empty, non-nil slice.
func SplitSQLStatements(sql string) []string {
	return splitSQLStatements(sql, "")
}

// SplitSQLStatementsForDialect splits SQL using dialect-specific client
// statement boundaries where the generic splitter would be too aggressive, or
// not aggressive enough. The dialect is folded through
// [platform.NormalizeDialect]; a blank or unrecognized dialect degrades to the
// generic scanning of [SplitSQLStatements] without error.
//
// The decisions a caller can see include these. SQL Server scripts are
// additionally split on the GO batch separator: a GO alone on its line ends the
// batch, an optional trailing count makes the batch come back that many times,
// and GO 0 discards the pending batch, matching SQL Server client tooling.
// MySQL, MariaDB and ClickHouse string literals are scanned with C-style
// backslash escapes, so a semicolon behind a backslash-escaped quote cannot
// leak out as a statement boundary. And compound routine bodies are recognized
// per dialect: an Oracle PL/SQL routine keeps its internal semicolons AND its
// closing one — the terminator after the final END belongs to the block, so it
// stays part of the returned statement instead of being dropped like an
// ordinary terminator.
func SplitSQLStatementsForDialect(sql, dialect string) []string {
	return splitSQLStatements(sql, platform.NormalizeDialect(dialect))
}

func splitSQLStatements(sql, dialect string) []string {
	if strings.TrimSpace(sql) == "" {
		return make([]string, 0)
	}

	sql = NormalizeClientDelimiters(sql)
	// Use SQL-standard string scanning so a semicolon inside a string literal
	// cannot leak out and be mis-split into an extra statement. Backslash
	// escapes are only honored for the dialects that actually process them;
	// the no-dialect path stays PostgreSQL-safe (backslash is literal).
	options := dialectlexer.Options(dialect)
	lexr := lexer.NewLexerWithOptions(sql, options)
	var statements []string
	var currentStatement strings.Builder
	state := sqlcompound.New(dialect)
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
		if platform.NormalizeDialect(dialect) == platform.SQLServer && token.MatchIdentifierValue("GO") {
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
				observeToken(&state, token)
				currentStatement.WriteString(token.Value)
				continue
			}
			skippingGoBatchLine = true
			continue
		}

		if token.Type == lexer.TokenSemicolon {
			if consumeSemicolon(&state, token, &currentStatement) {
				continue
			}

			// Found a statement terminator - add current statement if not empty
			stmt := strings.TrimSpace(currentStatement.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			currentStatement.Reset()
			state.Reset()
		} else {
			observeToken(&state, token)
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
		return make([]string, 0)
	}

	return statements
}

func handleSQLServerGoBatchSeparator(
	sql string,
	token lexer.Token,
	statements []string,
	currentStatement *strings.Builder,
	state *sqlcompound.State,
	batchStart int,
) ([]string, int, bool) {
	repeatCount, ok := sqlServerGoBatchSeparatorRepeatCountAt(sql, token.Start, token.End)
	if !ok {
		return statements, batchStart, false
	}
	statements, batchStart = appendSQLServerBatch(statements, batchStart, currentStatement.String(), repeatCount)
	currentStatement.Reset()
	state.Reset()
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

// consumeSemicolon writes a semicolon where it belongs and reports whether the
// statement continues past it.
//
// There are three answers rather than two. Inside a compound body the semicolon
// is body text and the statement goes on. At the end of an ordinary statement
// it is the client's terminator and is dropped. And at the end of a PL/SQL
// routine it is BOTH -- see [sqlcompound.State.TerminatorBelongsToStatement].
func consumeSemicolon(state *sqlcompound.State, token lexer.Token, current *strings.Builder) bool {
	if state.KeepSemicolonInsideStatement() {
		current.WriteString(token.Value)
		return true
	}
	if state.TerminatorBelongsToStatement() {
		current.WriteString(token.Value)
	}
	return false
}

// observeToken feeds an identifier to the compound-body state. Only identifiers
// carry the keywords the state reads; a quoted `"BEGIN"` arrives with its
// quotes and is therefore not the keyword.
func observeToken(state *sqlcompound.State, token lexer.Token) {
	if token.Type != lexer.TokenIdentifier {
		return
	}
	state.Word(token.Value)
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
