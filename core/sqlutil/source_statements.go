package sqlutil

import (
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/lexer"
)

// SourceStatement is one statement as it was written, rather than as an
// executor would normalize it.
type SourceStatement struct {
	// Text runs from the statement's first meaningful token through its
	// terminating semicolon, verbatim. Whitespace and comments that preceded
	// the first token are not part of it; whitespace inside it — including a
	// newline between the body and the terminator — is.
	Text string
	// Terminated reports whether a semicolon closed the statement, as opposed
	// to the input ending.
	Terminated bool
}

// SplitSourceStatements returns each statement's source text.
//
// [SplitSQLStatementsForDialect] answers a different question and cannot be
// post-processed into this one: it trims the statement on both sides, so
//
//	CREATE TABLE q (id int)
//	;
//
// comes back as `CREATE TABLE q (id int)`, indistinguishable from the same
// statement written with the semicolon flush against it. Anything that hashes
// statement text needs to tell those apart, because the two spell different
// bytes and therefore different digests.
func SplitSourceStatements(sql, dialect string) []SourceStatement {
	if strings.TrimSpace(sql) == "" {
		return nil
	}

	normalized := NormalizeClientDelimiters(sql)
	lexr := lexer.NewLexerWithOptions(normalized, dialectlexer.Options(platform.NormalizeDialect(dialect)))
	state := statementSplitState{dialect: platform.NormalizeDialect(dialect)}

	var statements []SourceStatement
	var current strings.Builder
	for {
		token := lexr.NextToken()
		if token.Type == lexer.TokenEOF {
			break
		}
		if token.Type == lexer.TokenSemicolon && !state.keepSemicolonInsideStatement() {
			statements = appendSourceStatement(statements, current.String()+token.Value, true)
			current.Reset()
			state.reset()
			continue
		}
		state.observe(token)
		current.WriteString(token.Value)
	}
	return appendSourceStatement(statements, current.String(), false)
}

// appendSourceStatement trims the leading trivia off one accumulated statement
// and keeps it when anything is left.
//
// Only the leading side is trimmed. The trailing side is the whole point: the
// bytes between the last token of the body and the terminator are part of what
// was written.
func appendSourceStatement(statements []SourceStatement, raw string, terminated bool) []SourceStatement {
	text := trimLeadingSQLTrivia(raw)
	// A terminated statement whose body is empty -- a stray `;`, or one
	// following nothing but a comment -- is trivia too, so the terminator alone
	// does not make it content.
	if strings.TrimSpace(strings.TrimSuffix(text, ";")) == "" {
		return statements
	}
	return append(statements, SourceStatement{Text: text, Terminated: terminated})
}

// trimLeadingSQLTrivia drops leading whitespace and comments, returning "" when
// nothing but trivia is left.
func trimLeadingSQLTrivia(raw string) string {
	rest := raw
	for {
		trimmed := strings.TrimLeft(rest, " \t\r\n")
		switch {
		case strings.HasPrefix(trimmed, "--"):
			if end := strings.IndexAny(trimmed, "\r\n"); end >= 0 {
				rest = trimmed[end:]
				continue
			}
			return ""
		case strings.HasPrefix(trimmed, "/*"):
			if end := strings.Index(trimmed[2:], "*/"); end >= 0 {
				rest = trimmed[2+end+2:]
				continue
			}
			return ""
		default:
			rest = trimmed
		}
		break
	}
	if strings.TrimSpace(rest) == "" {
		return ""
	}
	return rest
}
