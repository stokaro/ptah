package sqlutil

import (
	"strings"

	"ptah.run/internal/clientdelimiter"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlcompound"
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
	// ClientDelimited reports whether a client delimiter the script declared
	// closed the statement, such as `$$` after `-- atlas:delimiter $$` or `//`
	// after `DELIMITER //`. Text then ends in the semicolon that delimiter is
	// read as, which the script does not hold. It is false for a semicolon the
	// script wrote, and for a statement the input ended.
	ClientDelimited bool
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

	normalized, origins := clientDelimitersFor(sql, dialect)
	lexr := lexer.NewLexerWithOptions(normalized, dialectlexer.Options(dialect))
	state := sqlcompound.New(dialect)

	var statements []SourceStatement
	var current strings.Builder
	for {
		token := lexr.NextToken()
		if token.Type == lexer.TokenEOF {
			break
		}
		if token.Type == lexer.TokenSemicolon && !state.KeepSemicolonInsideStatement() {
			statements = appendSourceStatement(statements, SourceStatement{
				Text:            current.String() + token.Value,
				Terminated:      true,
				ClientDelimited: standsForClientDelimiter(sql, origins, token.Start),
			})
			current.Reset()
			state.Reset()
			continue
		}
		observeToken(&state, token)
		current.WriteString(token.Value)
	}
	return appendSourceStatement(statements, SourceStatement{Text: current.String()})
}

// standsForClientDelimiter reports whether the semicolon at offset of the
// rewritten text replaced a client delimiter: the written text holds something
// other than a semicolon at the place origins maps it to.
func standsForClientDelimiter(sql string, origins clientdelimiter.Origins, offset int) bool {
	written := origins.Of(offset)
	return written < 0 || written >= len(sql) || sql[written] != ';'
}

// appendSourceStatement trims the leading trivia off one accumulated statement
// and keeps it when anything is left.
//
// Only the leading side is trimmed. The trailing side is the whole point: the
// bytes between the last token of the body and the terminator are part of what
// was written.
func appendSourceStatement(statements []SourceStatement, raw SourceStatement) []SourceStatement {
	raw.Text = trimLeadingSQLTrivia(raw.Text)
	// A terminated statement whose body is empty -- a stray `;`, or one
	// following nothing but a comment -- is trivia too, so the terminator alone
	// does not make it content.
	if strings.TrimSpace(strings.TrimSuffix(raw.Text, ";")) == "" {
		return statements
	}
	return append(statements, raw)
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
