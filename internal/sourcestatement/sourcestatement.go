// Package sourcestatement recovers a statement the migrator sent as the
// migration body spells it.
//
// The migrator sends each statement trimmed and without its terminator, and
// reports a failure by the statement's 1-based position among the statements it
// split the body into. A diagnostic that quotes the statement the way the
// author wrote it reads the body's own text at that position instead. Apply,
// validate and lint in ptah-compat quote a failed statement this way, which is
// how the community Atlas binary quotes it, and the `migrate apply` report
// lists every statement a file executed this way too.
package sourcestatement

import (
	"strings"

	"ptah.run/core/sqlutil"
)

// Written is the statement at the 1-based index of body, as body spells it
// from its first token through its terminating semicolon. sent is the
// statement as the migrator sent it, and dialect names the engine whose rules
// split body. A statement a client delimiter closes, such as `$$` after
// `-- atlas:delimiter $$`, is quoted without it, as the community Atlas binary
// quotes it: the delimiter tells the client where the statement ends and is not
// part of it.
//
// The body's text at index is used only when it is the statement that was
// sent once its terminator and its comments are set aside, so a body the
// migrator split differently -- a template, a directive, a query unit --
// quotes sent rather than a neighbor. A last statement with no semicolon after
// it is quoted up to its last character that is not whitespace, which is the
// text the migrator sent. Written returns sent whenever index is out of range.
//
// The split is the dialect's plain statement split. Where the migrator splits a
// body into query units instead, a unit's position need not name the same
// statement here, and the check above then quotes sent.
func Written(body, dialect string, index int, sent string) string {
	return writtenAt(sqlutil.SplitSourceStatements(body, dialect), index-1, sent, dialect)
}

// Each is [Written] for every statement of body at once: entry i of the result
// is the statement sent[i] as body spells it. sent holds the statements of body
// in the order the caller split it, and body is split once whatever its length.
//
// A statement in sent may have its comments taken out. The comparison with the
// body's text sets comments aside as well as the terminator, and the text
// returned is the body's, so a comment the author wrote inside a statement
// stays in it. A last statement with no semicolon after it is the body's text
// up to its last character that is not whitespace, a comment after it
// included, which is how the community Atlas binary lists it.
func Each(body, dialect string, sent []string) []string {
	statements := sqlutil.SplitSourceStatements(body, dialect)
	written := make([]string, len(sent))
	for i, statement := range sent {
		written[i] = writtenAt(statements, i, statement, dialect)
	}
	return written
}

// writtenAt is the body's text for sent at the 0-based position of statements,
// or sent when the body's text there is another statement.
func writtenAt(statements []sqlutil.SourceStatement, position int, sent, dialect string) string {
	if position < 0 || position >= len(statements) {
		return sent
	}
	candidate := statements[position]
	text := strings.TrimSpace(candidate.Text)
	if candidate.Terminated {
		text = strings.TrimSuffix(text, ";")
	}
	if !sameStatement(text, sent, dialect) {
		return sent
	}
	if candidate.ClientDelimited {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace(candidate.Text)
}

// sameStatement reports whether two spellings of a statement differ in
// comments and surrounding whitespace alone.
func sameStatement(written, sent, dialect string) bool {
	if strings.TrimSpace(written) == strings.TrimSpace(sent) {
		return true
	}
	return strings.TrimSpace(sqlutil.StripCommentsForDialect(written, dialect)) ==
		strings.TrimSpace(sqlutil.StripCommentsForDialect(sent, dialect))
}
