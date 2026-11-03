// Package sqlscript decides how a statement ends when a SQL script is written
// from a list of statements.
//
// Every writer that turns planned statements into a script -- the native
// migration generator, the Atlas `sql` template, plan files, split inspect
// output and the foreign migration layouts -- asks the same question of each
// statement: does a semicolon follow it? An executable statement needs one. A
// statement that is only comments does not: it is the planner's note or warning
// with nothing to run after it, and a semicolon there ends nothing and reads as
// a typo in a file a reviewer has to approve (stokaro/ptah#3903). And a
// statement that ends in a line comment takes its semicolon on the next line,
// since on the same line the comment would swallow it.
//
// The answer lives here, once, so that every writer gives it the same way.
package sqlscript

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/lexer"
)

// CommentOnly reports whether statement holds nothing but comments and
// whitespace. An empty statement is comment-only too.
//
// The statement is scanned as SQL, so a comment marker inside a string literal
// does not make an executable statement look like a comment.
func CommentOnly(statement string) bool {
	return strings.TrimSpace(sqlutil.StripComments(statement)) == ""
}

// EndsInLineComment reports whether statement ends inside a line comment: its
// last token is a `--` or `#` comment with no line break after it. Whatever is
// written after such a statement on the same line is part of the comment, so a
// semicolon there ends nothing and the next statement runs into this one.
//
// A splitter that trims a statement leaves it in this shape when the statement
// was written with a comment between its last token and its semicolon, as a
// view body can be: the line break that ended the comment is trimmed with the
// rest of the trailing whitespace.
//
// The statement is scanned by the dialect-neutral rules [CommentOnly] uses, so
// a `#` that starts a SQL Server temporary table name reads as a comment too.
// Answering true there costs a line break before the semicolon and nothing
// else.
func EndsInLineComment(statement string) bool {
	scanner := lexer.NewLexer(statement)
	var last lexer.Token
	for token := scanner.NextToken(); token.Type != lexer.TokenEOF; token = scanner.NextToken() {
		last = token
	}
	return last.Type == lexer.TokenComment && !strings.HasPrefix(last.Value, "/*")
}

// Terminator returns what a script writes after statement: a semicolon after
// an executable statement, and nothing after one that is only comments. An
// executable statement that ends in a line comment, as [EndsInLineComment]
// reads it, takes its semicolon on a line of its own.
//
// It does not look at whether statement already ends with a semicolon. Each
// writer keeps its own handling of that, so an executable statement is written
// exactly as before.
func Terminator(statement string) string {
	switch {
	case CommentOnly(statement):
		return ""
	case EndsInLineComment(statement):
		return "\n;"
	default:
		return ";"
	}
}

// BatchEnd returns what a script of dialect writes after statement and its
// terminator to end the batch: a line break and the dialect's batch separator
// (descriptor.Scripts.BatchSeparator) after an executable statement. It is
// empty after a statement that is only comments, which has nothing to run,
// and for a dialect whose scripts have no batches.
//
// SQL Server is the dialect that has one, GO. A CREATE TRIGGER, PROCEDURE,
// FUNCTION or VIEW must open its batch, and a trigger's or a routine's body
// runs to the end of its batch, so with nothing between them a module
// statement takes the statements after it into its body. Ptah runs every
// statement of a script as a batch of its own, so a separator after each one
// is the script that sqlcmd runs as Ptah does. Measured on SQL Server 2025, a
// separator after the module statements alone is not enough: sqlcmd refuses
// a CREATE VIEW that follows a CREATE TABLE in its batch with Msg 111.
func BatchEnd(dialect, statement string) string {
	if CommentOnly(statement) {
		return ""
	}
	described, ok := platform.Lookup(dialect)
	if !ok || described.Scripts.BatchSeparator == "" {
		return ""
	}
	return "\n" + described.Scripts.BatchSeparator
}

// StatementEnd returns what a script of dialect writes after a statement a
// splitter returned: [Terminator], then [BatchEnd].
//
// Where a batch separator follows, a statement that already ends in a
// semicolon takes no second one. A splitter keeps the semicolon after the last
// statement of a T-SQL body that is not a BEGIN ... END block, since the body
// runs to the end of the batch, and a second semicolon there writes `;;`.
func StatementEnd(dialect, statement string) string {
	end := BatchEnd(dialect, statement)
	if end != "" && strings.HasSuffix(strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, dialect)), ";") {
		return end
	}
	return Terminator(statement) + end
}

// WithBatchEnd returns a statement a renderer wrote, terminator included,
// with [BatchEnd] after it: the separator goes on the line after the
// statement's last line, and the line breaks the statement ended with follow
// the separator, so the layout around it stays as it was.
func WithBatchEnd(dialect, statement string) string {
	end := BatchEnd(dialect, statement)
	if end == "" {
		return statement
	}
	body := strings.TrimRight(statement, " \t\r\n")
	return body + end + statement[len(body):]
}
