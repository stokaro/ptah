// Package sqlscript decides how a statement ends when a SQL script is written
// from a list of statements.
//
// Every writer that turns planned statements into a script -- the native
// migration generator, the Atlas `sql` template, plan files, split inspect
// output and the foreign migration layouts -- asks the same question of each
// statement: does a semicolon follow it? An executable statement needs one. A
// statement that is only comments does not: it is the planner's note or warning
// with nothing to run after it, and a semicolon there ends nothing and reads as
// a typo in a file a reviewer has to approve (stokaro/ptah#3903).
//
// The answer lives here, once, so that every writer gives it the same way.
package sqlscript

import (
	"strings"

	"ptah.run/core/sqlutil"
)

// CommentOnly reports whether statement holds nothing but comments and
// whitespace. An empty statement is comment-only too.
//
// The statement is scanned as SQL, so a comment marker inside a string literal
// does not make an executable statement look like a comment.
func CommentOnly(statement string) bool {
	return strings.TrimSpace(sqlutil.StripComments(statement)) == ""
}

// Terminator returns what a script writes after statement: a semicolon after
// an executable statement, and nothing after one that is only comments.
//
// It does not look at whether statement already ends with a semicolon. Each
// writer keeps its own handling of that, so an executable statement is written
// exactly as before.
func Terminator(statement string) string {
	if CommentOnly(statement) {
		return ""
	}
	return ";"
}
