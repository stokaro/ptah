package sqlutil_test

import (
	"fmt"

	"go.5x5.cz/ptah/core/sqlutil"
)

// ExampleRebind rewrites one portable `?` template for each placeholder family
// Rebind targets: ordinal `$N` for the PostgreSQL family, `@pN` for SQL Server,
// `:N` for Oracle, and untouched for MySQL, whose native placeholder is
// already `?`. The question mark inside the single-quoted literal is user
// data, not a placeholder, and is skipped on every dialect.
func ExampleRebind() {
	const query = `UPDATE users SET name = ? WHERE id = ? AND note <> 'why?'`

	for _, dialect := range []string{"postgres", "sqlserver", "oracle", "mysql"} {
		fmt.Printf("%s\n  %s\n", dialect, sqlutil.Rebind(dialect, query))
	}

	// Output:
	// postgres
	//   UPDATE users SET name = $1 WHERE id = $2 AND note <> 'why?'
	// sqlserver
	//   UPDATE users SET name = @p1 WHERE id = @p2 AND note <> 'why?'
	// oracle
	//   UPDATE users SET name = :1 WHERE id = :2 AND note <> 'why?'
	// mysql
	//   UPDATE users SET name = ? WHERE id = ? AND note <> 'why?'
}

// ExampleSplitStatements feeds a small script through the one-call
// composition: comments are stripped and the remainder is split into
// executable statements. The semicolon inside the string literal does not
// split the INSERT, and the comments do not survive into the output — this is
// the form a caller executes statements in, not the source text (for that,
// see [sqlutil.SplitSourceStatements]).
func ExampleSplitStatements() {
	script := `-- seed schema
CREATE TABLE notes (id INT, body TEXT);
INSERT INTO notes VALUES (1, 'first; not a terminator'); /* trailing note */
`

	for i, stmt := range sqlutil.SplitStatements(script) {
		fmt.Printf("%d: %s\n", i+1, stmt)
	}

	// Output:
	// 1: CREATE TABLE notes (id INT, body TEXT)
	// 2: INSERT INTO notes VALUES (1, 'first; not a terminator')
}

// ExampleSplitStatementsForDialect shows why the dialect matters when
// replaying a migration file. The MySQL script declares its own terminator
// with a DELIMITER directive so the trigger body can carry semicolons, and
// its string literal hides a semicolon behind a backslash-escaped quote — a
// C-style escape only the MySQL/MariaDB/ClickHouse scanners honor. With
// dialect "mysql" the trigger comes back whole; a dialect-blind split would
// have cut it inside the literal.
func ExampleSplitStatementsForDialect() {
	script := `DELIMITER $$
CREATE TRIGGER audit_note AFTER INSERT ON notes
FOR EACH ROW
BEGIN
  INSERT INTO audit (msg) VALUES ('it\'s stored; safely');
END$$
DELIMITER ;
DROP TABLE drafts;`

	for i, stmt := range sqlutil.SplitStatementsForDialect("mysql", script) {
		fmt.Printf("-- statement %d --\n%s\n", i+1, stmt)
	}

	// Output:
	// -- statement 1 --
	// CREATE TRIGGER audit_note AFTER INSERT ON notes
	// FOR EACH ROW
	// BEGIN
	//   INSERT INTO audit (msg) VALUES ('it\'s stored; safely');
	// END
	// -- statement 2 --
	// DROP TABLE drafts
}

// ExampleSplitSourceStatements contrasts the verbatim split with the
// executable one. SplitSourceStatements keeps each statement as it was
// written — the newline before the terminator stays in Text, and Terminated
// records whether a semicolon closed the statement at all — while
// SplitSQLStatementsForDialect trims both sides and drops the terminator.
// Anything hashing statement bytes needs the verbatim form, because the two
// spellings below digest differently.
func ExampleSplitSourceStatements() {
	sql := "CREATE TABLE q (id int)\n;\nCREATE INDEX i ON q (id)"

	for _, stmt := range sqlutil.SplitSourceStatements(sql, "postgres") {
		fmt.Printf("%q terminated=%t\n", stmt.Text, stmt.Terminated)
	}
	for _, stmt := range sqlutil.SplitSQLStatementsForDialect(sql, "postgres") {
		fmt.Printf("%q\n", stmt)
	}

	// Output:
	// "CREATE TABLE q (id int)\n;" terminated=true
	// "CREATE INDEX i ON q (id)" terminated=false
	// "CREATE TABLE q (id int)"
	// "CREATE INDEX i ON q (id)"
}

// ExampleDefaultLooksLikeExpression classifies DEFAULT strings the way the
// catalog readers route them: a value the server wrapped in single or double
// quotes is a literal, anything else is SQL the server will evaluate, and an
// empty string is no default at all. The answer decides whether a default is
// declared as a value (rendered quoted) or as an expression (rendered as SQL).
func ExampleDefaultLooksLikeExpression() {
	for _, def := range []string{"'active'", "now()", `"pending"`, ""} {
		fmt.Printf("%q -> %t\n", def, sqlutil.DefaultLooksLikeExpression(def))
	}

	// Output:
	// "'active'" -> false
	// "now()" -> true
	// "\"pending\"" -> false
	// "" -> false
}
