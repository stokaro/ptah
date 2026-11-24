//go:build integration

package integration_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A grant on a database name pattern that reaches the database, such as
// GRANT SELECT ON `d%`.*, gives the role the privilege on every table there,
// and a declared revoke on one of them is refused, naming the grant: for a
// pattern with "%" and one with "_", on a table the plan creates and on one
// that exists. Measured on MySQL 8.4.13 and MariaDB 11.8.9 with the pattern
// grants left out of the read: the plan for a created table has only CREATE
// TABLE t3 and the apply exits 0, the plan for an existing one reads as
// synced, and the pattern gives a member of the role t3 all the same. The
// refusal changes nothing: SHOW GRANTS lists the pattern grant,
// the role holds nothing on a table, and the member reads t1 through the
// pattern.
//
// The server reads a pattern its own way, which SQL LIKE does not follow. A
// backslash takes the next character literally under every sql_mode, so the
// name with each "_" escaped reaches the database, through a session under
// NO_BACKSLASH_ESCAPES too. And "_" is one byte, so two of them reach a
// database whose name has a two-byte letter in their place. Measured with LIKE
// deciding the match: the escaped name under NO_BACKSLASH_ESCAPES on MySQL
// 8.4.13, and the two-byte letter on MySQL 8.4.13 and MariaDB 11.8.9, read as
// synced while the member reads t3.
func TestSchemaApplyMySQLFamilyRevokeUnderAPatternGrantE2E(t *testing.T) {
	const (
		oneTable  = "CREATE TABLE t1 (id INT PRIMARY KEY);\n"
		twoTables = "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\n"
	)
	const revokeCreated = twoTables + "REVOKE SELECT ON t3 FROM %[1]s;\n"
	// The tables of the database with a two-byte letter in its name have no
	// key: the read of a key's name in a database of that name fails on the
	// server with Error 1280, on both engines (stokaro/ptah#6629).
	const (
		oneKeylessTable = "CREATE TABLE t1 (id INT);\n"
		revokeKeyless   = "CREATE TABLE t1 (id INT);\nCREATE TABLE t3 (id INT);\nREVOKE SELECT ON t3 FROM %[1]s;\n"
	)
	// A pattern is the database name, prefix and all, without its last cut
	// bytes, with each old string of replace written as its new one and
	// wildcard after it.
	tests := []struct {
		name               string
		prefix             string
		schema             string
		declared           string
		cut                int
		replace            []string
		wildcard           string
		sqlMode            string
		noBackslashEscapes bool
	}{
		{name: "a pattern with a percent sign", prefix: "fk_built", schema: oneTable, declared: revokeCreated, cut: 4, wildcard: "%", sqlMode: serverSQLMode},
		{name: "a pattern with an underscore", prefix: "fk_built", schema: oneTable, declared: revokeCreated, cut: 1, wildcard: "_", sqlMode: serverSQLMode},
		{name: "a table that exists", prefix: "fk_built", schema: twoTables, declared: twoTables + "REVOKE ALL ON t3 FROM %[1]s;\n", cut: 4, wildcard: "%", sqlMode: serverSQLMode},
		{
			name: "the name with each underscore escaped", prefix: "fk_built", schema: oneTable, declared: revokeCreated,
			replace: []string{"_", `\_`}, sqlMode: serverSQLMode,
		},
		{
			name: "the name with each underscore escaped, under NO_BACKSLASH_ESCAPES", prefix: "fk_built", schema: oneTable,
			declared: revokeCreated, replace: []string{"_", `\_`}, sqlMode: noBackslashEscapesMode, noBackslashEscapes: true,
		},
		{
			name: "two underscores in place of a two-byte letter", prefix: "fk_\u00e9", schema: oneKeylessTable, declared: revokeKeyless,
			replace: []string{"\u00e9", "__"}, sqlMode: serverSQLMode,
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunIn(c, engine.admin, test.prefix, test.schema, nil)
				run.url = withSQLMode(c, run.url, test.sqlMode)
				c.Assert(strings.Contains(sessionSQLMode(c, run.url), "NO_BACKSLASH_ESCAPES"), qt.Equals, test.noBackslashEscapes)
				pattern := strings.NewReplacer(test.replace...).Replace(run.database[:len(run.database)-test.cut]) + test.wildcard
				grantOnPattern(c, run, pattern)

				applied := run.apply(c, test.declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				quoted := regexp.QuoteMeta("SELECT ON `" + pattern + "`.*")
				c.Assert(applied.Stderr, qt.Matches, fmt.Sprintf(
					`(?s).*role "%[1]s" holds %[2]s, which covers table t3, so a revoke on the table cannot take it: revoke %[2]s from "%[1]s" instead.*`,
					run.role, quoted))
				c.Assert(showGrants(c, run), qt.Contains, "GRANT SELECT ON `"+pattern+"`.* TO `"+run.role+"`")
				c.Assert(run.privileges(c), qt.IsNil)
				_, err := run.member(c).ExecContext(c.Context(), "SELECT id FROM t1")
				c.Assert(err, qt.IsNil)
			})
		}
	}
}

// A grant on a database name pattern that does not reach the database keeps
// nothing on its tables, and a declared revoke on a table the plan creates is
// left out of the plan rather than refused: a pattern that needs one more
// character, one that differs from the name only in case on a server that
// keeps names apart by case, and the name with an escaped "%" in place of its
// first "_", which matches only a "%". Measured on MySQL 8.4.13 and MariaDB
// 11.8.9 with lower_case_table_names=0, a member of a role holding GRANT
// UPDATE ON `WC%`.* is refused an UPDATE on a table of wc1, while
// information_schema compares the pattern with wc1 alike under LIKE.
func TestSchemaApplyMySQLFamilyRevokeOutsideAPatternGrantE2E(t *testing.T) {
	const declared = "CREATE TABLE t1 (id INT PRIMARY KEY);\nCREATE TABLE t3 (id INT PRIMARY KEY);\nREVOKE SELECT ON t3 FROM %[1]s;\n"
	// A pattern is a format of the database name, of its prefix in upper case,
	// and of the name with an escaped "%" in place of its first "_".
	tests := []struct {
		name    string
		pattern string
	}{
		{name: "one character more", pattern: "%[1]s_"},
		{name: "another case", pattern: "%[2]s%%"},
		{name: "an escaped percent sign in place of an underscore", pattern: "%[3]s"},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, "CREATE TABLE t1 (id INT PRIMARY KEY);\n", nil)
				pattern := fmt.Sprintf(test.pattern, run.database, strings.ToUpper(run.database[:len(run.database)-4]),
					strings.Replace(run.database, "_", `\%`, 1))
				grantOnPattern(c, run, pattern)

				applied := run.apply(c, declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				again := run.apply(c, declared, "--dry-run")
				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
				c.Assert(showGrants(c, run), qt.Contains, "GRANT SELECT ON `"+pattern+"`.* TO `"+run.role+"`")
				c.Assert(run.privileges(c), qt.IsNil)
			})
		}
	}
}

// grantOnPattern grants the run's role SELECT on the databases pattern
// reaches. Dropping the role when the test ends drops the grant.
func grantOnPattern(c *qt.C, run columnGrantRun, pattern string) {
	c.Helper()
	// #nosec G202 -- the pattern and the role are names this test generates.
	_, err := run.scratch.admin.ExecContext(c.Context(), "GRANT SELECT ON `"+pattern+"`.* TO "+run.role)
	c.Assert(err, qt.IsNil)
}

// showGrants is what SHOW GRANTS lists for the run's role, as the server
// prints it.
func showGrants(c *qt.C, run columnGrantRun) string {
	c.Helper()
	// #nosec G202 -- the role is a name this test generates.
	rows, err := run.scratch.admin.QueryContext(c.Context(), "SHOW GRANTS FOR "+run.role)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var lines []string
	for rows.Next() {
		var line string
		c.Assert(rows.Scan(&line), qt.IsNil)
		lines = append(lines, line)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return strings.Join(lines, "\n")
}
