//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// MySQL and MariaDB take `Name` and `name` for one column, and so does the
// server's in-memory copy of the column privileges, but mysql.columns_priv
// keeps a row per spelling. A privilege granted under one spelling and revoked
// under the other stays in effect while the table loses its row: measured on
// all four servers, a user holding the role reads a column the declaration
// takes away and cannot update one it gives.

// spellingSchema is t1, the role and its column privileges; %[1]s is the role
// and %[2]s the column privileges, one GRANT statement each.
const spellingSchema = "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n" +
	"CREATE VIEW v1 AS SELECT id, name FROM t1;\nCREATE ROLE %[1]s;\n"

// grantStatements is the GRANT and REVOKE lines of a printed plan, in order,
// with the database the run creates left out of each name.
func grantStatements(printed, database string) []string {
	statements := make([]string, 0)
	for line := range strings.SplitSeq(printed, "\n") {
		for _, verb := range []string{"GRANT ", "REVOKE "} {
			if strings.HasPrefix(line, verb) {
				statements = append(statements, strings.ReplaceAll(line, "`"+database+"`.", ""))
			}
		}
	}
	return statements
}

// A declaration that spells a column the way the catalog does not is applied
// under the declared spelling on MySQL and matches the catalog's on MariaDB.
// A user holding the role reads and updates the column as the declaration
// spells it, and the next plan finds nothing to change.
//
// Measured on MySQL 8.4.13 and 26.10.0, a column privilege a user holds
// through a role serves only a statement that spells the column the way the
// GRANT did: with SELECT (Name) granted to the role, `SELECT Name` runs and
// `SELECT name` is ERROR 1143. So on MySQL the plan revokes the privilege on
// `Name` before it grants the declared one on `name`, and the catalog never
// holds both. MariaDB 11.8.9 and 12.3.3 serve both spellings, so there the
// held privilege stands and the one the plan adds takes the catalog's
// spelling.
func TestSchemaApplyMySQLFamilyColumnGrantSpellingE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		admin   dbtarget.Engine
		planned []string
		want    []string
	}{
		{
			name:  "MySQL",
			admin: dbtarget.MySQLAdmin,
			planned: []string{
				"REVOKE SELECT (`Name`) ON `t1` FROM `%[1]s`;",
				"GRANT SELECT (`name`) ON `t1` TO `%[1]s`;",
				"GRANT UPDATE (`name`) ON `t1` TO `%[1]s`;",
			},
			want: []string{"column t1.name SELECT NO", "column t1.name UPDATE NO"},
		},
		{
			name:    "MariaDB",
			admin:   dbtarget.MariaDBAdmin,
			planned: []string{"GRANT UPDATE (`Name`) ON `t1` TO `%[1]s`;"},
			want:    []string{"column t1.Name SELECT NO", "column t1.Name UPDATE NO"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRun(c, test.admin, []string{"GRANT SELECT (Name) ON `%[1]s`.t1 TO %[2]s"})
			declared := spellingSchema + "GRANT SELECT (name) ON t1 TO %[1]s;\nGRANT UPDATE (name) ON t1 TO %[1]s;\n"

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(grantStatements(applied.Stdout, run.database), qt.DeepEquals, forRole(test.planned, run.role))
			c.Assert(run.privileges(c), qt.DeepEquals, test.want)
			member := run.member(c)
			var names int
			c.Assert(member.QueryRowContext(c.Context(), "SELECT COUNT(name) FROM t1").Scan(&names), qt.IsNil)
			c.Assert(names, qt.Equals, 0)
			_, err := member.ExecContext(c.Context(), "UPDATE t1 SET name = 'b'")
			c.Assert(err, qt.IsNil)

			again := run.apply(c, declared, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// A declaration that spells one column two ways is refused before anything
// runs, and so is a plan that changes a role's privileges on a table where the
// catalog already holds one column under two spellings for it. The role keeps
// what it held.
func TestSchemaApplyMySQLFamilyColumnGrantSpellingE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		held     []string
		declared string
		wantErr  string
		want     []string
	}{
		{
			name:     "two spellings declared",
			declared: spellingSchema + "GRANT SELECT (Name) ON t1 TO %[1]s;\nGRANT UPDATE (name) ON t1 TO %[1]s;\n",
			wantErr:  "spell the column one way",
		},
		{
			name: "two spellings held",
			held: []string{
				"GRANT SELECT (Name) ON `%[1]s`.t1 TO %[2]s",
				"GRANT UPDATE (name) ON `%[1]s`.t1 TO %[2]s",
			},
			declared: spellingSchema + "GRANT UPDATE (name) ON t1 TO %[1]s;\n",
			wantErr:  "revoke the privileges under both spellings by hand",
			want:     []string{"column t1.Name SELECT NO", "column t1.name UPDATE NO"},
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRun(c, engine.admin, test.held)

				applied := run.apply(c, test.declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", applied.Stdout))
				c.Assert(applied.Stderr, qt.Contains, test.wantErr)
				c.Assert(run.privileges(c), qt.DeepEquals, test.want)
			})
		}
	}
}

// unmanagedSchema is t1 and v1 as the run creates them, and no role: the
// declarations that add to it grant to or revoke from the run's role without
// creating it.
const unmanagedSchema = "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n" +
	"CREATE VIEW v1 AS SELECT id, name FROM t1;\n"

// A revoke takes a column privilege under every spelling the server takes for
// the column, from a role the declaration does not create too. Measured on
// MySQL 8.4.13 and 26.10.0, a declared `REVOKE SELECT (name)` planned nothing
// against a held SELECT (Name), and a user holding the role read `Name` after
// the apply. The revoke spelled as the catalog spells it is the control, and
// SELECT (id), which the declaration says nothing about, stays.
func TestSchemaApplyMySQLFamilyRevokesAColumnPrivilegeUnderEverySpellingE2E(t *testing.T) {
	tests := []struct {
		name   string
		revoke string
	}{
		{name: "a revoke spelled otherwise", revoke: "REVOKE SELECT (name) ON t1 FROM %[1]s;\n"},
		{name: "a revoke spelled as held", revoke: "REVOKE SELECT (Name) ON t1 FROM %[1]s;\n"},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRun(c, engine.admin, []string{"GRANT SELECT (id, Name) ON `%[1]s`.t1 TO %[2]s"})
				declared := unmanagedSchema + test.revoke

				applied := run.apply(c, declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(grantStatements(applied.Stdout, run.database), qt.DeepEquals, []string{
					"REVOKE SELECT (`Name`) ON `t1` FROM `" + run.role + "`;",
				})
				c.Assert(run.privileges(c), qt.DeepEquals, []string{"column t1.id SELECT NO"})
				member := run.member(c)
				_, err := member.ExecContext(c.Context(), "SELECT id FROM t1")
				c.Assert(err, qt.IsNil)
				_, err = member.ExecContext(c.Context(), "SELECT Name FROM t1")
				c.Assert(err, qt.ErrorMatches, `(?s).*command denied to user.*`)

				again := run.apply(c, declared, "--dry-run")

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// A grant to a role the declaration does not create replaces a held privilege
// that differs from it only in the spelling of the column, on MySQL. Measured
// on MySQL 8.4.13 and 26.10.0, the plan granted SELECT (name) beside a held
// SELECT (Name), the catalog held the column under both spellings, and the
// next apply, adding UPDATE (name), was refused. MariaDB serves both
// spellings, so there the held privilege stands.
func TestSchemaApplyMySQLFamilyRespellsAColumnPrivilegeOfARoleTheSchemaDoesNotCreateE2E(t *testing.T) {
	tests := []struct {
		name        string
		admin       dbtarget.Engine
		planned     []string
		want        []string
		plannedNext []string
		wantNext    []string
	}{
		{
			name:        "MySQL",
			admin:       dbtarget.MySQLAdmin,
			planned:     []string{"REVOKE SELECT (`Name`) ON `t1` FROM `%[1]s`;", "GRANT SELECT (`name`) ON `t1` TO `%[1]s`;"},
			want:        []string{"column t1.name SELECT NO"},
			plannedNext: []string{"GRANT UPDATE (`name`) ON `t1` TO `%[1]s`;"},
			wantNext:    []string{"column t1.name SELECT NO", "column t1.name UPDATE NO"},
		},
		{
			name:        "MariaDB",
			admin:       dbtarget.MariaDBAdmin,
			want:        []string{"column t1.Name SELECT NO"},
			plannedNext: []string{"GRANT UPDATE (`Name`) ON `t1` TO `%[1]s`;"},
			wantNext:    []string{"column t1.Name SELECT NO", "column t1.Name UPDATE NO"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRun(c, test.admin, []string{"GRANT SELECT (Name) ON `%[1]s`.t1 TO %[2]s"})
			declared := unmanagedSchema + "GRANT SELECT (name) ON t1 TO %[1]s;\n"
			next := declared + "GRANT UPDATE (name) ON t1 TO %[1]s;\n"

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(grantStatements(applied.Stdout, run.database), qt.DeepEquals, forRole(test.planned, run.role))
			c.Assert(run.privileges(c), qt.DeepEquals, test.want)
			member := run.member(c)
			var names int
			c.Assert(member.QueryRowContext(c.Context(), "SELECT COUNT(name) FROM t1").Scan(&names), qt.IsNil)
			c.Assert(names, qt.Equals, 0)

			widened := run.apply(c, next, "--auto-approve")

			c.Assert(widened.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", widened.Stdout, widened.Stderr))
			c.Assert(grantStatements(widened.Stdout, run.database), qt.DeepEquals, forRole(test.plannedNext, run.role))
			c.Assert(run.privileges(c), qt.DeepEquals, test.wantNext)
			_, err := member.ExecContext(c.Context(), "UPDATE t1 SET name = 'b'")
			c.Assert(err, qt.IsNil)

			again := run.apply(c, next, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// A plan that moves a privilege to the declared spelling grants it again
// right after the revokes, so a statement that fails later in the plan leaves
// the role with the column. Measured on MySQL 8.4.13 and 26.10.0 with the
// GRANT placed after the tables, a UNIQUE key failing on duplicate rows left
// the role holding nothing on t1.
func TestSchemaApplyMySQLFamilyRespelledColumnPrivilegeOutlivesAFailedApplyE2E(t *testing.T) {
	tests := []struct {
		name  string
		admin dbtarget.Engine
		want  []string
	}{
		{name: "MySQL", admin: dbtarget.MySQLAdmin, want: []string{"column t1.name SELECT NO"}},
		{name: "MariaDB", admin: dbtarget.MariaDBAdmin, want: []string{"column t1.Name SELECT NO"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, test.admin, revokeFirstBase, []string{"GRANT SELECT (Name) ON `%[1]s`.t1 TO %[2]s"})
			failing := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50), UNIQUE KEY t1_name (name));\n" +
				"CREATE ROLE %[1]s;\nGRANT SELECT (name) ON t1 TO %[1]s;\n"

			failed := run.apply(c, failing, "--auto-approve")

			c.Assert(failed.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", failed.Stdout))
			c.Assert(failed.Stdout+failed.Stderr, qt.Contains, "Duplicate entry")
			c.Assert(run.privileges(c), qt.DeepEquals, test.want)
			member := run.member(c)
			var names int
			c.Assert(member.QueryRowContext(c.Context(), "SELECT COUNT(name) FROM t1").Scan(&names), qt.IsNil)
			c.Assert(names, qt.Equals, 2)
		})
	}
}

// forRole is planned with the role put in place of %[1]s.
func forRole(planned []string, role string) []string {
	statements := make([]string, 0, len(planned))
	for _, statement := range planned {
		statements = append(statements, strings.ReplaceAll(statement, "%[1]s", role))
	}
	return statements
}
