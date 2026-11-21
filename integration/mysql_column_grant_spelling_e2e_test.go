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

// A declaration that grants one column under two spellings is refused before
// anything runs, and so is one that revokes, under another spelling, a
// privilege it grants on the column, and a plan that changes a role's
// privileges on a table where the catalog already holds one column under two
// spellings for it. The role keeps what it held. Measured on MySQL 8.4.13 and
// 26.10.0 and MariaDB 11.8.9 and 12.3.3, a GRANT SELECT (Name) beside a REVOKE
// SELECT (name) planned nothing, and a user holding the role read the column.
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
			wantErr:  `is granted privileges on one column of TABLE t1 as "Name" and as "name"`,
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
		{
			name:     "a grant and a revoke spelled two ways",
			held:     []string{"GRANT SELECT (Name) ON `%[1]s`.t1 TO %[2]s"},
			declared: spellingSchema + "GRANT SELECT (Name) ON t1 TO %[1]s;\nREVOKE SELECT (name) ON t1 FROM %[1]s;\n",
			wantErr:  `is granted SELECT on one column of TABLE t1 as "Name" and has it revoked as "name"`,
			want:     []string{"column t1.Name SELECT NO"},
		},
		{
			name:     "a grant and a revoke spelled two ways, for a role the schema does not create",
			held:     []string{"GRANT SELECT (Name) ON `%[1]s`.t1 TO %[2]s"},
			declared: unmanagedSchema + "GRANT SELECT (Name) ON t1 TO %[1]s;\nREVOKE SELECT (name) ON t1 FROM %[1]s;\n",
			wantErr:  `is granted SELECT on one column of TABLE t1 as "Name" and has it revoked as "name"`,
			want:     []string{"column t1.Name SELECT NO"},
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
// so is a grant the script revokes again under the same spelling; SELECT (id),
// which the declaration says nothing about, stays. Two revokes of the
// privilege under two spellings ask for one absence, so they are planned as
// one.
func TestSchemaApplyMySQLFamilyRevokesAColumnPrivilegeUnderEverySpellingE2E(t *testing.T) {
	tests := []struct {
		name   string
		revoke string
	}{
		{name: "a revoke spelled otherwise", revoke: "REVOKE SELECT (name) ON t1 FROM %[1]s;\n"},
		{name: "a revoke spelled as held", revoke: "REVOKE SELECT (Name) ON t1 FROM %[1]s;\n"},
		{
			name:   "a grant and then a revoke spelled as held",
			revoke: "GRANT SELECT (Name) ON t1 TO %[1]s;\nREVOKE SELECT (Name) ON t1 FROM %[1]s;\n",
		},
		{
			name:   "two revokes spelled two ways",
			revoke: "REVOKE SELECT (Name) ON t1 FROM %[1]s;\nREVOKE SELECT (name) ON t1 FROM %[1]s;\n",
		},
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

// keptBesideHeld is what the role holds before the runs of the second-spelling
// tests: SELECT, UPDATE and INSERT on Name and SELECT on id of t1, and SELECT
// on Name of v1.
var keptBesideHeld = []string{
	"GRANT SELECT (id, Name), UPDATE (Name), INSERT (Name) ON `%[1]s`.t1 TO %[2]s",
	"GRANT SELECT (Name) ON `%[1]s`.v1 TO %[2]s",
}

// A plan that would leave a role holding one column under two spellings is
// refused before it runs, on MySQL, where a grant takes the declared spelling.
// Measured on MySQL 8.4.13 and 26.10.0, a role the schema does not create kept
// INSERT and UPDATE on Name while the plan moved SELECT to name, and the next
// apply that changed the role's privileges on t1 exited 2 with the
// two-spellings refusal. The role keeps what it held, and a user holding it
// reads the column as the catalog spells it.
func TestSchemaApplyMySQLRefusesASecondSpellingOfAColumnE2E(t *testing.T) {
	tests := []struct {
		name     string
		held     []string
		declared string
		wantErr  string
		want     []string
	}{
		{
			name:     "a respelled privilege beside kept ones",
			held:     keptBesideHeld,
			declared: unmanagedSchema + "GRANT SELECT (name) ON t1 TO %[1]s;\n",
			wantErr:  `keeps INSERT, UPDATE on one column of t1 as "Name", and the plan grants SELECT on it as "name"`,
			want: []string{
				"column t1.id SELECT NO", "column t1.Name INSERT NO", "column t1.Name SELECT NO",
				"column t1.Name UPDATE NO", "column v1.Name SELECT NO",
			},
		},
		{
			name:     "a new privilege beside a kept one",
			held:     []string{"GRANT SELECT (Name) ON `%[1]s`.t1 TO %[2]s"},
			declared: unmanagedSchema + "GRANT UPDATE (name) ON t1 TO %[1]s;\n",
			wantErr:  `keeps SELECT on one column of t1 as "Name", and the plan grants UPDATE on it as "name"`,
			want:     []string{"column t1.Name SELECT NO"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRun(c, dbtarget.MySQLAdmin, test.held)

			applied := run.apply(c, test.declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", applied.Stdout))
			c.Assert(applied.Stderr, qt.Contains, test.wantErr)
			c.Assert(run.privileges(c), qt.DeepEquals, test.want)
			_, err := run.member(c).ExecContext(c.Context(), "SELECT Name FROM t1")
			c.Assert(err, qt.IsNil)
		})
	}
}

// A grant beside privileges a role keeps on the column is planned where it
// leaves one spelling: on MariaDB, which compares the column without case and
// grants it as the catalog spells it; on MySQL for a role the schema creates,
// whose privileges the declaration leaves out are revoked; and on both where
// the declaration revokes the kept privileges, under either spelling, as the
// second-spelling refusal advises. A revoke of UPDATE and INSERT beside a
// grant of SELECT names no privilege twice. A user holding the role runs what
// the declaration gives, and the next plan finds nothing to change.
func TestSchemaApplyMySQLFamilyGrantsBesideAKeptColumnPrivilegeE2E(t *testing.T) {
	tests := []struct {
		name     string
		admin    dbtarget.Engine
		held     []string
		declared string
		planned  []string
		want     []string
		reads    string
	}{
		{
			name:     "MariaDB, a respelled privilege beside kept ones",
			admin:    dbtarget.MariaDBAdmin,
			held:     keptBesideHeld,
			declared: unmanagedSchema + "GRANT SELECT (name) ON t1 TO %[1]s;\n",
			want: []string{
				"column t1.id SELECT NO", "column t1.Name INSERT NO", "column t1.Name SELECT NO",
				"column t1.Name UPDATE NO", "column v1.Name SELECT NO",
			},
			reads: "SELECT name FROM t1",
		},
		{
			name:     "MariaDB, a new privilege beside a kept one",
			admin:    dbtarget.MariaDBAdmin,
			held:     []string{"GRANT SELECT (Name) ON `%[1]s`.t1 TO %[2]s"},
			declared: unmanagedSchema + "GRANT UPDATE (name) ON t1 TO %[1]s;\n",
			planned:  []string{"GRANT UPDATE (`Name`) ON `t1` TO `%[1]s`;"},
			want:     []string{"column t1.Name SELECT NO", "column t1.Name UPDATE NO"},
			reads:    "UPDATE t1 SET name = 'b'",
		},
		{
			name:     "MySQL, a role the schema creates",
			admin:    dbtarget.MySQLAdmin,
			held:     keptBesideHeld,
			declared: spellingSchema + "GRANT SELECT (name) ON t1 TO %[1]s;\n",
			planned: []string{
				"REVOKE INSERT (`Name`) ON `t1` FROM `%[1]s`;",
				"REVOKE SELECT (`Name`) ON `t1` FROM `%[1]s`;",
				"REVOKE UPDATE (`Name`) ON `t1` FROM `%[1]s`;",
				"REVOKE SELECT (`id`) ON `t1` FROM `%[1]s`;",
				"REVOKE SELECT (`Name`) ON `v1` FROM `%[1]s`;",
				"GRANT SELECT (`name`) ON `t1` TO `%[1]s`;",
			},
			want:  []string{"column t1.name SELECT NO"},
			reads: "SELECT name FROM t1",
		},
		{
			name:     "MySQL, the kept privileges revoked as the catalog spells them",
			admin:    dbtarget.MySQLAdmin,
			held:     keptBesideHeld,
			declared: unmanagedSchema + revokeKeptPrivileges("Name"),
			planned:  revokedBesideARespelledGrant,
			want:     []string{"column t1.id SELECT NO", "column t1.name SELECT NO", "column v1.Name SELECT NO"},
			reads:    "SELECT name FROM t1",
		},
		{
			name:     "MySQL, the kept privileges revoked as the declaration spells them",
			admin:    dbtarget.MySQLAdmin,
			held:     keptBesideHeld,
			declared: unmanagedSchema + revokeKeptPrivileges("name"),
			planned:  revokedBesideARespelledGrant,
			want:     []string{"column t1.id SELECT NO", "column t1.name SELECT NO", "column v1.Name SELECT NO"},
			reads:    "SELECT name FROM t1",
		},
		{
			name:     "MariaDB, the kept privileges revoked as the catalog spells them",
			admin:    dbtarget.MariaDBAdmin,
			held:     keptBesideHeld,
			declared: unmanagedSchema + revokeKeptPrivileges("Name"),
			planned: []string{
				"REVOKE INSERT (`Name`) ON `t1` FROM `%[1]s`;",
				"REVOKE UPDATE (`Name`) ON `t1` FROM `%[1]s`;",
			},
			want:  []string{"column t1.id SELECT NO", "column t1.Name SELECT NO", "column v1.Name SELECT NO"},
			reads: "SELECT name FROM t1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRun(c, test.admin, test.held)

			applied := run.apply(c, test.declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(grantStatements(applied.Stdout, run.database), qt.DeepEquals, forRole(test.planned, run.role))
			c.Assert(run.privileges(c), qt.DeepEquals, test.want)
			_, err := run.member(c).ExecContext(c.Context(), test.reads)
			c.Assert(err, qt.IsNil)

			again := run.apply(c, test.declared, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// revokeKeptPrivileges grants the run's role SELECT on name of t1 and revokes
// the UPDATE and INSERT it keeps there, with the column of the revoke spelled
// as given.
func revokeKeptPrivileges(column string) string {
	return "GRANT SELECT (name) ON t1 TO %[1]s;\nREVOKE UPDATE (" + column + "), INSERT (" + column + ") ON t1 FROM %[1]s;\n"
}

// revokedBesideARespelledGrant is the MySQL plan for [revokeKeptPrivileges]
// against [keptBesideHeld]: the privileges on Name revoked, then SELECT granted
// on name.
var revokedBesideARespelledGrant = []string{
	"REVOKE INSERT (`Name`) ON `t1` FROM `%[1]s`;",
	"REVOKE SELECT (`Name`) ON `t1` FROM `%[1]s`;",
	"REVOKE UPDATE (`Name`) ON `t1` FROM `%[1]s`;",
	"GRANT SELECT (`name`) ON `t1` TO `%[1]s`;",
}

// forRole is planned with the role put in place of %[1]s.
func forRole(planned []string, role string) []string {
	statements := make([]string, 0, len(planned))
	for _, statement := range planned {
		statements = append(statements, strings.ReplaceAll(statement, "%[1]s", role))
	}
	return statements
}
