//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// liquibaseDBMSChangelog holds a changeset for SQLite, one for MySQL, and one
// whose changes each name their own databases (stokaro/ptah#3715).
const liquibaseDBMSChangelog = `<databaseChangeLog>` +
	`<changeSet id="1" author="s" dbms="sqlite"><sql>CREATE TABLE on_sqlite (id int);</sql></changeSet>` +
	`<changeSet id="2" author="s" dbms="mysql"><sql>CREATE TABLE on_mysql (id int);</sql></changeSet>` +
	`<changeSet id="3" author="s"><sql dbms="!sqlite">CREATE TABLE not_sqlite (id int);</sql>` +
	`<sql dbms="sqlite, postgresql">CREATE TABLE also_sqlite (id int);</sql></changeSet>` +
	`</databaseChangeLog>`

// TestLiquibaseDBMSNameE2E_HappyPath drives the shipped binary with
// --liquibase-dbms sqlite, applies the result to SQLite, and reads the schema
// back: the tables Liquibase would have created on SQLite exist, and the ones
// it would have created only elsewhere do not. What was left out is named on
// stderr.
func TestLiquibaseDBMSNameE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	work := c.TempDir()
	writeLiquibaseSource(c, work, "changelog.xml", liquibaseDBMSChangelog)

	_, importErr, err := runCLIProcess(ctx, work, binary,
		"migrations", "import", "--from", "liquibase", "--source-dir", "legacy", "--migrations-dir", "migrations",
		"--liquibase-dbms", "sqlite")
	c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("import: %s", importErr))
	c.Assert(importErr, qt.Equals, "Skipped 1 changeset(s):\n"+
		"  changelog.xml s:2: dbms=\"mysql\" does not select sqlite\n"+
		"Skipped 1 change(s) inside imported changesets:\n"+
		"  changelog.xml s:3 <sql>: dbms=\"!sqlite\" does not select sqlite\n")
	_, upErr, err := runCLIProcess(ctx, work, binary,
		"migrations", "up", "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")
	c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("up: %s", upErr))

	schema, _, err := runCLIProcess(ctx, work, binary, "db", "read", "--db-url", "sqlite://app.db")

	c.Assert(exitStatusOf(c, err), qt.Equals, 0)
	c.Assert(schema, qt.Contains, `CREATE TABLE "on_sqlite"`)
	c.Assert(schema, qt.Contains, `CREATE TABLE "also_sqlite"`)
	c.Assert(schema, qt.Not(qt.Contains), "on_mysql")
	c.Assert(schema, qt.Not(qt.Contains), "not_sqlite")
}

// TestLiquibaseDBMSNameE2E_FailurePath is the control: the same changelog
// without the name is refused, because nothing says which database the history
// ran on, and a Ptah dialect name is not a name Liquibase gives a database.
func TestLiquibaseDBMSNameE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)

	tests := []struct {
		name   string
		args   []string
		stderr string
	}{
		{
			name: "no name",
			stderr: `error: parse liquibase source: liquibase changeset s_1 in "changelog.xml" is conditional on dbms` +
				liquibaseSelectorRefusal,
		},
		{
			name:   "a Ptah dialect name",
			args:   []string{"--liquibase-dbms", "sqlite3"},
			stderr: `(?s)error: --liquibase-dbms: "sqlite3" is not a database name Liquibase knows \(known: .*\)` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			writeLiquibaseSource(c, work, "changelog.xml", liquibaseDBMSChangelog)
			args := append([]string{
				"migrations", "import", "--from", "liquibase", "--source-dir", "legacy", "--migrations-dir", "migrations",
			}, test.args...)

			stdout, stderr, err := runCLIProcess(ctx, work, binary, args...)

			c.Assert(exitStatusOf(c, err), qt.Equals, 2)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Matches, test.stderr)
		})
	}
}
