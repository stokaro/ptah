//go:build integration

package integration_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// liquibaseIgnoreLinesChangelog skips a block, a count, and a whole changeset
// with --ignoreLines. Liquibase 5.0.4 applied it on SQLite and created t1, t1b
// and t2, and recorded no changeset 9 (stokaro/ptah#3727).
const liquibaseIgnoreLinesChangelog = "--liquibase formatted sql\n" +
	"--ignoreLines:start\n--changeset s:9\nCREATE TABLE hidden_changeset (id int);\n--ignoreLines:end\n" +
	"--changeset s:1\nCREATE TABLE t1 (id int);\n--ignoreLines:start\nCREATE TABLE ignored_block (id int);\n" +
	"--ignoreLines:end\nCREATE TABLE t1b (id int);\n" +
	"--changeset s:2\n--ignoreLines:1\nCREATE TABLE ignored_count (id int);\nCREATE TABLE t2 (id int);\n"

// TestLiquibaseIgnoreLinesE2E_HappyPath drives the shipped binaries over the
// changelog above and reads the database back: the tables are the ones
// Liquibase created, through the native import and through the compatibility
// surface's numbered-file copy.
func TestLiquibaseIgnoreLinesE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	native := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), native)
	compat := filepath.Join(c.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), compat)

	t.Run("native import", func(t *testing.T) {
		c := qt.New(t)
		work := c.TempDir()
		writeLiquibaseSource(c, work, "changelog.sql", liquibaseIgnoreLinesChangelog)
		_, importErr, err := runCLIProcess(ctx, work, native,
			"migrations", "import", "--from", "liquibase", "--source-dir", "legacy", "--migrations-dir", "migrations")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("import: %s", importErr))
		_, upErr, err := runCLIProcess(ctx, work, native,
			"migrations", "up", "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("up: %s", upErr))

		schema, _, err := runCLIProcess(ctx, work, native, "db", "read", "--db-url", "sqlite://app.db")

		c.Assert(exitStatusOf(c, err), qt.Equals, 0)
		c.Assert(schema, qt.Contains, `CREATE TABLE "t1"`)
		c.Assert(schema, qt.Contains, `CREATE TABLE "t1b"`)
		c.Assert(schema, qt.Contains, `CREATE TABLE "t2"`)
		c.Assert(schema, qt.Not(qt.Contains), "hidden_changeset")
		c.Assert(schema, qt.Not(qt.Contains), "ignored_block")
		c.Assert(schema, qt.Not(qt.Contains), "ignored_count")
	})

	t.Run("compatibility numbered copy", func(t *testing.T) {
		c := qt.New(t)
		work := c.TempDir()
		writeLiquibaseSource(c, work, "1_init.sql", liquibaseIgnoreLinesChangelog)
		_, hashErr, err := runCLIProcess(ctx, work, compat, "migrate", "hash", "--dir", "file://legacy?format=liquibase")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("migrate hash: %s", hashErr))
		_, applyErr, err := runCLIProcess(ctx, work, compat,
			"migrate", "apply", "--dir", "file://legacy?format=liquibase", "--url", "sqlite://app.db")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("apply: %s", applyErr))

		schema, _, err := runCLIProcess(ctx, work, native, "db", "read", "--db-url", "sqlite://app.db")

		c.Assert(exitStatusOf(c, err), qt.Equals, 0)
		c.Assert(schema, qt.Contains, `CREATE TABLE "t1"`)
		c.Assert(schema, qt.Contains, `CREATE TABLE "t2"`)
		c.Assert(schema, qt.Not(qt.Contains), "hidden_changeset")
		c.Assert(schema, qt.Not(qt.Contains), "ignored_block")
		c.Assert(schema, qt.Not(qt.Contains), "ignored_count")
	})
}

// TestLiquibasePropertyReferenceE2E_FailurePath covers a changelog whose SQL
// holds a property reference. Liquibase 5.0.4 created the table under a
// different name depending on its environment -- from_property from the
// changelog, from_env with TBL set -- so the import refuses it, and writes
// nothing.
func TestLiquibasePropertyReferenceE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	work := c.TempDir()
	writeLiquibaseSource(c, work, "changelog.sql",
		"--liquibase formatted sql\n--property name:tbl value:from_property\n--changeset s:1\nCREATE TABLE ${tbl} (id int);\n")

	stdout, stderr, err := runCLIProcess(ctx, work, binary,
		"migrations", "import", "--from", "liquibase", "--source-dir", "legacy", "--migrations-dir", "migrations")

	c.Assert(exitStatusOf(c, err), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, `error: parse liquibase source: liquibase changeset s:1 in "changelog.sql" uses the `+
		`property reference ${tbl}; Liquibase fills it in when it runs, and an environment variable, a Java system `+
		`property or a command-line parameter of that name wins over any property the changelog defines, so the `+
		"changelog does not record the value that ran -- write the value in, or import the changeset by hand\n")
	_, statErr := os.Stat(filepath.Join(work, "migrations"))
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}
