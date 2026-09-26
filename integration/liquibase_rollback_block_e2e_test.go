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

// liquibaseRollbackBlockChangelog rolls its first changeset back with a
// /* liquibase rollback block and declares its second needs none. Liquibase
// 5.0.4 applied it on SQLite, and rollback-count --count=2 dropped accounts and
// ledgers and kept audit_log (stokaro/ptah#3735).
const liquibaseRollbackBlockChangelog = "--liquibase formatted sql\n" +
	"--changeset s:1\nCREATE TABLE accounts (id int);\nCREATE TABLE ledgers (id int);\n" +
	"/* liquibase rollback\nDROP TABLE ledgers;\nDROP TABLE accounts;\n*/\n" +
	"--changeset s:2\nCREATE TABLE audit_log (id int);\n--rollback not required\n"

// liquibaseRollbackBlockNumbered holds a block whose inner `/* ... */` ends a
// SQL comment early. Liquibase 5.0.4 ends the block at the line that ends in
// `*/`, and its update created accounts and ledgers.
const liquibaseRollbackBlockNumbered = "--liquibase formatted sql\n" +
	"--changeset s:1\nCREATE TABLE accounts (id int);\n" +
	"/* liquibase rollback\nDROP TABLE accounts; /* accounts first */ DROP TABLE ledgers;\n*/\n" +
	"CREATE TABLE ledgers (id int);\n"

// TestLiquibaseRollbackBlockE2E_HappyPath drives the shipped binaries over the
// changelogs above and reads the database back. The native import rolls back
// what Liquibase rolls back, and the compatibility surface's numbered-file copy
// leaves the block out, says so, and applies what Liquibase applied.
func TestLiquibaseRollbackBlockE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	native := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), native)
	compat := filepath.Join(c.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), compat)

	t.Run("native import rolls back as Liquibase does", func(t *testing.T) {
		c := qt.New(t)
		work := c.TempDir()
		writeLiquibaseSource(c, work, "changelog.sql", liquibaseRollbackBlockChangelog)
		_, importErr, err := runCLIProcess(ctx, work, native,
			"migrations", "import", "--from", "liquibase", "--source-dir", "legacy", "--migrations-dir", "migrations")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("import: %s", importErr))
		_, upErr, err := runCLIProcess(ctx, work, native,
			"migrations", "up", "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("up: %s", upErr))
		_, downErr, err := runCLIProcess(ctx, work, native,
			"migrations", "down", "--confirm", "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("down: %s", downErr))

		schema, _, err := runCLIProcess(ctx, work, native, "db", "read", "--db-url", "sqlite://app.db")

		c.Assert(exitStatusOf(c, err), qt.Equals, 0)
		c.Assert(schema, qt.Contains, `CREATE TABLE "audit_log"`)
		c.Assert(schema, qt.Not(qt.Contains), "accounts")
		c.Assert(schema, qt.Not(qt.Contains), "ledgers")
	})

	t.Run("compatibility numbered copy", func(t *testing.T) {
		c := qt.New(t)
		work := c.TempDir()
		writeLiquibaseSource(c, work, "1_init.sql", liquibaseRollbackBlockNumbered)
		_, importErr, err := runCLIProcess(ctx, work, compat,
			"migrate", "import", "--from", "file://legacy?format=liquibase", "--to", "file://out")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("migrate import: %s", importErr))
		c.Assert(importErr, qt.Equals, "warning: an Atlas migration holds no rollback, so the rollback of this "+
			"changeset was not imported:\n  1_init.sql s:1\n")
		copied, err := os.ReadFile(filepath.Join(work, "out", "1_init.sql"))
		c.Assert(err, qt.IsNil)
		c.Assert(string(copied), qt.Equals, "--changeset s:1\nCREATE TABLE accounts (id int);\nCREATE TABLE ledgers (id int);\n")
		_, applyErr, err := runCLIProcess(ctx, work, compat,
			"migrate", "apply", "--dir", "file://out", "--url", "sqlite://app.db")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("apply: %s", applyErr))

		schema, _, err := runCLIProcess(ctx, work, native, "db", "read", "--db-url", "sqlite://app.db")

		c.Assert(exitStatusOf(c, err), qt.Equals, 0)
		c.Assert(schema, qt.Contains, `CREATE TABLE "accounts"`)
		c.Assert(schema, qt.Contains, `CREATE TABLE "ledgers"`)
	})
}

// TestLiquibaseRollbackBlockE2E_FailurePath covers a block whose lines
// Liquibase runs together: Liquibase 5.0.4 applied the changeset and then
// failed its rollback on `DELETE FROM accountsWHERE id = 1`. The import refuses
// the changelog, and writes nothing.
func TestLiquibaseRollbackBlockE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	work := c.TempDir()
	writeLiquibaseSource(c, work, "changelog.sql", "--liquibase formatted sql\n--changeset s:1\n"+
		"CREATE TABLE accounts (id int);\n/* liquibase rollback\nDELETE FROM accounts\nWHERE id = 1;\n*/\n")

	stdout, stderr, err := runCLIProcess(ctx, work, binary,
		"migrations", "import", "--from", "liquibase", "--source-dir", "legacy", "--migrations-dir", "migrations")

	c.Assert(exitStatusOf(c, err), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, `error: parse liquibase source: liquibase changeset s:1 in "changelog.sql" has a `+
		`rollback Liquibase does not run as written: Liquibase joins the lines of a /* liquibase rollback block, `+
		`and the block and a --rollback line after it, with nothing between them, so "DELETE FROM accounts" and `+
		`"WHERE id = 1;" run as "DELETE FROM accountsWHERE id = 1;" -- write the rollback as --rollback lines`+"\n")
	_, statErr := os.Stat(filepath.Join(work, "migrations"))
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}
