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

// liquibaseVacuumChangelog creates a table and then runs VACUUM, which SQLite
// refuses inside a transaction ("cannot VACUUM from within a transaction"). The
// second changeset carries runInTransaction:false for that reason, so the
// engine decides whether the import kept the attribute.
const liquibaseVacuumChangelog = "--liquibase formatted sql\n" +
	"--changeset s:1\nCREATE TABLE kept (id int);\n" +
	"--changeset s:2 runInTransaction:false\nVACUUM;\n"

// TestLiquibaseChangesetAttributesE2E_HappyPath drives the shipped binary over
// the changeset attributes that convert rather than refuse (stokaro/ptah#3714).
// runInTransaction:false becomes a no-transaction migration, which SQLite shows
// by running VACUUM; imported without it, `migrations up` fails on the VACUUM.
// ignore="true" is left out, as Liquibase leaves it out, and named on stderr.
func TestLiquibaseChangesetAttributesE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)

	t.Run("runInTransaction false applies outside a transaction", func(t *testing.T) {
		c := qt.New(t)
		work := c.TempDir()
		writeLiquibaseSource(c, work, "changelog.sql", liquibaseVacuumChangelog)

		_, importErr, err := runCLIProcess(ctx, work, binary,
			"migrations", "import", "--from", "liquibase", "--source-dir", "legacy", "--migrations-dir", "migrations")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("import: %s", importErr))
		_, upErr, err := runCLIProcess(ctx, work, binary,
			"migrations", "up", "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("up: %s", upErr))

		status, _, err := runCLIProcess(ctx, work, binary,
			"migrations", "status", "--db-url", "sqlite://app.db", "--migrations-dir", "migrations")

		c.Assert(exitStatusOf(c, err), qt.Equals, 0)
		c.Assert(status, qt.Contains, "Current Version: 2\n")
		c.Assert(status, qt.Contains, "Pending Migrations: 0\n")
	})

	t.Run("ignore true is left out and named", func(t *testing.T) {
		c := qt.New(t)
		work := c.TempDir()
		writeLiquibaseSource(c, work, "changelog.xml",
			`<databaseChangeLog><changeSet id="1" author="s"><sql>CREATE TABLE kept (id int);</sql></changeSet>`+
				`<changeSet id="2" author="s" ignore="true"><sql>DROP TABLE kept;</sql></changeSet></databaseChangeLog>`)

		stdout, stderr, err := runCLIProcess(ctx, work, binary,
			"migrations", "import", "--from", "liquibase", "--source-dir", "legacy", "--migrations-dir", "migrations")

		c.Assert(exitStatusOf(c, err), qt.Equals, 0)
		c.Assert(stdout, qt.Contains, "Wrote 2 migration file(s) to migrations\n")
		c.Assert(stderr, qt.Equals, "Skipped 1 changeset(s):\n"+
			"  changelog.xml s:2: ignore=\"true\": Liquibase never runs this changeset\n")
		_, statErr := os.Stat(filepath.Join(work, "migrations", "0000000002_s_2.up.sql"))
		c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
	})
}

// TestLiquibaseChangesetAttributesE2E_FailurePath covers a changeset attribute
// a Ptah migration has no form for. failOnError="false" makes Liquibase record
// the changeset as run when it fails; imported without it, a failure would stop
// the apply instead.
func TestLiquibaseChangesetAttributesE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	work := c.TempDir()
	writeLiquibaseSource(c, work, "changelog.xml",
		`<databaseChangeLog><changeSet id="1" author="s" failOnError="false"><sql>DROP TABLE absent;</sql></changeSet></databaseChangeLog>`)

	stdout, stderr, err := runCLIProcess(ctx, work, binary,
		"migrations", "import", "--from", "liquibase", "--source-dir", "legacy", "--migrations-dir", "migrations")

	c.Assert(exitStatusOf(c, err), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, `error: parse liquibase source: liquibase changeset s_1 in "changelog.xml" sets `+
		`failOnError=false, which Ptah cannot carry because Liquibase records the changeset as run when it fails, `+
		"and a failed Ptah migration stops the apply -- import it by hand\n")
	_, statErr := os.Stat(filepath.Join(work, "migrations"))
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestLiquibaseChangesetAttributesCompatE2E_HappyPath covers the compatibility
// surface, which reads Liquibase through the same recognizer. A numbered file
// with runInTransaction:false -- the form `ptah-compat migrate diff` writes for
// a statement that cannot run in a transaction -- applies; the pinned community
// binary v1.3.0 runs it in a transaction, and the VACUUM fails there. An
// ignored changeset in a conventional file is left out of the import and named.
func TestLiquibaseChangesetAttributesCompatE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binary)

	t.Run("numbered no-transaction file applies", func(t *testing.T) {
		c := qt.New(t)
		work := c.TempDir()
		writeLiquibaseSource(c, work, "1_table.sql", "--liquibase formatted sql\n--changeset atlas:1-1\nCREATE TABLE kept (id int);\n")
		writeLiquibaseSource(c, work, "2_vacuum.sql", "--liquibase formatted sql\n--changeset atlas:2-1 runInTransaction:false\nVACUUM;\n")
		_, hashErr, err := runCLIProcess(ctx, work, binary, "migrate", "hash", "--dir", "file://legacy?format=liquibase")
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("migrate hash: %s", hashErr))

		stdout, stderr, err := runCLIProcess(ctx, work, binary,
			"migrate", "apply", "--dir", "file://legacy?format=liquibase", "--url", "sqlite://app.db")

		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("apply: %s", stderr))
		c.Assert(stdout, qt.Contains, "Migration complete. Current version: 2\n")
	})

	t.Run("ignored changeset is left out and named", func(t *testing.T) {
		c := qt.New(t)
		work := c.TempDir()
		writeLiquibaseSource(c, work, "changelog.sql", "--liquibase formatted sql\n"+
			"--changeset s:1\nCREATE TABLE kept (id int);\n--changeset s:2 ignore:true\nDROP TABLE kept;\n")

		stdout, stderr, err := runCLIProcess(ctx, work, binary,
			"migrate", "import", "--from", "file://legacy?format=liquibase", "--to", "file://migrations")

		c.Assert(exitStatusOf(c, err), qt.Equals, 0)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "warning: Liquibase never runs these changesets, so they were not imported:\n"+
			"  changelog.sql s:2: ignore=\"true\": Liquibase never runs this changeset\n")
		_, statErr := os.Stat(filepath.Join(work, "migrations", "2_s_2.sql"))
		c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
	})
}
