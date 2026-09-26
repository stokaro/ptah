//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestLiquibaseDiffRollbackE2E_HappyPath writes two migrations with
// `ptah-compat migrate diff` in the Liquibase layout -- one creating widgets,
// one dropping it -- and runs their rollbacks through the native import, which
// reads formatted SQL as Liquibase does. Each rollback runs: the second one
// recreates widgets from a statement written over several `--rollback` lines,
// and the first drops it again.
//
// Liquibase 5.0.4 ran the same two changesets on SQLite and did the same:
// rollback-count --count=1 recreated widgets, and a second one dropped it. The
// `--rollback: <SQL>` spelling the pinned community binary v1.3.0 writes is a
// comment to Liquibase, so the changeset had no rollback and rollback-count
// refused it (stokaro/ptah#3752).
func TestLiquibaseDiffRollbackE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	native := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), native)
	compat := filepath.Join(c.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), compat)
	work := c.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(work, "migrations"), 0o755), qt.IsNil)
	created := filepath.Join(work, "created.sql")
	c.Assert(os.WriteFile(created, []byte("CREATE TABLE widgets (\n  id INTEGER PRIMARY KEY,\n  name TEXT NOT NULL\n);\n"), 0o600), qt.IsNil)
	dropped := filepath.Join(work, "dropped.sql")
	c.Assert(os.WriteFile(dropped, []byte("-- no tables\n"), 0o600), qt.IsNil)
	diff := func(name, target string) {
		_, diffErr, err := runCLIProcess(ctx, work, compat, "migrate", "diff", name,
			"--dir", "file://migrations?format=liquibase", "--dev-url", "sqlite://file?mode=memory", "--to", "file://"+target)
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("migrate diff %s: %s", name, diffErr))
	}
	diff("create_widgets", created)
	diff("drop_widgets", dropped)
	_, importErr, err := runCLIProcess(ctx, work, native,
		"migrations", "import", "--from", "liquibase", "--source-dir", "migrations", "--migrations-dir", "imported")
	c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("import: %s", importErr))
	_, upErr, err := runCLIProcess(ctx, work, native,
		"migrations", "up", "--allow-destructive", "--db-url", "sqlite://app.db", "--migrations-dir", "imported")
	c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("up: %s", upErr))

	_, downErr, err := runCLIProcess(ctx, work, native,
		"migrations", "down", "--confirm", "--target", "1", "--db-url", "sqlite://app.db", "--migrations-dir", "imported")
	c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("down to 1: %s", downErr))
	recreated, _, err := runCLIProcess(ctx, work, native, "db", "read", "--db-url", "sqlite://app.db")
	c.Assert(exitStatusOf(c, err), qt.Equals, 0)
	_, downErr, err = runCLIProcess(ctx, work, native,
		"migrations", "down", "--confirm", "--db-url", "sqlite://app.db", "--migrations-dir", "imported")
	c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("down to 0: %s", downErr))
	emptied, _, err := runCLIProcess(ctx, work, native, "db", "read", "--db-url", "sqlite://app.db")

	c.Assert(exitStatusOf(c, err), qt.Equals, 0)
	c.Assert(recreated, qt.Contains, `CREATE TABLE "widgets"`)
	c.Assert(recreated, qt.Contains, `"name" TEXT NOT NULL`)
	c.Assert(emptied, qt.Not(qt.Contains), "widgets")
}
