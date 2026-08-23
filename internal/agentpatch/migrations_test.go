package agentpatch_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentgate"
	"go.5x5.cz/ptah/internal/agentpatch"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agentworkspace"
	"go.5x5.cz/ptah/internal/migrateops"
	"go.5x5.cz/ptah/migration/migrator"
)

// migrationScope builds a workspace whose migrations class holds one hashed
// migration pair, which is what a real project looks like before a patch.
func migrationScope(c *qt.C) *agentworkspace.Scope {
	c.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1700000000_init.up.sql"),
		[]byte("CREATE TABLE users (id BIGINT PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1700000000_init.down.sql"),
		[]byte("DROP TABLE users;\n"), 0o600), qt.IsNil)
	_, err := migrateops.Rehash(dir, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)

	workspace, err := agentworkspace.Open(agentworkspace.Config{
		Root: root,
		Classes: map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig{
			agentpolicy.ClassMigrations: {Dir: "migrations", Writable: true},
		},
		Dialect: "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = workspace.Close() })

	scope, err := workspace.Scope(agentpolicy.ClassMigrations)
	c.Assert(err, qt.IsNil)
	return scope
}

// realGates is the gate runner the production path uses, so these tests measure
// the checks rather than a stand-in for them.
func realGates(c *qt.C) *agentgate.Runner {
	c.Helper()
	runner, err := agentgate.New(agentgate.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	return runner
}

// addStatus is the patch a model would compose to add one column.
func addStatus() []agentpatch.Change {
	return []agentpatch.Change{
		{
			Path:      "1700000100_add_status.up.sql",
			Operation: agentpatch.Create,
			Content:   "ALTER TABLE users ADD COLUMN status TEXT;\n",
		},
		{
			Path:      "1700000100_add_status.down.sql",
			Operation: agentpatch.Create,
			Content:   "ALTER TABLE users DROP COLUMN status;\n",
		},
	}
}

func TestApply_MigrationsRefreshTheIntegrityFile(t *testing.T) {
	// The end-to-end property #1487 asks for: after an agent-written migration
	// lands, the directory still validates. A patch that wrote the files and
	// left the checksum alone would produce a directory every executing Ptah
	// verb refuses.
	c := qt.New(t)
	scope := migrationScope(c)
	gates := realGates(c)

	before, err := gates.Run(context.Background(), scope)
	c.Assert(err, qt.IsNil)
	c.Assert(before.OK, qt.IsTrue)

	base, err := scope.Digest()
	c.Assert(err, qt.IsNil)
	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:          agentpolicy.ClassMigrations,
		ExpectedDigest: base,
		Changes:        addStatus(),
		Summary:        "add a status column",
	})
	c.Assert(err, qt.IsNil)

	result, err := agentpatch.Apply(context.Background(), plan, gates)

	c.Assert(err, qt.IsNil)
	c.Assert(result.RolledBack, qt.IsFalse)
	c.Assert(result.IntegrityRefreshed, qt.IsTrue)
	c.Assert(result.Introduced, qt.HasLen, 0)
	c.Assert(result.Verification.OK, qt.IsTrue)

	// The measured digest covers the rewritten checksum file, which the plan's
	// projection could not know about; both are reported rather than one being
	// quietly presented as the other.
	c.Assert(result.ProjectedDigest, qt.Equals, plan.ResultDigest())
	c.Assert(result.ResultDigest, qt.Not(qt.Equals), result.ProjectedDigest)

	after, err := gates.Run(context.Background(), scope)
	c.Assert(err, qt.IsNil)
	c.Assert(after.OK, qt.IsTrue)

	content, err := scope.ReadFile("1700000100_add_status.up.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, "ALTER TABLE users ADD COLUMN status TEXT;\n")
}

func TestApply_MigrationsRollBackToAValidatingDirectory(t *testing.T) {
	// The rollback has to restore the checksum too. Undoing the files and
	// leaving a checksum that describes the failed patch would leave the
	// repository in the state the gate exists to prevent.
	c := qt.New(t)
	scope := migrationScope(c)
	gates := realGates(c)

	base, err := scope.Digest()
	c.Assert(err, qt.IsNil)
	plan, err := agentpatch.PlanPatch(scope, agentpatch.Patch{
		Class:          agentpolicy.ClassMigrations,
		ExpectedDigest: base,
		Changes: []agentpatch.Change{{
			Path:      "1700000100_add_status.up.sql",
			Operation: agentpatch.Create,
			Content:   "ALTER TABL users ADD COLUMN status TEXT;\n",
		}},
	})
	c.Assert(err, qt.IsNil)

	result, err := agentpatch.Apply(context.Background(), plan, gates)

	c.Assert(err, qt.ErrorIs, agentpatch.ErrGateFailed)
	c.Assert(result.RolledBack, qt.IsTrue)
	c.Assert(result.RollbackFailure, qt.Equals, "")
	c.Assert(result.ResultDigest, qt.Equals, base)

	after, err := gates.Run(context.Background(), scope)
	c.Assert(err, qt.IsNil)
	c.Assert(after.OK, qt.IsTrue)

	_, statErr := scope.Stat("1700000100_add_status.up.sql")
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}
