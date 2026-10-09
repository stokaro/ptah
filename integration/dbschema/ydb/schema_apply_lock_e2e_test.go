//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
)

const lockE2EEntities = `package entities

//ptah:schema:table name="lock_items" schema="ptah_ydb_e2e_lock"
type LockItem struct {
	//ptah:schema:field name="id" type="BIGINT" primary
	ID int64
}
`

// removeLockNode drops Ptah's coordination node when it exists, so a test
// starts from a database no locking run has used.
func removeLockNode(c *qt.C, line ydbLine) {
	c.Helper()
	if slices.Contains(directoryNames(c, context.Background(), line), dblock.YDBLockNode) {
		dropLockNode(c, line)
	}
}

// `ptah schema apply` locks on the coordination node the first locking run
// creates, and a dry run writes nothing to the database, the node included: it
// runs unlocked while the node does not exist. The schema reader leaves the
// node out of what it does not describe, so no plan offers to drop it.
func TestYDBBinary_SchemaApplyDryRunCreatesNoLockNode(t *testing.T) {
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			entities := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(entities, "items.go"), []byte(lockE2EEntities), 0o600), qt.IsNil)
			conn := openYDB(c, line)
			c.Cleanup(func() { dropDirectory(c, conn, "ptah_ydb_e2e_lock", "lock_items") })
			removeLockNode(c, line)
			before := directoryNames(c, ctx, line)

			previewed, previewErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", entities,
				"--schemas", "ptah_ydb_e2e_lock", "--dry-run")
			afterPreview := directoryNames(c, ctx, line)
			applied, applyErr := runBinary(ctx, binary, "schema", "apply", "--db-url", url, "--root-dir", entities,
				"--schemas", "ptah_ydb_e2e_lock", "--auto-approve")
			afterApply := directoryNames(c, ctx, line)
			live, readErr := dbschema.ReadSchemaWithSchemasContext(ctx, conn, nil)

			c.Assert(previewErr, qt.IsNil, qt.Commentf("schema apply --dry-run:\n%s", previewed))
			c.Assert(previewed, qt.Contains, "CREATE TABLE `ptah_ydb_e2e_lock/lock_items`")
			c.Assert(afterPreview, qt.DeepEquals, before)
			c.Assert(before, qt.Not(qt.Contains), dblock.YDBLockNode)
			c.Assert(applyErr, qt.IsNil, qt.Commentf("schema apply:\n%s", applied))
			c.Assert(afterApply, qt.Contains, dblock.YDBLockNode)
			c.Assert(readErr, qt.IsNil)
			c.Assert(live.FeatureCoverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("", dblock.YDBLockNode)).State, qt.Equals, schemaext.Complete)
			_, found, lookupErr := live.FeatureObjects.Get(ydbcoordination.Ref("", dblock.YDBLockNode))
			c.Assert(lookupErr, qt.IsNil)
			c.Assert(found, qt.IsFalse)
		})
	}
}
