package atlasschema_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
)

// TestSchemaSeamsCarryTheDropPolicyIntoTheVirtualTableGuard pins the plumbing
// between a caller's diff policy and the SQLite virtual-table guard.
//
// The guard runs before the policy: [atlasschema.PlanApply] reaches it inside
// schemadiff.CompareWithDatabase and only calls applyDiffPolicy on what comes
// back, and [atlasschema.Diff] calls it directly a few lines above its own
// applyDiffPolicy. So a project configured with `skip drop_table` was refused
// for a `DROP TABLE` the policy had already deleted, and told to set
// PTAH_SQLITE_ALLOW_UNREGISTERED_VIRTUAL_MODULE or
// PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP to obtain a plan that does nothing.
//
// Measured on the command before the fix, on an fts4 database built by a system
// SQLite that has the module, with `diff.skip: [drop_table]` in ptah.yaml:
//
//	ptah schema apply --db-url sqlite://fts4.db --to sqlite://desired.db
//	  -> unsupported feature: the database holds virtual table "docs" ..., exit 2
//	same run, both opt-ins set
//	  -> Schema is synced, no changes to be made., exit 0
//
// The fixture here uses fts5 because that module IS registered, so this build
// can create the index the guard then refuses to drop. That exercises the
// removal refusal and the plumbing; the unregistered-module half of the same
// policy lives in internal/sqlitevirtual, where the description can be built
// directly.
//
// The zero-policy rows are the controls. Without them this would pass just as
// well against a guard that had been switched off.
func TestSchemaSeamsCarryTheDropPolicyIntoTheVirtualTableGuard(t *testing.T) {
	tests := []struct {
		name         string
		policy       atlasschema.DiffPolicy
		wantErr      bool
		wantContains []string
		wantChanges  bool
	}{
		{
			// The control: the drop IS planned, so the refusal stands.
			name:    "a plan that would drop the virtual table is refused",
			policy:  atlasschema.DiffPolicy{},
			wantErr: true,
			wantContains: []string{
				`virtual table "docs" (module fts5)`,
				"the desired schema does not name",
			},
		},
		{
			// The row this test exists for.
			name:        "a plan whose table drops the policy removes is not refused",
			policy:      atlasschema.DiffPolicy{SkipDropTable: true},
			wantErr:     false,
			wantChanges: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" (apply)", func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(sqlitevirtual.AllowDropEnvVar)(t)
			dbPath, schemaPath := virtualDropPolicyFixture(c, t.TempDir())
			conn := connectSQLite(c, dbPath)
			defer dbschema.CloseAndWarn(conn)

			plan, err := atlasschema.PlanApply(t.Context(), conn, atlasschema.ApplyOptions{
				ToURLs: []string{"file://" + schemaPath},
				Policy: tt.policy,
			})

			c.Assert(err != nil, qt.Equals, tt.wantErr)
			for _, fragment := range tt.wantContains {
				c.Assert(err.Error(), qt.Contains, fragment)
			}
			c.Assert(plan.HasChanges(), qt.Equals, tt.wantChanges)
		})

		t.Run(tt.name+" (diff)", func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(sqlitevirtual.AllowDropEnvVar)(t)
			dbPath, schemaPath := virtualDropPolicyFixture(c, t.TempDir())

			report, err := atlasschema.Diff(context.Background(), atlasschema.DiffOptions{
				FromURLs: []string{"sqlite://" + dbPath},
				ToURLs:   []string{"file://" + schemaPath},
				Policy:   tt.policy,
			})

			c.Assert(err != nil, qt.Equals, tt.wantErr)
			for _, fragment := range tt.wantContains {
				c.Assert(err.Error(), qt.Contains, fragment)
			}
			c.Assert(len(report.Changes) > 0, qt.Equals, tt.wantChanges)
		})
	}
}

// virtualDropPolicyFixture builds the one comparison both seams are measured
// on: a live database holding an FTS5 index beside an ordinary table, and a
// desired document naming only the ordinary one. No Ptah document can declare a
// virtual table, so the index is undeclared on every desired side there is --
// which is exactly the shape that plans the DROP.
func virtualDropPolicyFixture(c *qt.C, dir string) (dbPath, schemaPath string) {
	c.Helper()

	dbPath = virtualToggleFixture(c, dir, "current.db",
		`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
		`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
	)
	schemaPath = filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE users (\n  id INTEGER PRIMARY KEY\n);\n",
	), 0o600), qt.IsNil)
	return dbPath, schemaPath
}
