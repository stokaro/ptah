package generator_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/diffpolicy"
	"go.5x5.cz/ptah/migration/generator"
)

// TestGenerateMigrationCarriesTheDropPolicyIntoTheVirtualTableGuard is the
// generator's half of the same plumbing internal/atlasschema pins.
//
// The SQLite virtual-table guard runs inside schemadiff.CompareWithDatabase,
// which this package calls, while the skip filter that deletes the statements
// the guard predicts runs afterwards in planGeneratedMigrationSpecs. A project
// carrying `diff.skip: [drop_table]` was therefore refused for a DROP TABLE it
// had already configured away.
//
// Measured on the command, on an fts4 database built by a system SQLite that
// has the module: `ptah migrations generate` exited 2 with "the database holds
// virtual table \"docs\" (module fts4)" from a directory with no ptah.yaml, and
// exited 0 writing no migration from one carrying `diff.skip: [drop_table]`.
// The fixture below uses fts5 because that module is registered, so this build
// can create the index whose drop the guard refuses.
//
// The zero-policy row is the control: without it this passes against a guard
// that was simply switched off.
func TestGenerateMigrationCarriesTheDropPolicyIntoTheVirtualTableGuard(t *testing.T) {
	tests := []struct {
		name         string
		policy       generator.DiffPolicy
		wantErr      bool
		wantContains []string
	}{
		{
			name:    "a plan that would drop the virtual table is refused",
			policy:  generator.DiffPolicy{},
			wantErr: true,
			wantContains: []string{
				`virtual table "docs" (module fts5)`,
				"the desired schema does not name",
			},
		},
		{
			name: "a plan whose table drops the policy removes is not refused",
			policy: generator.DiffPolicy{
				SkipChangeKinds: []diffpolicy.ChangeKind{diffpolicy.DropTable},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			tempDir := t.TempDir()
			migrationsDir := filepath.Join(tempDir, "migrations")
			c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

			conn, err := dbschema.ConnectToDatabase(
				ctx, "sqlite://"+filepath.Join(tempDir, "app.db"),
			)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY)`)
			c.Assert(err, qt.IsNil)
			_, err = conn.ExecContext(ctx, `CREATE VIRTUAL TABLE docs USING fts5(title, body)`)
			c.Assert(err, qt.IsNil)

			_, err = generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
				Generated: &goschema.Database{
					Tables: []goschema.Table{{StructName: "users", Name: "users"}},
					Fields: []goschema.Field{
						{StructName: "users", Name: "id", Type: "INTEGER", Primary: true},
					},
				},
				DBConn:        conn,
				MigrationName: "drop_policy",
				OutputDir:     migrationsDir,
				DiffPolicy:    tt.policy,
			})

			c.Assert(err != nil, qt.Equals, tt.wantErr)
			for _, fragment := range tt.wantContains {
				c.Assert(err.Error(), qt.Contains, fragment)
			}
		})
	}
}
