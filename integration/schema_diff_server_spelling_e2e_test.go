//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// `schema diff` compares a database, or a migration directory replayed on the
// dev database, with the schema file that built it. The server stores a
// rewrite of each declaration in [serverRewrittenDeclarations]; compared as
// text, every one is dropped and created again (stokaro/ptah#3651). The
// comparison runs on the connection the --from side was read on, while it is
// still open, and asks that server how it spells the --to side's declarations.

// TestSchemaDiffFromADatabaseFindsAServerRewrittenDeclarationSyncedE2E diffs
// the database the SQL built against the same SQL as a file, with no dev
// database: the --from database answers for itself.
func TestSchemaDiffFromADatabaseFindsAServerRewrittenDeclarationSyncedE2E(t *testing.T) {
	for _, test := range serverRewrittenDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			source := databaseBuiltFrom(c, test.sql)

			out := runPtahNative(c, "schema", "diff", "--from", source, "--to", "file://"+schema)

			c.Assert(out, qt.Contains, "Schemas are synced")
		})
	}
}

// TestSchemaDiffFromADirectoryFindsAServerRewrittenDeclarationSyncedE2E diffs
// a directory whose one migration is the schema file, replayed on the dev
// database, against that file: the replay session answers before its cleanup.
func TestSchemaDiffFromADirectoryFindsAServerRewrittenDeclarationSyncedE2E(t *testing.T) {
	for _, test := range serverRewrittenDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb("schema", "diff", "--from", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Schemas are synced")
		})
	}
}

// TestSchemaDiffFromADatabasePlansAServerRewrittenDeclarationThatChangedE2E is
// the control for the database rows: a declaration that changed is planned.
func TestSchemaDiffFromADatabasePlansAServerRewrittenDeclarationThatChangedE2E(t *testing.T) {
	for _, test := range serverRewrittenControls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			source := databaseBuiltFrom(c, test.migration)

			out := runPtahNative(c, "schema", "diff", "--from", source, "--to", "file://"+schema)

			c.Assert(out, qt.Contains, test.wantInPlan)
		})
	}
}

// TestSchemaDiffFromADirectoryPlansAServerRewrittenDeclarationThatChangedE2E is
// the control for the directory rows.
func TestSchemaDiffFromADirectoryPlansAServerRewrittenDeclarationThatChangedE2E(t *testing.T) {
	for _, test := range serverRewrittenControls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb("schema", "diff", "--from", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, test.wantInPlan)
		})
	}
}
