//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestDownMigrationRestoresDroppedColumnLive executes both directions.
//
// Rendering is not applying, and a rollback that is never executed proves
// nothing about whether the column comes back. These rows run the forward plan
// against a real PostgreSQL, then the reverse plan, and ask
// information_schema.columns what the database holds afterwards. Without the
// identity-aware lookup the reverse plan is empty for the mismatched spellings
// and the column stays dropped -- with a rollback that exits 0.
func TestDownMigrationRestoresDroppedColumnLive(t *testing.T) {
	adminURL := livePostgresURLForRLSEnable(t)

	tests := []struct {
		name         string
		targetSchema string
		dbSchema     string
	}{
		{
			name:         "both sides spell the table the same way",
			targetSchema: "",
			dbSchema:     "",
		},
		{
			name:         "the target qualifies public and the database reports it bare",
			targetSchema: "public",
			dbSchema:     "",
		},
		{
			name:         "the target is bare and the database reports public",
			targetSchema: "",
			dbSchema:     "public",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbURL := createRLSEnableDatabase(c, adminURL)
			executeSQL(c, dbURL, []string{
				`CREATE TABLE users (id integer PRIMARY KEY, email text, legacy_note text)`,
				`INSERT INTO users (id, email, legacy_note) VALUES (1, 'a@example.com', 'keep')`,
			})

			desired := downColumnTarget(test.targetSchema)
			database := downColumnDatabase(test.dbSchema)

			forward := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, database, "postgres", must.Must(builtin.New())))
			up, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()),
				forward, "postgres",
			)
			c.Assert(err, qt.IsNil)
			executeSQL(c, dbURL, up)
			c.Assert(usersColumns(c, dbURL), qt.DeepEquals, []string{"email", "id"})

			down := planDownStatements(c, desired, database)
			c.Logf("down plan:\n%s", strings.Join(down, "\n"))
			executeSQL(c, dbURL, down)
			c.Assert(usersColumns(c, dbURL), qt.DeepEquals, []string{"email", "id", "legacy_note"})
		})
	}
}

// usersColumns reports the columns public.users holds, sorted by name.
func usersColumns(c *qt.C, dbURL string) []string {
	c.Helper()
	return queryStrings(c, dbURL,
		`SELECT column_name FROM information_schema.columns
		  WHERE table_schema = 'public' AND table_name = 'users'
		  ORDER BY column_name`)
}
