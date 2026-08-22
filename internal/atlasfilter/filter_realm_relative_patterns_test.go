package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
)

// TestScopeDatabaseReport_CountsPatternPartsAgainstWhatTheRunDescribes pins
// both scopes of the one rule, so they cannot drift apart again.
//
// Measured against the pinned community binary v1.3.0 on PostgreSQL 17, one
// database, two URLs. It answers the same pattern two ways and the URL is what
// decides:
//
//	-u postgres://…/db                    --exclude public.users.id -> column dropped
//	-u postgres://…/db?search_path=public --exclude public.users.id -> too many parts
//
// Ptah answered the second way to both, because the pattern was counted against
// the connection's schema while the run described the whole realm -- and the
// run really does describe it: the same URL returns `public` and `app` and both
// their tables (stokaro/ptah#1703).
func TestScopeDatabaseReport_CountsPatternPartsAgainstWhatTheRunDescribes(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		realm   bool
		// wantErr is the refusal, empty where the pattern is honored.
		wantErr string
		// wantTables and wantUsersColumns are the fixture's own unless the
		// pattern took something: both are stated on every row so a pattern
		// that reaches an object it was not aimed at fails the row that names
		// the object it was aimed at.
		wantTables       []string
		wantUsersColumns []string
	}{
		{
			name:             "a realm run reads the leading segment as the schema",
			pattern:          "public.users.name",
			realm:            true,
			wantTables:       []string{"users", "app.orders"},
			wantUsersColumns: []string{"id"},
		},
		{
			// The prefixing is the binary's own arithmetic, not a diagnostic
			// bug: it counts the connection's schema before the pattern. Ptah
			// counts the same way and says so differently -- the pattern the
			// user typed, and the one that reaches the same column here.
			name:    "a schema-bound run still has its schema slot filled",
			pattern: "public.users.name",
			realm:   false,
			wantErr: `too many parts in pattern "public.users.name": this connection is bound to schema "public", so a pattern names object or object.child; write "users.name"`,
			// A refusal returns no schema at all, which is the point: nothing
			// was filtered, so there is nothing to have been filtered wrongly.
			wantTables:       nil,
			wantUsersColumns: nil,
		},
		{
			// The other schema, reached only in the realm scope. It is what
			// separates "the depth is allowed" from "the pattern is honored":
			// app.orders is not in the connection's schema at all.
			name:             "a realm run reaches a column in another schema",
			pattern:          "app.orders.id",
			realm:            true,
			wantTables:       []string{"users", "app.orders"},
			wantUsersColumns: []string{"id", "name"},
		},
		{
			// The control that the first attempt at this fix failed. Zeroing
			// the default schema for a realm run also makes the depth rule
			// pass, and the pattern is then accepted and matches nothing --
			// exit 0 with the column still in the output, which is worse than
			// the refusal it replaced.
			name:             "a realm run still resolves an object with no schema of its own",
			pattern:          "public.users",
			realm:            true,
			wantTables:       []string{"app.orders"},
			wantUsersColumns: nil,
		},
		{
			// And a pattern that names nothing must still name nothing. A row
			// that only asserted "no error" would pass on a build that
			// accepted every three-part pattern and honored none.
			name:             "a realm run leaves an absent column absent",
			pattern:          "public.users.nosuch",
			realm:            true,
			wantTables:       []string{"users", "app.orders"},
			wantUsersColumns: []string{"id", "name"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, _, err := atlasfilter.ScopeDatabaseReport(defaultSchemaFixture(), atlasfilter.Scope{
				Exclude:               []string{test.pattern},
				DefaultSchema:         "public",
				RealmRelativePatterns: test.realm,
			})

			c.Assert(errorMessage(err), qt.Equals, test.wantErr)
			c.Assert(scopedTableNames(got), qt.DeepEquals, test.wantTables)
			c.Assert(usersColumnNames(got), qt.DeepEquals, test.wantUsersColumns)
		})
	}
}

// errorMessage renders an error as a string so a row can state the absent case
// as a value.
func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// scopedTableNames is [tableNames] for a result that may be absent, so a
// refusal row states its outcome as a value instead of branching the test.
func scopedTableNames(schema *dbschematypes.DBSchema) []string {
	if schema == nil {
		return nil
	}
	return tableNames(schema.Tables)
}

// usersColumnNames returns the columns of the fixture's users table, or nil
// when the table itself left -- or when the run was refused and there is no
// schema to look in.
func usersColumnNames(schema *dbschematypes.DBSchema) []string {
	if schema == nil {
		return nil
	}
	for _, table := range schema.Tables {
		if table.Name == "users" && table.Schema == "" {
			return columnNames(table.Columns)
		}
	}
	return nil
}
