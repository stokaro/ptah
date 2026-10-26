//go:build integration

package providercli_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	// The test reads the fixture's views back through a driver of its own, so
	// the answer does not pass through the provider the command used.
	_ "modernc.org/sqlite"

	"ptah.run/internal/testutils"
)

// commonObjectsSchema comments the table items, adds a table created with
// its comments, a view over items, a role and a grant to the role. The view's
// query has the spaces the fixture folds when it reads the view back, and the
// fixture's roles inherit nothing, so a second plan compares the provider's
// spelling of both.
const commonObjectsSchema = `tables:
  items:
    comment: the items
    columns:
      id: {type: INTEGER, not_null: true, comment: the key}
  notes:
    comment: the notes
    columns:
      id: {type: INTEGER, not_null: true, comment: the note key}
views:
  recent:
    body: "SELECT   id FROM items"
    comment: the recent items
roles:
  reader:
    login: true
    comment: reads items
grants:
  reader_items:
    role: reader
    privileges: [select]
    on_table: items
`

// commonObjectsFixture holds the table items, created through the provider,
// and leaves commonObjectsSchema to plan.
func commonObjectsFixture(c *qt.C) providerCLI {
	c.Helper()
	f := newProviderCLI(c, fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{}))
	c.Assert(os.WriteFile(f.schemaPath, []byte("tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\n"), 0o600), qt.IsNil)
	f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
	c.Assert(os.WriteFile(f.schemaPath, []byte(commonObjectsSchema), 0o600), qt.IsNil)
	return f
}

// keptCommonObjects reads the roles, grants and comments the fixture keeps
// beside its database, and the names of its views.
func (f providerCLI) keptCommonObjects(c *qt.C) (kept map[string]any, views []string) {
	c.Helper()
	data, err := os.ReadFile(f.databasePath + ".common.json")
	c.Assert(err, qt.IsNil)
	c.Assert(json.Unmarshal(data, &kept), qt.IsNil)
	db, err := sql.Open("sqlite", f.databasePath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT name || ': ' || sql FROM sqlite_schema WHERE type = 'view' ORDER BY name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	for rows.Next() {
		var view string
		c.Assert(rows.Scan(&view), qt.IsNil)
		views = append(views, view)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return kept, views
}

// TestProviderCommonObjects_HappyPath plans and applies a view, a role, a
// grant and comments, on a table it alters and on one it creates, through a
// provider that carries them on the common-object wire, reads each back from
// the database, and plans nothing the second time. The view keeps the query
// as declared; the comparison reads it in the provider's spelling.
func TestProviderCommonObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := commonObjectsFixture(c)

	f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath)
	f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve")

	kept, views := f.keptCommonObjects(c)
	c.Assert(views, qt.DeepEquals, []string{`recent: CREATE VIEW "recent" AS SELECT   id FROM items`})
	c.Assert(kept["roles"], qt.DeepEquals, []any{map[string]any{"name": "reader", "login": true}})
	c.Assert(kept["grants"], qt.DeepEquals, []any{map[string]any{"role": "reader", "privilege": "SELECT", "kind": "table", "name": "items"}})
	c.Assert(kept["comments"], qt.ContentEquals, []any{
		map[string]any{"kind": "view", "name": "recent", "text": "the recent items"},
		map[string]any{"kind": "role", "name": "reader", "text": "reads items"},
		map[string]any{"kind": "table", "name": "items", "text": "the items"},
		map[string]any{"kind": "column", "table": "items", "name": "id", "text": "the key"},
		map[string]any{"kind": "table", "name": "notes", "text": "the notes"},
		map[string]any{"kind": "column", "table": "notes", "name": "id", "text": "the note key"},
	})
	c.Assert(f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json"), qt.Contains, `"outcome": "no-changes"`)
}

// TestProviderCommonObjects_FailurePath refuses, before anything is written,
// a kind the provider does not carry, by name, and a declaration the provider
// cannot hold. The fixture's engine has common table identity, which lists no
// schema.validate of its own, so the host validates a declaration by
// rendering it and the provider refuses the role there. The client keeps the
// provider's words out of the message, since a provider's error text may hold
// a connection secret.
func TestProviderCommonObjects_FailurePath(t *testing.T) {
	tests := []struct {
		name, schema, want string
	}{
		{name: "a kind the provider does not carry",
			schema: "tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\nmatviews:\n  totals: {body: SELECT 1}\n",
			want:   "materialized views on the fixture engine, whose provider does not carry them"},
		{name: "a role the provider cannot hold",
			schema: "tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\nroles:\n  admin: {superuser: true}\n",
			want:   "provider render failed (Unknown)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := commonObjectsFixture(c)
			c.Assert(os.WriteFile(f.schemaPath, []byte(test.schema), 0o600), qt.IsNil)

			_, stderr, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath)

			c.Assert(code, qt.Not(qt.Equals), 0)
			c.Assert(stderr, qt.Contains, test.want)
			_, err := os.Stat(f.databasePath + ".common.json")
			c.Assert(os.IsNotExist(err), qt.IsTrue)
		})
	}
}
