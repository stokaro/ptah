//go:build integration

package providercli_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerconfig"
	"ptah.run/internal/testutils"
)

// builtInNameViews declares the table items and the view recent over it.
const builtInNameViews = "tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\n" +
	"views:\n  recent:\n    body: \"SELECT id FROM items\"\n"

// builtInNameViewsCLI is the CLI over the common-objects program serving the
// built-in engine name postgres, with builtInNameViews as its schema file.
// The provider owns its session, so the host holds no database/sql handle to
// it, although the built-in descriptor of postgres asks the server to spell
// a declared view body.
func builtInNameViewsCLI(c *qt.C) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "common-objects", testutils.FixtureRelease{})
	spec.ProviderID = builtInNameProviderID
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_ENGINE=postgres", "FIXTURE_DEV_PROVIDER_ID="+builtInNameProviderID)
	f := newProviderCLI(c, spec)
	spec.Environment = append(slices.Clone(spec.Environment), "PTAH_FIXTURE_PID_FILE="+f.pidPath)
	manifest, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{Spec: spec, Engines: []providerconfig.Engine{{ID: "postgres"}}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.manifestPath, manifest, 0o600), qt.IsNil)
	f.address = builtInNameAddress(f.databasePath)
	f.schemaPath = filepath.Join(filepath.Dir(f.schemaPath), "views.yaml")
	c.Assert(os.WriteFile(f.schemaPath, []byte(builtInNameViews), 0o600), qt.IsNil)
	return f
}

// The second apply compares a declared view with the one the database holds,
// which is where the comparison asks a PostgreSQL server to spell the declared
// body. The provider session has no transaction the host can roll back, so no
// server is asked: the view compares as the provider reads it, and the
// schema converges.
func TestProviderOfABuiltInEngineNameComparesAHeldView_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := builtInNameViewsCLI(c)

	f.success(c, "apply", "--provider-engine", "postgres", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
	_, diagnostics, code := f.run(c, c.Context(), "apply", "--provider-engine", "postgres", "--schema-file", f.schemaPath, "--tx-mode", "none",
		"--auto-approve")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))

	planned := f.success(c, "plan", "--provider-engine", "postgres", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")
	c.Assert(planned, qt.Contains, `"outcome": "no-changes"`)
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"items"})
	c.Assert(heldViewDefinitions(c, f.databasePath), qt.DeepEquals, []string{`recent: CREATE VIEW "recent" AS SELECT id FROM items`})
}

// heldViewDefinitions are the views the SQLite database at path holds, each
// as name: definition.
func heldViewDefinitions(c *qt.C, path string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), `SELECT name || ': ' || sql FROM sqlite_schema WHERE type = 'view' ORDER BY name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var views []string
	for rows.Next() {
		var view string
		c.Assert(rows.Scan(&view), qt.IsNil)
		views = append(views, view)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return views
}
