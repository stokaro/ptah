package schemaserve_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/internal/schemaserve"
)

// The exclusion fixture declares `widgets` with a column the database lacks
// and a `reporting_cache` the database does not hold. The database holds
// `widgets` and an `external_audit` nothing declares. Without an exclusion the
// page reports a table to add and one to remove; excluding both leaves the
// missing column as the only drift.

// excludeFixtureOptions serve the fixture with exclude as the selection.
func excludeFixtureOptions(c *qt.C, exclude []string) schemaserve.Options {
	c.Helper()
	dir := c.TempDir()
	databaseURL := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "app.db"))
	conn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(),
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE external_audit (id INTEGER PRIMARY KEY);")
	c.Assert(err, qt.IsNil)
	c.Assert(conn.Close(), qt.IsNil)
	schemaFile := writeSchemaFile(c, dir,
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT);\nCREATE TABLE reporting_cache (id INTEGER PRIMARY KEY);\n")
	return schemaserve.Options{
		Runtime:     selectedRuntime(c),
		DatabaseURL: databaseURL,
		SchemaFiles: []string{schemaFile},
		Exclude:     exclude,
	}
}

// TestHandler_LeavesExcludedObjectsOutOfTheComparison_HappyPath reads the
// drift table of the page. The unexcluded row is the control: the fixture
// adds a table and removes one.
func TestHandler_LeavesExcludedObjectsOutOfTheComparison_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		has     []string
		lacks   []string
	}{
		{
			name: "nothing excluded",
			has:  []string{"columns_added", "tables_added", "tables_removed"},
		},
		{
			name:    "both tables excluded",
			exclude: []string{"external_audit", "reporting_cache"},
			has:     []string{"columns_added"},
			lacks:   []string{"tables_added", "tables_removed", "external_audit", "reporting_cache"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			built, err := schemaserve.Handler(c.Context(), excludeFixtureOptions(c, test.exclude))
			c.Assert(err, qt.IsNil)

			body := get(c, built)

			for _, fragment := range test.has {
				c.Assert(body, qt.Contains, fragment)
			}
			for _, fragment := range test.lacks {
				c.Assert(body, qt.Not(qt.Contains), fragment)
			}
		})
	}
}
