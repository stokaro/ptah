package migrator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// verbatimDescriptionFiles holds the names whose two descriptions differ.
func verbatimDescriptionFiles() fstest.MapFS {
	return fstest.MapFS{
		"1__init.sql": &fstest.MapFile{Data: []byte("CREATE TABLE a (id int);\n")},
		"R__view.sql": &fstest.MapFile{Data: []byte("CREATE VIEW v AS SELECT 1 AS x;\n")},
	}
}

// atlasRevisionDescriptions reads version=description for every row of the
// Atlas revision table at dbPath, in version order.
func atlasRevisionDescriptions(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), "SELECT version || '=' || description FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	descriptions := make([]string, 0)
	for rows.Next() {
		var description string
		c.Assert(rows.Scan(&description), qt.IsNil)
		descriptions = append(descriptions, description)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return descriptions
}

// TestAtlasRevisionDescription_PtahsByDefault keeps the description Ptah
// records over an Atlas directory, which the native commands write.
func TestAtlasRevisionDescription_PtahsByDefault(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")

	err := runAtlasUp(c, dbPath, verbatimDescriptionFiles())

	c.Assert(err, qt.IsNil)
	c.Assert(atlasRevisionDescriptions(c, dbPath), qt.DeepEquals, []string{"1=init", "R=view"})
}

// TestAtlasRevisionDescription_VerbatimWhenSelected records the description
// the pinned community binary records for the same files, which
// ptah-compat selects.
func TestAtlasRevisionDescription_VerbatimWhenSelected(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "app.db")

	err := runAtlasUp(c, dbPath, verbatimDescriptionFiles(), migrator.WithAtlasVerbatimDescriptions())

	c.Assert(err, qt.IsNil)
	c.Assert(atlasRevisionDescriptions(c, dbPath), qt.DeepEquals, []string{"1=_init", "R=_view"})
}
