package goschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
)

// writeQualifiedDependsOnPackage writes a package whose table names its
// dependency by the qualified spelling, which is what an author writing about a
// table in a named schema reaches for.
func writeQualifiedDependsOnPackage(c *qt.C, declared string) string {
	dir := c.TempDir()
	source := `package fixture

//ptah:schema:table name="aa_first" schema="app" depends_on="` + declared + `"
type First struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
}

//ptah:schema:table name="zz_second" schema="app"
type Second struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
}
`
	err := os.WriteFile(filepath.Join(dir, "schema.go"), []byte(source), 0o600)
	c.Assert(err, qt.IsNil)
	return dir
}

// TestParseTableDependsOnFoldsTheQualifier_HappyPath pins that a declared
// dependency reaches its table whichever spelling the author used.
//
// The two names come from different places: the dependency map is keyed on the
// qualified name a table renders under, and the declaration names its
// dependency the way its author wrote it. Matching on one spelling silently
// produces no edge, and the render is simply unchanged -- there is no
// diagnostic to notice (stokaro/ptah#3113).
func TestParseTableDependsOnFoldsTheQualifier_HappyPath(t *testing.T) {
	rows := []struct {
		name     string
		declared string
	}{
		{name: "the qualified spelling", declared: "app.zz_second"},
		{name: "the bare spelling", declared: "zz_second"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeQualifiedDependsOnPackage(c, row.declared)

			db, err := goschema.ParseDir(dir)

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 2)
			// Finalize orders the tables, so the dependency comes first.
			c.Assert(db.Tables[0].Name, qt.Equals, "zz_second")
			c.Assert(db.Tables[1].Name, qt.Equals, "aa_first")
		})
	}
}
