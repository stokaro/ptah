package schemaload_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaload"
)

// TestReadManagedRows_ReadsWhatTheAnnotationNames covers the wiring publication
// depends on: the annotation names a file beside the Go source, and the rows
// have to be in the model before the artifact leaves the working copy.
func TestReadManagedRows_ReadsWhatTheAnnotationNames(t *testing.T) {
	c := qt.New(t)
	root := goRootWithManagedData(c, `
- code: NO
  name: Norway
- code: CZ
  name: Czechia
`)
	db, err := schemaload.LoadContext(context.Background(), schemaload.Options{RootDirs: []string{root}})
	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData, qt.HasLen, 1)
	c.Assert(db.ManagedData[0].Rows, qt.IsNil, qt.Commentf("loading a schema does not read row files"))

	err = schemaload.ReadManagedRows(db)

	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData[0].Rows, qt.HasLen, 2)
	c.Assert(db.ManagedData[0].Rows[0]["code"], qt.Equals, schemamodel.ManagedValue{Tag: "str", Text: "NO"})
	c.Assert(db.ManagedData[0].Rows[1]["name"], qt.Equals, schemamodel.ManagedValue{Tag: "str", Text: "Czechia"})
}

// TestReadManagedRows_EmptyFileIsNotAnUnreadOne pins the distinction a
// publisher decides on: an empty slice was read and declares nothing, a nil one
// was never read at all.
func TestReadManagedRows_EmptyFileIsNotAnUnreadOne(t *testing.T) {
	c := qt.New(t)
	root := goRootWithManagedData(c, "")
	db, err := schemaload.LoadContext(context.Background(), schemaload.Options{RootDirs: []string{root}})
	c.Assert(err, qt.IsNil)

	err = schemaload.ReadManagedRows(db)

	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData[0].Rows, qt.HasLen, 0)
	c.Assert(db.ManagedData[0].Rows, qt.IsNotNil)
}

func TestReadManagedRows_MissingFile(t *testing.T) {
	c := qt.New(t)
	root := goRootWithManagedData(c, "")
	c.Assert(os.Remove(filepath.Join(root, "countries.yaml")), qt.IsNil)
	db, err := schemaload.LoadContext(context.Background(), schemaload.Options{RootDirs: []string{root}})
	c.Assert(err, qt.IsNil)

	err = schemaload.ReadManagedRows(db)

	c.Assert(err, qt.ErrorMatches, `read managed data file .*countries.yaml" for table "countries": .*`)
}

func goRootWithManagedData(c *qt.C, rows string) string {
	c.Helper()
	root := c.TB.(*testing.T).TempDir()
	source := `package fixture

//ptah:schema:data table="countries" key="code" file="countries.yaml"
//ptah:schema:table name="countries"
type Country struct {
	//ptah:schema:field name="code" type="VARCHAR(2)" primary="true"
	Code string

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string
}
`
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(source), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "countries.yaml"), []byte(rows), 0o600), qt.IsNil)
	return root
}
