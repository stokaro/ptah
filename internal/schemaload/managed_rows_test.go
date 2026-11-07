package schemaload_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/manageddata"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
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
	db, err := schemaload.LoadContext(context.Background(), schemaload.Options{Owners: builtintest.Runtime(), RootDirs: []string{root}})
	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData, qt.HasLen, 1)
	c.Assert(db.ManagedData[0].Rows, qt.IsNil, qt.Commentf("loading a schema does not read row files"))

	err = schemaload.ReadManagedRows(db, []string{root})

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
	db, err := schemaload.LoadContext(context.Background(), schemaload.Options{Owners: builtintest.Runtime(), RootDirs: []string{root}})
	c.Assert(err, qt.IsNil)

	err = schemaload.ReadManagedRows(db, []string{root})

	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData[0].Rows, qt.HasLen, 0)
	c.Assert(db.ManagedData[0].Rows, qt.IsNotNil)
}

func TestReadManagedRows_MissingFile(t *testing.T) {
	c := qt.New(t)
	root := goRootWithManagedData(c, "")
	c.Assert(os.Remove(filepath.Join(root, "countries.yaml")), qt.IsNil)
	db, err := schemaload.LoadContext(context.Background(), schemaload.Options{Owners: builtintest.Runtime(), RootDirs: []string{root}})
	c.Assert(err, qt.IsNil)

	err = schemaload.ReadManagedRows(db, []string{root})

	c.Assert(err, qt.ErrorMatches, `read managed data file .*countries.yaml" for table "countries": .*`)
}

// TestReadManagedRows_RefusesARowFileOutsideTheRoots keeps publication from carrying
// a file off the machine. The rows read here leave for a registry, so a declaration
// that reaches out of the project would upload whatever it named.
func TestReadManagedRows_RefusesARowFileOutsideTheRoots(t *testing.T) {
	setup := qt.New(t)
	root := goRootWithManagedData(setup, "- code: US\n  name: United States\n")
	other := t.TempDir()

	tests := []struct {
		name  string
		roots []string
	}{
		{name: "another root", roots: []string{other}},
		{name: "no root", roots: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := schemaload.LoadContext(context.Background(), schemaload.Options{Owners: builtintest.Runtime(), RootDirs: []string{root}})
			c.Assert(err, qt.IsNil)

			err = schemaload.ReadManagedRows(db, test.roots)

			c.Assert(err, qt.ErrorIs, manageddata.ErrOutsideRoot)
			c.Assert(db.ManagedData[0].Rows, qt.IsNil)
		})
	}
}

// TestReadManagedRows_ReadsThroughTheRootThatHoldsTheFile is the composite schema:
// every --root-dir is merged into it, so a file under the second reads too.
func TestReadManagedRows_ReadsThroughTheRootThatHoldsTheFile(t *testing.T) {
	c := qt.New(t)
	root := goRootWithManagedData(c, "- code: US\n  name: United States\n")
	db, err := schemaload.LoadContext(context.Background(), schemaload.Options{Owners: builtintest.Runtime(), RootDirs: []string{root}})
	c.Assert(err, qt.IsNil)

	err = schemaload.ReadManagedRows(db, []string{t.TempDir(), root})

	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData[0].Rows, qt.HasLen, 1)
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
