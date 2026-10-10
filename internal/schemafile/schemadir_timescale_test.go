package schemafile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

const readingsTable = `schema "app" {
}

table "readings" {
  schema = schema.app
  column "time" {
    type = timestamptz
  }
}
`

// TestLoadPath_AttachesAHypertableDeclaredBesideAnotherFile pins that a
// directory of HCL files is one schema for table settings too: the
// `hypertable` block attaches to the table another file declares, and is
// refused when no file of the directory declares it.
func TestLoadPath_AttachesAHypertableDeclaredBesideAnotherFile(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"tables.hcl":    readingsTable,
		"timescale.hcl": "hypertable \"app\" \"readings\" {\n  column = \"time\"\n  chunk_interval = \"1 day\"\n}\n",
	})

	database, err := schemafile.LoadPath(dir, schemafile.Options{YAML: builtintest.Runtime().YAML()})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	hypertable, found, err := schemaext.FacetAs[*tsschema.DesiredHypertable](database.Tables[0].Facets, tsschema.HypertableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(hypertable, qt.DeepEquals, &tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day"})
}

// TestLoadPath_RefusesAHypertableNoFileDeclaresTheTableOf is the refusal the
// directory keeps at the level of the whole schema.
func TestLoadPath_RefusesAHypertableNoFileDeclaresTheTableOf(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"tables.hcl":    readingsTable,
		"timescale.hcl": "hypertable \"app\" \"events\" {\n  column = \"time\"\n}\n",
	})

	database, err := schemafile.LoadPath(dir, schemafile.Options{YAML: builtintest.Runtime().YAML()})

	c.Assert(err, qt.ErrorMatches, `(?s).*timescale\.hcl.*hypertable "app\.events" names a table this schema does not declare`)
	c.Assert(database, qt.IsNil)
}
