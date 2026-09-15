package schemaload_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"oras.land/oras-go/v2/content/memory"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaartifact"
	"ptah.run/internal/schemaload"
)

// materializedArtifact publishes a schema that declares rows, pulls it back and
// writes it to a directory, which is what `ptah schema pull` does. It returns
// the path of the canonical HCL.
func materializedArtifact(c *qt.C, db *schemamodel.Database) string {
	store := memory.New()
	_, err := schemaartifact.PushTo(
		context.Background(), store, db, schemaartifact.PushOptions{Latest: true},
	)
	c.Assert(err, qt.IsNil)
	pulled, err := schemaartifact.PullFrom(context.Background(), store, "latest")
	c.Assert(err, qt.IsNil)
	written, err := schemaartifact.Materialize(pulled, filepath.Join(c.TempDir(), "schema.hcl"))
	c.Assert(err, qt.IsNil)
	return written[0]
}

// regionsDatabase declares one table and the rows the annotation owns in it.
// The row file name is the author's, and it points into the working copy that
// published the artifact: nothing at the consuming end can read it, which is
// what makes the layer beside the schema the only account of the rows.
func regionsDatabase() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Region", Name: "regions"}},
		Fields: []schemamodel.Field{
			{StructName: "Region", FieldName: "Code", Name: "code", Type: "varchar(8)", Primary: true},
			{StructName: "Region", FieldName: "Name", Name: "name", Type: "varchar(64)", Nullable: false},
		},
	}
	schemamodel.Finalize(db)
	db.ManagedData = []schemamodel.ManagedData{{
		Table: "regions",
		Keys:  []string{"code"},
		File:  "regions.yaml",
		Rows: []schemamodel.ManagedRow{
			{"code": {Tag: "str", Text: "CZ"}, "name": {Tag: "str", Text: "Czechia"}},
			{"code": {Tag: "str", Text: "SK"}, "name": {Tag: "str", Text: "Slovakia"}},
		},
	}}
	return db
}

// TestLoad_MaterializedArtifactCarriesItsDeclaredRows is the consumer half of
// stokaro/ptah#3256. A process that holds database credentials and no registry
// credentials cannot read the artifact itself: it reads what was materialized
// for it. Reading the canonical HCL alone gives it a declaration whose `file`
// names a path only the publisher had, so the rows have to arrive through the
// layer written beside the schema.
func TestLoad_MaterializedArtifactCarriesItsDeclaredRows(t *testing.T) {
	c := qt.New(t)
	path := materializedArtifact(c, regionsDatabase())

	db, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})

	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData, qt.HasLen, 1)
	c.Assert(db.ManagedData[0].Rows, qt.DeepEquals, []schemamodel.ManagedRow{
		{"code": {Tag: "str", Text: "CZ"}, "name": {Tag: "str", Text: "Czechia"}},
		{"code": {Tag: "str", Text: "SK"}, "name": {Tag: "str", Text: "Slovakia"}},
	})
}

// TestLoad_MaterializedArtifactIsReadableAsADirectory keeps the directory
// spelling working too: `--schema-file` takes a directory of schema files, and
// a materialized artifact is one.
func TestLoad_MaterializedArtifactIsReadableAsADirectory(t *testing.T) {
	c := qt.New(t)
	path := materializedArtifact(c, regionsDatabase())

	db, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{filepath.Dir(path)}})

	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData, qt.HasLen, 1)
	c.Assert(db.ManagedData[0].Rows, qt.HasLen, 2)
}

// TestLoad_SchemaWithoutARowLayerKeepsItsDeclaration is the control: the layer
// is read where it exists, and its absence is left to the planner, which
// already refuses a declaration carrying no rows and says so.
func TestLoad_SchemaWithoutARowLayerKeepsItsDeclaration(t *testing.T) {
	c := qt.New(t)
	path := materializedArtifact(c, regionsDatabase())
	c.Assert(os.Remove(filepath.Join(filepath.Dir(path), schemaartifact.ManagedDataFileName)), qt.IsNil)

	db, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})

	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData, qt.HasLen, 1)
	c.Assert(db.ManagedData[0].Rows, qt.IsNil)
}

// TestLoad_RefusesARowLayerTheSchemaDoesNotDeclare keeps the reader from
// accepting rows nobody asked for. A layer that names another table is a
// mismatch between two halves of one statement, and skipping it quietly is how
// a schema deploys without the rows its author declared.
func TestLoad_RefusesARowLayerTheSchemaDoesNotDeclare(t *testing.T) {
	c := qt.New(t)
	path := materializedArtifact(c, regionsDatabase())
	layer := filepath.Join(filepath.Dir(path), schemaartifact.ManagedDataFileName)
	c.Assert(os.Remove(layer), qt.IsNil)
	c.Assert(os.WriteFile(layer, []byte(
		`{"sets":[{"table":"countries","keys":["code"],"columns":["code"],"rows":[]}]}`+"\n",
	), 0o600), qt.IsNil)

	_, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{path}})

	c.Assert(err, qt.ErrorMatches, `.*carries no rows for it.*`)
}
