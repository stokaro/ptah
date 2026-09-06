package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemafile"
)

// TestLoadSources_CarriesTheFamiliesLoadCarries is the difference stokaro/ptah#1999
// was, stated as a test.
//
// Every CLI verb reaches a schema file through [schemafile.LoadSources], and
// every unit test reached it through [schemafile.Load]. The first merges each
// parsed file into one description and the second returns it directly, so a
// family missing from the merge was invisible to the whole suite and visible on
// a live server as `Schema is synced, no changes to be made` for a document
// declaring four objects.
//
// One source is enough to see it: the merge runs for one file as well as for
// five. Loading the SAME document both ways and comparing is what makes the
// claim about the merge rather than about the parser.
func TestLoadSources_CarriesTheFamiliesLoadCarries(t *testing.T) {
	c := qt.New(t)
	path := writeSQLServerDocument(c)

	direct, err := schemafile.Load(path, schemafile.Options{Dialect: platform.SQLServer})
	c.Assert(err, qt.IsNil)
	merged, err := schemafile.LoadSources(
		[]schemafile.Source{{URL: path}},
		schemafile.Options{Dialect: platform.SQLServer},
	)
	c.Assert(err, qt.IsNil)

	// Non-vacuity: the document really does declare one of each, so two empty
	// descriptions cannot pass as agreement.
	c.Assert(direct.Synonyms, qt.HasLen, 1)
	c.Assert(direct.ExtendedProperties, qt.HasLen, 1)

	c.Assert(merged.Synonyms, qt.DeepEquals, direct.Synonyms)
	c.Assert(merged.ExtendedProperties, qt.DeepEquals, direct.ExtendedProperties)
}

// TestLoadSources_MergesTheFamiliesAcrossFiles is the multi-file half, and the
// one an operator with a directory of schema files meets.
func TestLoadSources_MergesTheFamiliesAcrossFiles(t *testing.T) {
	c := qt.New(t)

	merged, err := schemafile.LoadSources(
		[]schemafile.Source{{URL: writeSQLServerDocument(c)}, {URL: writeSecondSynonymDocument(c)}},
		schemafile.Options{Dialect: platform.SQLServer},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(synonymNames(merged), qt.DeepEquals, []string{"s_gauge", "s_other"})
}

func writeSQLServerDocument(c *qt.C) string {
	c.Helper()
	const document = `
schema "dbo" {
}

table "gauge" {
  schema = schema.dbo
  column "id" {
    type = INT
  }
  primary_key {
    columns = [column.id]
  }
}

synonym "s_gauge" {
  schema = schema.dbo
  target = "other.dbo.gauge"
}

extended_property "ptah_flag" {
  schema = schema.dbo
  table  = "gauge"
  value  = "on"
}
`
	return writeDocument(c, "schema.hcl", document)
}

func writeSecondSynonymDocument(c *qt.C) string {
	c.Helper()
	const document = `
synonym "s_other" {
  schema = schema.dbo
  target = "other.dbo.other"
}
`
	return writeDocument(c, "second.hcl", document)
}

func writeDocument(c *qt.C, name, document string) string {
	c.Helper()
	path := filepath.Join(c.TB.(*testing.T).TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

func synonymNames(database *schemamodel.Database) []string {
	names := make([]string, 0, len(database.Synonyms))
	for _, synonym := range database.Synonyms {
		names = append(names, synonym.Name)
	}
	return names
}
