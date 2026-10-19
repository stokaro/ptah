package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
	"ptah.run/engine"
	"ptah.run/internal/schemafile"
)

// feedLimits is what an engine no build ships states about its coverage
// kinds: HCL and DBML cannot spell a feed, HCL cannot spell a pulse, and YAML
// and SQL cannot spell a tide.
var feedLimits = []coverage.FormatLimit{
	{Format: coverage.FormatHCL, Kinds: []coverage.Kind{"feed", "pulse"}},
	{Format: coverage.FormatDBML, Kinds: []coverage.Kind{"feed"}},
	{Format: coverage.FormatYAML, Kinds: []coverage.Kind{"tide"}},
	{Format: coverage.FormatSQL, Kinds: []coverage.Kind{"tide"}},
}

// feedOwner returns the vocabulary of a runtime of that engine alone, which
// registers the kinds feed, pulse and tide and states limits about them.
func feedOwner(limits []coverage.FormatLimit) coverage.Vocabulary {
	return must.Must(engine.New(engine.Provider{
		ID: "example.org/feeds", CoverageKinds: []coverage.Kind{"feed", "pulse", "tide"}, CoverageFormatLimits: limits,
	})).CoverageVocabulary()
}

// ownerLoadOptions reads every format with the formats' own syntax and the
// vocabulary of the given owner.
func ownerLoadOptions(vocabulary coverage.Vocabulary) schemafile.Options {
	return schemafile.Options{YAML: yamlext.None(), SQL: sqlext.None(), HCL: hclext.None(), CoverageVocabulary: vocabulary}
}

// writeSchemaFiles writes files into a new directory and returns it.
func writeSchemaFiles(c *qt.C, files map[string]string) string {
	dir := c.TempDir()
	for name, contents := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600), qt.IsNil)
	}
	return dir
}

// ownerLoads are the documents each test loads: one per format, a second
// YAML extension, and a directory of SQL files, whose entries the loader reads
// through a path of its own.
var ownerLoads = []struct {
	name  string
	files map[string]string
	path  string
}{
	{name: "HCL", files: map[string]string{"schema.hcl": "schema \"main\" {\n}\n"}, path: "schema.hcl"},
	{name: "DBML", files: map[string]string{"schema.dbml": "Table users {\n  id integer [pk]\n}\n"}, path: "schema.dbml"},
	{name: "YAML", files: map[string]string{"schema.yaml": "tables:\n  users:\n    fields:\n      id:\n        type: INTEGER\n"}, path: "schema.yaml"},
	{name: "YML", files: map[string]string{"schema.yml": "tables:\n  users:\n    fields:\n      id:\n        type: INTEGER\n"}, path: "schema.yml"},
	{name: "SQL", files: map[string]string{"schema.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n"}, path: "schema.sql"},
	{name: "SQL directory", files: map[string]string{"001.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n"}, path: "."},
}

// An engine that states which of its coverage kinds a format cannot spell
// has each one recorded by a document in that format, without an edit to the
// loader: the statement travels in the vocabulary the load is given.
func TestAnOwnerFormatLimitIsRecordedByTheFormat(t *testing.T) {
	want := map[string][]coverage.Object{
		"HCL": unsupportedRecords("feed", "pulse"),
		"DBML": unsupportedRecords(coverage.Composite, coverage.Domain, coverage.Extension, "feed",
			coverage.Policy, coverage.Range, coverage.Role, coverage.Sequence),
		"YAML":          unsupportedRecords(coverage.Composite, coverage.Domain, coverage.Range, coverage.Sequence, "tide"),
		"YML":           unsupportedRecords(coverage.Composite, coverage.Domain, coverage.Range, coverage.Sequence, "tide"),
		"SQL":           unsupportedRecords("tide"),
		"SQL directory": unsupportedRecords("tide"),
	}
	for _, load := range ownerLoads {
		t.Run(load.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaFiles(c, load.files)

			database, err := schemafile.LoadPath(filepath.Join(dir, load.path), ownerLoadOptions(feedOwner(feedLimits)))

			c.Assert(err, qt.IsNil)
			c.Assert(database.NotDescribed.Objects, qt.DeepEquals, want[load.name])
		})
	}
}

// The control: the same engine before it states a limit registers the same
// kinds, and no format records one of them. What each format records then is
// its own common limits.
func TestAnOwnerKindIsNotRecordedBeforeItsOwnerStatesALimit(t *testing.T) {
	want := map[string][]coverage.Object{
		"HCL": nil,
		"DBML": unsupportedRecords(coverage.Composite, coverage.Domain, coverage.Extension,
			coverage.Policy, coverage.Range, coverage.Role, coverage.Sequence),
		"YAML":          unsupportedRecords(coverage.Composite, coverage.Domain, coverage.Range, coverage.Sequence),
		"YML":           unsupportedRecords(coverage.Composite, coverage.Domain, coverage.Range, coverage.Sequence),
		"SQL":           nil,
		"SQL directory": nil,
	}
	for _, load := range ownerLoads {
		t.Run(load.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaFiles(c, load.files)

			database, err := schemafile.LoadPath(filepath.Join(dir, load.path), ownerLoadOptions(feedOwner(nil)))

			c.Assert(err, qt.IsNil)
			c.Assert(database.NotDescribed.Objects, qt.DeepEquals, want[load.name])
		})
	}
}

// LoadSources reads a SQL source through an arm of its own, which keeps the
// statements of an import tree, so it records the owner's SQL limit itself.
// The second load is the control: the same engine before it states a limit.
func TestAnOwnerFormatLimitIsRecordedForASQLSource(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaFiles(c, map[string]string{"schema.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n"})
	sources := []schemafile.Source{{URL: filepath.Join(dir, "schema.sql")}}

	stated, err := schemafile.LoadSources(sources, ownerLoadOptions(feedOwner(feedLimits)))
	c.Assert(err, qt.IsNil)
	c.Assert(stated.NotDescribed.Objects, qt.DeepEquals, unsupportedRecords("tide"))

	unstated, err := schemafile.LoadSources(sources, ownerLoadOptions(feedOwner(nil)))
	c.Assert(err, qt.IsNil)
	c.Assert(unstated.NotDescribed.Objects, qt.IsNil)
}
