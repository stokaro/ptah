package schemafile_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/rundiag"
	"ptah.run/internal/schemafile"
)

// A load that names no writer for its readers' warnings raises them for the
// run its context carries: the HCL names the tolerance drops, and a
// constraint a SQL file drops and adds again differently. A command that
// loads a file only to check it, as `schema plan` checks a saved plan, names
// none.
func TestLoad_RaisesReaderWarningsForTheRun(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		source   string
		dialect  string
		category *rundiag.Category
		count    int
	}{
		{name: "the HCL names the tolerance drops", file: "schema.hcl", source: unknownHCLNamesSource, category: rundiag.IgnoredSchemaFileSettings, count: 2},
		{name: "a constraint declared again", file: "schema.sql", source: redeclaredKeySource, dialect: "postgres", category: rundiag.RedeclaredConstraints, count: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), test.file)
			c.Assert(os.WriteFile(path, []byte(test.source), 0o600), qt.IsNil)
			var stream bytes.Buffer
			report := rundiag.New(rundiag.Options{Stream: &stream})
			runtime := builtintest.Runtime()

			_, err := schemafile.Load(rundiag.With(t.Context(), report), "file://"+path, schemafile.Options{
				Dialect: test.dialect, YAML: runtime.YAML(), SQL: runtime.SQL(), HCL: runtime.HCL(), IgnoreUnknownHCLNames: true,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Count(test.category), qt.Equals, test.count, qt.Commentf("stream:\n%s", stream.String()))
		})
	}
}

// redeclaredKeySource drops a foreign key and adds it again with another
// ON DELETE.
const redeclaredKeySource = "CREATE TABLE owners (id bigint PRIMARY KEY);\n" +
	"CREATE TABLE items (id bigint PRIMARY KEY, owner_id bigint,\n" +
	"  CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id));\n" +
	"ALTER TABLE items DROP CONSTRAINT items_owner_fk;\n" +
	"ALTER TABLE items ADD CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id) ON DELETE CASCADE;\n"
