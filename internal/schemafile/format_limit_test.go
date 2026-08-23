package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/internal/schemafile"
)

// Only `.sql` has CREATE VIRTUAL TABLE, so silence about a live SQLite virtual
// table is intent there and is not intent in HCL, where the document could not
// have named it (stokaro/ptah#1028).
//
// The record now carries why. "Unsupported, derived from another fact Ptah
// holds" is the document's FORMAT speaking, not a read that failed and not a
// selection the user wrote, and a user told only that virtual tables were "not
// described" cannot tell those three apart (stokaro/ptah#1346).
func TestAFormatThatCannotExpressAKindSaysSoAndSaysWhy(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		contents string
		want     []coverage.Object
	}{
		{
			name:     "HCL cannot name a virtual table",
			file:     "schema.hcl",
			contents: "schema \"main\" {\n}\n",
			want: []coverage.Object{{
				Kind:       coverage.VirtualTable,
				Reason:     coverage.Unsupported,
				Provenance: coverage.DerivedFromFact,
			}},
		},
		{
			// The control. A `.sql` document CAN name one, so it claims full
			// authority and carries no record -- without this row a loader that
			// recorded the limit for every format would pass the row above.
			name:     "SQL can",
			file:     "schema.sql",
			contents: "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
			want:     nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), test.file)
			c.Assert(os.WriteFile(path, []byte(test.contents), 0o600), qt.IsNil)

			database, err := schemafile.LoadPath(path, schemafile.Options{})

			c.Assert(err, qt.IsNil)
			c.Assert(database.NotDescribed.Objects, qt.DeepEquals, test.want)
		})
	}
}
