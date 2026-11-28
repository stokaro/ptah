package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// A database is declared by its schema block, or by a table or a sequence in
// it, once, in declaration order; a block keeps its attributes.
func TestServerDatabases_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		desired *schemamodel.Database
		want    []schemamodel.Schema
	}{
		{name: "no desired schema"},
		{
			name:    "schema blocks",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app", Charset: "utf8mb4"}, {Name: "audit"}}},
			want:    []schemamodel.Schema{{Name: "app", Charset: "utf8mb4"}, {Name: "audit"}},
		},
		{
			name: "a table and a sequence in databases no block declares",
			desired: &schemamodel.Database{
				Schemas:   []schemamodel.Schema{{Name: "app"}},
				Tables:    []schemamodel.Table{{Schema: "db", Name: "t"}, {Schema: "app", Name: "u"}, {Schema: "db", Name: "v"}},
				Sequences: []schemamodel.Sequence{{Schema: "seqs", Name: "s"}},
			},
			want: []schemamodel.Schema{{Name: "app"}, {Name: "db"}, {Name: "seqs"}},
		},
		{
			name:    "a table that names no database",
			desired: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "t"}, {Schema: " ", Name: "u"}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprep.ServerDatabases(test.desired), qt.DeepEquals, test.want)
		})
	}
}
