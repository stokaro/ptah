package atlashclrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRenderSQLiteKeyColumnNullabilityFollowsTheTableShape pins what this
// document may claim about a SQLite key column.
//
// The rule is SQLite's, not SQL's: a key column is NOT NULL unless it is the
// rowid alias, and the leniency that lets a key column hold NULL at all survives
// only on an ordinary rowid table that is not STRICT (stokaro/ptah#1235).
// Writing `null = true` for the key of a STRICT or WITHOUT ROWID table describes
// a database SQLite will not build, and the document then disagrees with the
// `{{ json . }}` rendering of the very database it was applied to.
//
// The rows run in both directions. Drop the table's shape from the decision and
// the STRICT and WITHOUT ROWID rows write `null = true`; force NOT NULL onto
// every SQLite key column and the two rowid rows lose theirs.
func TestRenderSQLiteKeyColumnNullabilityFollowsTheTableShape(t *testing.T) {
	tests := []struct {
		name        string
		table       goschema.Table
		fields      []goschema.Field
		wantNullSet bool
	}{
		{
			name:        "rowid table keeps its nullable key column",
			table:       goschema.Table{StructName: "T", Name: "users"},
			fields:      []goschema.Field{{StructName: "T", Name: "id", Type: "text", Primary: true, Nullable: true}},
			wantNullSet: true,
		},
		{
			name:        "without rowid table writes the key column NOT NULL",
			table:       goschema.Table{StructName: "T", Name: "users", WithoutRowID: true},
			fields:      []goschema.Field{{StructName: "T", Name: "id", Type: "text", Primary: true, Nullable: true}},
			wantNullSet: false,
		},
		{
			name:        "strict table writes the key column NOT NULL",
			table:       goschema.Table{StructName: "T", Name: "users", Strict: true},
			fields:      []goschema.Field{{StructName: "T", Name: "id", Type: "text", Primary: true, Nullable: true}},
			wantNullSet: false,
		},
		{
			name:        "strict rowid alias keeps its nullable key column",
			table:       goschema.Table{StructName: "T", Name: "users", Strict: true},
			fields:      []goschema.Field{{StructName: "T", Name: "id", Type: "integer", Primary: true, Nullable: true}},
			wantNullSet: true,
		},
		{
			name: "strict table level composite key writes both columns NOT NULL",
			table: goschema.Table{
				StructName: "T",
				Name:       "memberships",
				Strict:     true,
				PrimaryKey: []string{"team", "member"},
			},
			fields: []goschema.Field{
				{StructName: "T", Name: "team", Type: "text", Nullable: true},
				{StructName: "T", Name: "member", Type: "text", Nullable: true},
			},
			wantNullSet: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &goschema.Database{
				Tables: []goschema.Table{test.table},
				Fields: test.fields,
			}
			goschema.Finalize(db)

			rendered, err := atlashclrender.RenderInspected(db, "sqlite", "main")
			c.Assert(err, qt.IsNil)

			// Every fixture declares exactly its key columns as nullable, and
			// the attribute is written only for a nullable column -- NOT NULL
			// is the absence of it -- so one answer covers the whole table.
			hcl := string(rendered.Data)
			c.Assert(strings.Contains(hcl, "null = true"), qt.Equals, test.wantNullSet,
				qt.Commentf("rendered HCL:\n%s", hcl))
		})
	}
}
