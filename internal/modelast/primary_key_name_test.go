package modelast_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

// TestPrimaryKeyName_DecidesTheFormAndCarriesTheName pins both halves of
// stokaro/ptah#2180.
//
// An inline `PRIMARY KEY` has nowhere to carry a name, so a named single-column
// key written that way reached PostgreSQL 17 as `t_named_pkey` and `--dry-run`
// reported the schema synced afterwards. The composite case was reported as
// unaffected and was not: it kept the table-level form and still arrived as
// `t_comp_pkey`, because nothing carried the name at all.
func TestPrimaryKeyName_DecidesTheFormAndCarriesTheName(t *testing.T) {
	tests := []struct {
		name  string
		table schemamodel.Table
		// want is where the key ended up: "(inline)" when it stayed on the
		// column, "(unnamed)" when it became a table-level constraint with no
		// name, and the name itself otherwise. One value rather than two
		// fields, so a row states an outcome instead of choosing an assertion.
		want string
	}{
		{
			name: "a named single-column key takes the table-level form",
			table: schemamodel.Table{
				Name: "t", StructName: "T",
				PrimaryKey: []string{"b"}, PrimaryKeyName: "c_pk",
			},
			want: "c_pk",
		},
		{
			name: "a named composite key keeps its name",
			table: schemamodel.Table{
				Name: "t", StructName: "T",
				PrimaryKey: []string{"a", "b"}, PrimaryKeyName: "c_comp_pk",
			},
			want: "c_comp_pk",
		},
		{
			// THE control. An unnamed single-column key must stay inline, or
			// every table in every document grows a constraint clause and the
			// diff against a database that has none never settles.
			name: "an unnamed single-column key stays inline",
			table: schemamodel.Table{
				Name: "t", StructName: "T",
				PrimaryKey: []string{"b"},
			},
			want: "(inline)",
		},
		{
			// The other control: an unnamed composite key was already
			// table-level and must not gain a name.
			name: "an unnamed composite key keeps the table-level form and no name",
			table: schemamodel.Table{
				Name: "t", StructName: "T",
				PrimaryKey: []string{"a", "b"},
			},
			want: "(unnamed)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// The fields carry Primary the way a declaration delivers them:
			// every column the key names is marked, and the conversion decides
			// whether that mark survives or becomes a table-level constraint.
			fields := []schemamodel.Field{
				{StructName: "T", Name: "a", Type: "INTEGER", Primary: slices.Contains(test.table.PrimaryKey, "a")},
				{StructName: "T", Name: "b", Type: "INTEGER", Primary: slices.Contains(test.table.PrimaryKey, "b")},
			}

			node := modelast.FromTable(test.table, fields, nil, "")

			c.Assert(primaryKeyPlacement(node), qt.Equals, test.want)
			// A key rendered at the table level must not ALSO be marked on the
			// column: the server would be handed two primary keys. The inline
			// row is where that mark belongs, and it is the only one that has
			// it.
			c.Assert(anyColumnIsPrimary(node), qt.Equals, test.want == "(inline)")
		})
	}
}

// primaryKeyPlacement spells where the key ended up, so a row states one
// outcome rather than picking which assertion to make.
func primaryKeyPlacement(node *ast.CreateTableNode) string {
	for _, constraint := range node.Constraints {
		if constraint.Type != ast.PrimaryKeyConstraint {
			continue
		}
		if constraint.Name == "" {
			return "(unnamed)"
		}
		return constraint.Name
	}
	return "(inline)"
}

// anyColumnIsPrimary reports whether the key was left on a column instead.
func anyColumnIsPrimary(node *ast.CreateTableNode) bool {
	for _, column := range node.Columns {
		if column.Primary {
			return true
		}
	}
	return false
}
