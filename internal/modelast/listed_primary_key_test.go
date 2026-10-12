package modelast_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

// TestCreateTable_HoldsAKeyDeclaredTwiceOnce declares a table's primary key on
// its columns and again as a PRIMARY KEY entry of the constraint list. The
// CREATE TABLE holds the key once, as the entry, because a server refuses a
// statement that holds two primary keys. A planner adds the entry after the
// CREATE it builds, and a render writes it inside. A key of the list over other
// columns is a second key and is left for the server to refuse.
func TestCreateTable_HoldsAKeyDeclaredTwiceOnce(t *testing.T) {
	tests := []struct {
		name         string
		table        schemamodel.Table
		primary      []string
		listed       []string
		wantPlanned  []string
		wantRendered []string
	}{
		{
			name:         "a column key the list declares again",
			primary:      []string{"id"},
			listed:       []string{"id"},
			wantPlanned:  nil,
			wantRendered: []string{"constraint PRIMARY (id)"},
		},
		{
			name:         "a composite key the list declares again",
			table:        schemamodel.Table{PrimaryKey: []string{"id", "a"}},
			primary:      []string{"id", "a"},
			listed:       []string{"id", "a"},
			wantPlanned:  nil,
			wantRendered: []string{"constraint PRIMARY (id, a)"},
		},
		{
			name:         "a composite key the list declares in another order",
			table:        schemamodel.Table{PrimaryKey: []string{"id", "a"}},
			primary:      []string{"id", "a"},
			listed:       []string{"a", "id"},
			wantPlanned:  []string{"constraint  (id, a)"},
			wantRendered: []string{"constraint  (id, a)", "constraint PRIMARY (a, id)"},
		},
		{
			name:         "a list key over another column",
			primary:      []string{"id"},
			listed:       []string{"a"},
			wantPlanned:  []string{"column id"},
			wantRendered: []string{"column id", "constraint PRIMARY (a)"},
		},
		{
			name:         "a column key alone",
			primary:      []string{"id"},
			wantPlanned:  []string{"column id"},
			wantRendered: []string{"column id"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := test.table
			table.StructName, table.Name = "HK", "hk"
			database := schemamodel.Database{
				Tables: []schemamodel.Table{table},
				Fields: []schemamodel.Field{
					{StructName: "HK", Name: "id", Type: "INT", Primary: slices.Contains(test.primary, "id")},
					{StructName: "HK", Name: "a", Type: "INT", Primary: slices.Contains(test.primary, "a")},
				},
				Constraints: listedPrimaryKey(test.listed),
			}

			planned := modelast.FromTableWithConstraints(table, database.Fields, nil, platform.MySQL, database.Constraints)

			c.Assert(primaryKeysOf(planned), qt.DeepEquals, test.wantPlanned)
			c.Assert(primaryKeysOf(renderedTable(c, database)), qt.DeepEquals, test.wantRendered)
		})
	}
}

// listedPrimaryKey is the constraint list holding one PRIMARY KEY of hk over
// columns, or none when columns is empty.
func listedPrimaryKey(columns []string) []schemamodel.Constraint {
	if len(columns) == 0 {
		return nil
	}
	return []schemamodel.Constraint{{StructName: "HK", Table: "hk", Name: "PRIMARY", Type: "PRIMARY KEY", Columns: columns}}
}

// renderedTable is the CREATE TABLE a render of database builds for MySQL.
func renderedTable(c *qt.C, database schemamodel.Database) *ast.CreateTableNode {
	c.Helper()
	var created []*ast.CreateTableNode
	c.Assert(modelast.WalkDatabase(database, platform.MySQL, func(node ast.Node) error {
		if table, ok := node.(*ast.CreateTableNode); ok {
			created = append(created, table)
		}
		return nil
	}, modelast.Lowering{Context: context.Background()}), qt.IsNil)
	c.Assert(created, qt.HasLen, 1)
	return created[0]
}

// primaryKeysOf lists the primary keys node holds: each column marked primary,
// as "column <name>", and each PRIMARY KEY constraint, as "constraint <name>
// (<columns>)".
func primaryKeysOf(node *ast.CreateTableNode) []string {
	var keys []string
	for _, column := range node.Columns {
		if column.Primary {
			keys = append(keys, "column "+column.Name)
		}
	}
	for _, constraint := range node.Constraints {
		if constraint.Type == ast.PrimaryKeyConstraint {
			keys = append(keys, fmt.Sprintf("constraint %s (%s)", constraint.Name, strings.Join(constraint.Columns, ", ")))
		}
	}
	return keys
}
