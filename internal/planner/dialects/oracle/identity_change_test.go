package oracle_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	oracleplanner "ptah.run/internal/planner/dialects/oracle"
)

// TestPlanner_OracleCarriesTheIdentityAColumnHolds plans a change to the
// identity of column ID and reads the MODIFY it writes. The operation carries
// the generation the column holds, which the renderer reads to tell a changed
// generation from an identity given to a column that has none.
func TestPlanner_OracleCarriesTheIdentityAColumnHolds(t *testing.T) {
	tests := []struct {
		name, change, previous string
	}{
		{name: "a changed generation", change: "BY_DEFAULT -> ALWAYS", previous: "BY_DEFAULT"},
		{name: "a dropped identity", change: "ALWAYS -> ", previous: "ALWAYS"},
		{name: "an identity given to a plain column", change: " -> BY_DEFAULT", previous: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName: "T",
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName: "ID",
					Changes:    map[string]string{"identity": test.change},
					Desired:    schemamodel.Field{Name: "ID", StructName: "T", Type: "NUMBER(10)", IdentityGeneration: "ALWAYS"},
				}},
			}}}

			nodes, err := oracleplanner.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

			c.Assert(err, qt.IsNil)
			modifies := modifyColumnOperations(nodes)
			c.Assert(modifies, qt.HasLen, 1)
			c.Assert(modifies[0].PreviousIdentity, qt.Equals, test.previous)
			c.Assert(modifies[0].Changed, qt.Equals, ast.ColumnProperties{Identity: true})
		})
	}
}
