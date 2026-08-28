package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestPlanner_OracleUserTypesPlanWhateverSchemaTheyName is the control for a
// lookup this planner no longer makes.
//
// It indexed the desired schema by the object's NAME and searched it by the
// object's QUALIFIED name -- the same string only for a declaration that names
// no schema. A domain or composite declaring one produced no node at all, and
// the plan reported success: the type was never created, and every column typed
// by it failed at apply time for a reason nothing in the plan explained.
//
// Measured before the operand moved onto the change: `zip` planned one
// statement and `app.zip` planned none.
//
// The unqualified rows are the control. Without them a planner that ignored the
// schema entirely would satisfy the qualified rows, and the property here is
// that BOTH spellings plan.
func TestPlanner_OracleUserTypesPlanWhateverSchemaTheyName(t *testing.T) {
	tests := []struct {
		name   string
		diff   *difftypes.SchemaDiff
		wantIn string
	}{
		{
			name: "a domain naming no schema",
			diff: &difftypes.SchemaDiff{DomainsAdded: difftypes.DomainChanges{
				{StructName: "Zip", Name: "zip", BaseType: "VARCHAR2(10)"},
			}},
			wantIn: "zip",
		},
		{
			name: "a domain naming one",
			diff: &difftypes.SchemaDiff{DomainsAdded: difftypes.DomainChanges{
				{StructName: "Zip", Name: "zip", Schema: "app", BaseType: "VARCHAR2(10)"},
			}},
			wantIn: "app",
		},
		{
			name: "a composite naming no schema",
			diff: &difftypes.SchemaDiff{CompositeTypesAdded: difftypes.CompositeTypeChanges{
				{StructName: "Addr", Name: "addr", Fields: []schemamodel.CompositeField{
					{Name: "line1", Type: "VARCHAR2(80)"},
				}},
			}},
			wantIn: "addr",
		},
		{
			name: "a composite naming one",
			diff: &difftypes.SchemaDiff{CompositeTypesAdded: difftypes.CompositeTypeChanges{
				{StructName: "Addr", Name: "addr", Schema: "app", Fields: []schemamodel.CompositeField{
					{Name: "line1", Type: "VARCHAR2(80)"},
				}},
			}},
			wantIn: "app",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := mysql.NewForDialect(platform.Oracle, capability.ForDialect(platform.Oracle)).
				GenerateMigrationAST(test.diff, &schemamodel.Database{})
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.Not(qt.HasLen), 0,
				qt.Commentf("the declaration travels with the change; nothing is left to look up"))

			sql, err := renderer.RenderSQL(platform.Oracle, nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.wantIn, qt.Commentf("plan:\n%s", sql))
		})
	}
}
