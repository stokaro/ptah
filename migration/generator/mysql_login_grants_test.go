package generator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
)

// A read names the database while a declaration may leave it out. Rolling
// back a new privilege must not take the table's preexisting grant option,
// including when only a column privilege keeps it. A grant on another
// database is a different object and cannot preserve the option here.
func TestPlanBidirectionalSchemaDiff_MySQLFamilyKeepsExistingGrantOption(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range []struct {
			name, objectType, heldObject, column, sqlTarget string
			revokeOption                                    bool
		}{
			{name: "table grant", objectType: "TABLE", heldObject: "app.items", sqlTarget: "`items`"},
			{name: "column grant", objectType: "TABLE", heldObject: "app.items", column: "value", sqlTarget: "`items`"},
			{name: "another database", objectType: "TABLE", heldObject: "archive.items", sqlTarget: "`items`", revokeOption: true},
			{name: "a database name with a dot", objectType: "SCHEMA", heldObject: "app.items", sqlTarget: "`items`.*", revokeOption: true},
		} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				semantics := identifier.ForDialect(dialect)
				semantics.DefaultSchema = "app"
				added := difftypes.GrantRef{Role: "app_user", Privilege: "INSERT", ObjectType: test.objectType, ObjectName: "items", WithOption: true}
				held := difftypes.GrantRef{Role: "app_user", Privilege: "SELECT", ObjectType: test.objectType,
					ObjectName: test.heldObject, Column: test.column, WithOption: true}
				diff := &difftypes.SchemaDiff{
					IdentifierSemantics: &semantics,
					GrantsAdded:         []difftypes.GrantRef{added},
					CurrentGrants:       []difftypes.GrantRef{held},
				}
				runtime := must.Must(builtin.New())

				plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
					Runtime: runtime, Diff: diff, DesiredSchema: &schemamodel.Database{}, CurrentSchema: &catalog.Database{},
					Dialect: dialect, Capabilities: capability.MySQL84(),
				})

				c.Assert(err, qt.IsNil)
				up, err := builtin.RenderSQL(dialect, plan.Forward.Nodes...)
				c.Assert(err, qt.IsNil)
				down, err := builtin.RenderSQL(dialect, plan.Reverse.Nodes...)
				c.Assert(err, qt.IsNil)
				c.Assert(up, qt.Contains, "GRANT INSERT ON "+test.sqlTarget+" TO `app_user` WITH GRANT OPTION")
				c.Assert(down, qt.Contains, "REVOKE INSERT ON "+test.sqlTarget+" FROM `app_user`")
				c.Assert(down, qt.Not(qt.Contains), "REVOKE SELECT")
				c.Assert(strings.Contains(down, "REVOKE GRANT OPTION"), qt.Equals, test.revokeOption)
			})
		}
	}
}
