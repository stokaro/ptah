package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
)

// notNullNameChange is a table whose column's NOT NULL moves from current to desired.
func notNullNameChange(current, desired string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "t",
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName:                  "a",
			Changes:                     make(map[string]string),
			NotNullConstraintNameChange: &difftypes.NotNullConstraintNameChange{Current: current, Desired: desired},
		}},
	}}}
}

// Each row is the rollback of a forward rename of a NOT NULL. Without the name it held
// the constraint stays under the forward name, and the rolled-back database compares
// as different from the one the migrations build. A transition from no name has nothing
// to give back.
func TestPlanBidirectionalSchemaDiff_NotNullNameRollsBack(t *testing.T) {
	tests := []struct {
		name    string
		current string
		desired string
		want    string
	}{
		{name: "a renamed constraint", current: "nn_old", desired: "nn_new", want: "-- Add/modify columns for table: t\n" + `ALTER TABLE "t" RENAME CONSTRAINT "nn_new" TO "nn_old";` + "\n\n"},
		{name: "a constraint the database reported no name for", desired: "nn_new", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.Postgres18()
			diff := notNullNameChange(test.current, test.desired)

			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(),
				generator.BidirectionalSchemaPlanOptions{
					Runtime:       must.Must(builtin.New()),
					Diff:          diff,
					DesiredSchema: &schemamodel.Database{},
					CurrentSchema: &catalog.Database{},
					Dialect:       platform.Postgres,
					Capabilities:  caps,
				})

			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Reverse.Nodes...)), qt.Equals, test.want)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].NotNullConstraintNameChange.Desired, qt.Equals, test.desired,
				qt.Commentf("the reversal must not write through to the forward diff"))
		})
	}
}
