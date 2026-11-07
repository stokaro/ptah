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

// notNullRollback renders the rollback of diff, whose prior database holds the columns of
// t the table lists.
func notNullRollback(c *qt.C, diff *difftypes.SchemaDiff, current []catalog.Column) string {
	c.Helper()
	caps := capability.Postgres18()
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(),
		generator.BidirectionalSchemaPlanOptions{
			Runtime:       must.Must(builtin.New()),
			Diff:          diff,
			DesiredSchema: &schemamodel.Database{},
			CurrentSchema: &catalog.Database{Tables: []catalog.Table{{Name: "t", Schema: "public", Columns: current}}},
			Dialect:       platform.Postgres,
			Capabilities:  caps,
		})
	c.Assert(err, qt.IsNil)
	return must.Must(builtin.RenderSQLWithCapabilities(platform.Postgres, caps, plan.Reverse.Nodes...))
}

// A rollback gives a column the forward plan made nullable the name it held. The name
// the server gives needs no statement of its own, and SET NOT NULL lands on it. That name
// does not carry the table's schema.
func TestPlanBidirectionalSchemaDiff_AColumnMadeNullableRollsBackToItsName(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		released string
		want     string
	}{
		{name: "a chosen name", table: "t", released: "nn", want: `ALTER TABLE "t" ADD CONSTRAINT "nn" NOT NULL "a";`},
		{name: "the name the server gave", table: "t", released: "t_a_not_null", want: `ALTER TABLE "t" ALTER COLUMN "a" SET NOT NULL;`},
		{name: "the name the server gave, in a schema", table: "public.t", released: "t_a_not_null", want: `ALTER TABLE "public"."t" ALTER COLUMN "a" SET NOT NULL;`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName: test.table,
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName:                    "a",
					Changes:                       map[string]string{"nullable": "false -> true"},
					ReleasedNotNullConstraintName: test.released,
				}},
			}}}

			rollback := notNullRollback(c, diff, []catalog.Column{{Name: "a", DataType: "text", IsNullable: "NO", NotNullConstraintName: test.released}})

			c.Assert(rollback, qt.Contains, test.want)
		})
	}
}

// A rollback that adds a column under a name holds the column the forward plan made NOT
// NULL as the holder of that name, until the rollback makes it nullable: the name the
// plan stated, the one the desired side holds, or the one the server gave.
func TestPlanBidirectionalSchemaDiff_AColumnMadeNotNullHoldsItsNameInTheRollback(t *testing.T) {
	tests := []struct {
		name    string
		change  *difftypes.NotNullConstraintNameChange
		desired string
		held    string
	}{
		{name: "the name the plan stated", change: &difftypes.NotNullConstraintNameChange{Desired: "nn"}, held: "nn"},
		{name: "the name the desired side holds", desired: "kept", held: "kept"},
		{name: "the name the server gave", held: "t_a_not_null"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:      "t",
				ColumnsRemoved: []schemamodel.Field{{Name: "b", Type: "text", NotNullConstraintName: test.held}},
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName:                  "a",
					Changes:                     map[string]string{"nullable": "true -> false"},
					NotNullConstraintNameChange: test.change,
					Desired:                     schemamodel.Field{Name: "a", Type: "text", NotNullConstraintName: test.desired},
				}},
			}}}

			rollback := notNullRollback(c, diff, []catalog.Column{{Name: "a", DataType: "text", IsNullable: "YES"}})

			c.Assert(rollback, qt.Contains, `RENAME CONSTRAINT "`+test.held+`" TO "`+test.held+`__ptah_old"`)
		})
	}
}

// A column the forward plan changed in another way holds nothing it did not hold, so the
// rollback moves no name for it.
func TestPlanBidirectionalSchemaDiff_AColumnThatStaysNullableHoldsNoName(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:      "t",
		ColumnsRemoved: []schemamodel.Field{{Name: "b", Type: "text", NotNullConstraintName: "t_a_not_null"}},
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "a",
			Changes:    map[string]string{"type": "varchar -> text"},
			Desired:    schemamodel.Field{Name: "a", Type: "text", Nullable: true},
		}},
	}}}

	rollback := notNullRollback(c, diff, []catalog.Column{{Name: "a", DataType: "text", IsNullable: "YES"}})

	c.Assert(rollback, qt.Not(qt.Contains), "RENAME CONSTRAINT")
}
