package planner_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestEveryPlannerButYDBRefusesSerialSequenceChanges drives a hand-built diff
// that changes the start of a Serial's sequence through every registered
// planner but YDB's. The comparison records the change only on a target with
// capability.SerialSequenceOptions, so only such a diff reaches them, and a
// planner that read nothing of it would plan nothing and report the column
// synced.
func TestEveryPlannerButYDBRefusesSerialSequenceChanges(t *testing.T) {
	c := qt.New(t)
	dialects, err := planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	// The planners Ptah ships: a name NormalizeDialect knows. Another test of
	// this package registers planners of its own under names it does not, and
	// those plan whatever their test asks.
	dialects = slices.DeleteFunc(dialects, func(dialect string) bool {
		return dialect == platform.YDB || platform.NormalizeDialect(dialect) != dialect
	})
	c.Assert(len(dialects) >= 8, qt.IsTrue, qt.Commentf("registered dialects: %v", dialects))

	id := schemamodel.Field{StructName: "User", Name: "id", Type: "BIGSERIAL", Primary: true, AutoInc: true,
		IdentityGeneration: "BY_DEFAULT", IdentityStart: "100"}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "users",
		Desired: difftypes.TableDeclaration{
			Table: schemamodel.Table{StructName: "User", Name: "users"}, Fields: []schemamodel.Field{id},
		},
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "id", Changes: map[string]string{"identity_start": "1 -> 100"}, Desired: id,
		}},
	}}}
	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(diff, dialect)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*the diff changes identity_start of column "id" of table "users".*`)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
