package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// itemsWithIncrement is table items whose BIGSERIAL key steps by increment.
func itemsWithIncrement(increment string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "BIGSERIAL", Primary: true, AutoInc: true,
			IdentityGeneration: "BY_DEFAULT", IdentityIncrement: increment}},
	}
	schemamodel.Finalize(db)
	return db
}

// TestPlanBidirectionalSchemaDiff_SerialSequenceRollsBack pins the rollback of
// a change to a Serial's sequence on YDB: it puts the increment the database
// held back, under the same absolute path, without a restart, and it carries
// what the forward read knew about the database -- its path and the sequence's
// restart -- because the rollback runs against the same one.
func TestPlanBidirectionalSchemaDiff_SerialSequenceRollsBack(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		DatabasePath: "/local",
		Tables: []catalog.Table{{Name: "items", Type: "TABLE", Columns: []catalog.Column{{
			Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true,
			IsAutoIncrement: true, IdentityIncrement: "5",
		}}}},
		Constraints: []catalog.Constraint{{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}
	desired := itemsWithIncrement("10")
	diff := schemadiff.CompareWithDialect(desired, current, platform.YDB)

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: current,
		Dialect:       platform.YDB,
		Capabilities:  capability.YDB262(),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Forward.Nodes, qt.DeepEquals, []ast.Node{&ast.AlterSerialSequenceNode{
		Table: "items", Column: "id", Path: "/local/items/_serial_column_id", Start: 1, Increment: 10,
	}})
	c.Assert(plan.Reverse.Diff.CurrentDatabasePath, qt.Equals, "/local")
	c.Assert(plan.Reverse.Nodes, qt.DeepEquals, []ast.Node{&ast.AlterSerialSequenceNode{
		Table: "items", Column: "id", Path: "/local/items/_serial_column_id", Start: 1, Increment: 5,
	}})
}

// TestPlanBidirectionalSchemaDiff_ARestartedSequenceRefusesBothDirections is
// the restart's half: the rollback alters the same sequence, whose restart YDB
// replays on that ALTER as on the forward one, so a forward diff that carries
// a restart plans neither direction.
func TestPlanBidirectionalSchemaDiff_ARestartedSequenceRefusesBothDirections(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		DatabasePath: "/local",
		Tables: []catalog.Table{{Name: "items", Type: "TABLE", Columns: []catalog.Column{{
			Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true,
			IsAutoIncrement: true, IdentityIncrement: "5", SequenceRestart: "100",
		}}}},
		Constraints: []catalog.Constraint{{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}
	desired := itemsWithIncrement("10")
	diff := schemadiff.CompareWithDialect(desired, current, platform.YDB)

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: current,
		Dialect:       platform.YDB,
		Capabilities:  capability.YDB262(),
	})

	c.Assert(err, qt.ErrorMatches, `.*the sequence was restarted at 100, and YDB replays that restart .*`)
	c.Assert(plan, qt.IsNil)
}
