package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
	"ptah.run/internal/schemaprep"
)

// twoTablesOfOneCheckName declares a CHECK named ck on t1 and CK on t2.
func twoTablesOfOneCheckName() schemamodel.Database {
	var database schemamodel.Database
	for _, table := range []struct{ name, check string }{{"t1", "ck"}, {"t2", "CK"}} {
		database.Tables = append(database.Tables, schemamodel.Table{StructName: table.name, Name: table.name})
		database.Fields = append(database.Fields, schemamodel.Field{StructName: table.name, Name: "a", Type: "int"})
		database.Constraints = append(database.Constraints, schemamodel.Constraint{
			StructName: table.name, Name: table.check, Type: "CHECK", Table: table.name, CheckExpression: "a > 0",
		})
	}
	return database
}

// A render writes the first table and then stops at the second CHECK, which
// MySQL refuses, so it is refused before any statement is written. The
// refusal is settled inside Ptah, so no server is involved.
func TestCollectDatabase_TwoTablesOfOneCheckNameOnMySQL_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := modelast.CollectDatabase(twoTablesOfOneCheckName(), platform.MySQL, modelast.Lowering{Context: t.Context()})

	c.Assert(err, qt.ErrorIs, schemaprep.ErrDuplicateCheckName)
	c.Assert(statements, qt.IsNil)
}

// The control: MariaDB keeps a CHECK name per table, so the same schema
// renders. A refusal that ignored the descriptor would fail it.
func TestCollectDatabase_TwoTablesOfOneCheckNameOnMariaDB_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements, err := modelast.CollectDatabase(twoTablesOfOneCheckName(), platform.MariaDB, modelast.Lowering{Context: t.Context()})

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.IsNotNil)
}
