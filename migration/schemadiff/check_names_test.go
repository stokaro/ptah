package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/schemaprep"
	"ptah.run/migration/schemadiff"
)

// twoTablesOfOneCheckName declares a CHECK named ck on t1 and CK on t2.
func twoTablesOfOneCheckName() *schemamodel.Database {
	desired := &schemamodel.Database{}
	for _, table := range []struct{ name, check string }{{"t1", "ck"}, {"t2", "CK"}} {
		desired.Tables = append(desired.Tables, schemamodel.Table{StructName: table.name, Name: table.name})
		desired.Fields = append(desired.Fields, schemamodel.Field{StructName: table.name, Name: "a", Type: "int"})
		desired.Constraints = append(desired.Constraints, schemamodel.Constraint{
			StructName: table.name, Name: table.check, Type: "CHECK", Table: table.name, CheckExpression: "a > 0",
		})
	}
	return desired
}

func serverOf(dialect string) catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: dialect, Schema: "app", IdentifierSemantics: identifier.ForDialect(dialect)}
}

// The comparison reads the dialect the server answered with: a MySQL server
// keeps a CHECK name per schema and refuses the pair partway through an apply,
// so the comparison refuses it first.
func TestCompareWithDatabaseInfo_TwoTablesOfOneCheckNameOnMySQL_FailurePath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), twoTablesOfOneCheckName(), &catalog.Database{},
		serverOf("mysql"), nil, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, schemaprep.ErrDuplicateCheckName)
	c.Assert(diff, qt.IsNil)
}

// The control: a server that answered as MariaDB keeps the name per table, so
// the same declaration compares.
func TestCompareWithDatabaseInfo_TwoTablesOfOneCheckNameOnMariaDB_HappyPath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), twoTablesOfOneCheckName(), &catalog.Database{},
		serverOf("mariadb"), nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff, qt.IsNotNil)
}
