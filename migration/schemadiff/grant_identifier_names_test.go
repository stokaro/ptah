package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A grant declaration may spell its table and columns differently from the
// declaration of the table, and on SQL Server only the catalog can say whether
// T and t, or NAME and name, are one name. The comparison asks it about every
// name a grant declares; a name it does not ask about keeps its spelling, so
// the grant on NAME never matches the name column and every plan grants it
// and revokes it again.
func TestCompareWithDatabase_ResolvesTheNamesAGrantDeclares(t *testing.T) {
	c := qt.New(t)
	conn := &nameCaptureConnection{}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", Schema: "dbo"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "name", Type: "NVARCHAR(50)"}},
		Grants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "dbo.T", Columns: []string{"NAME"}}},
	}

	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, desired, &catalog.Database{}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, errNamesCaptured)
	c.Assert(diff, qt.IsNil)
	c.Assert(conn.names, qt.Contains, "T")
	c.Assert(conn.names, qt.Contains, "NAME")
}
