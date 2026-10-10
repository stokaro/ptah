package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine/builtin"
)

// TestLowerDesired_YDBRegistersItsLowering pins the lowering the built-in
// runtime registers for YDB: a declared UNIQUE becomes the unique index YDB
// holds, and a privilege spelled as GRANT does becomes the permission name
// YDB reports. The declaration itself is left as written.
func TestLowerDesired_YDBRegistersItsLowering(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts"}},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Account", Name: "login", Type: "TEXT", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "Account", Name: "uq_accounts_login", Type: "UNIQUE", Columns: []string{"login"}},
		},
		Grants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT ROW"}, OnTable: "accounts"}},
	}

	lowered, err := must.Must(builtin.New()).LowerDesired(t.Context(), schemapreparation.LoweringRequest{
		Target: "ydb", Desired: desired, Current: &catalog.Database{}, Semantics: identifier.ForDialect("ydb"),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(lowered.Constraints, qt.HasLen, 0)
	c.Assert(lowered.Indexes, qt.HasLen, 1)
	c.Assert(lowered.Indexes[0].Name, qt.Equals, "uq_accounts_login")
	c.Assert(lowered.Indexes[0].Unique, qt.IsTrue)
	c.Assert(lowered.Grants[0].Privileges, qt.DeepEquals, []string{"YDB.GRANULAR.SELECT_ROW"})
	c.Assert(desired.Constraints, qt.HasLen, 1)
	c.Assert(desired.Grants[0].Privileges, qt.DeepEquals, []string{"SELECT ROW"})
}
