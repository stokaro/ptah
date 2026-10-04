package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// uniqueAccounts is a table with a named UNIQUE, an unnamed one, a column's
// own UNIQUE, a UNIQUE over the key and one the renderer refuses.
func uniqueAccounts() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts", Schema: "app"}},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "id", Type: "BIGINT", Primary: true, Unique: true},
			{StructName: "Account", Name: "email", Type: "TEXT", Nullable: true, Unique: true},
			{StructName: "Account", Name: "tenant", Type: "BIGINT", Nullable: true},
			{StructName: "Account", Name: "login", Type: "TEXT", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "Account", Name: "uq_tenant_login", Type: "UNIQUE", Columns: []string{"tenant", "login"},
				IncludeColumns: []string{"email"}},
			{StructName: "Account", Type: "unique", Columns: []string{"login"}},
			{StructName: "Account", Name: "uq_id", Type: "UNIQUE", Columns: []string{"id"}},
			{StructName: "Account", Name: "uq_later", Type: "UNIQUE", Columns: []string{"tenant"}, Deferrable: true},
			{StructName: "Account", Name: "ck", Type: "CHECK", CheckExpression: "tenant > 0"},
		},
	}
}

// TestUniqueConstraintsAsIndexesFor_HappyPath writes each UNIQUE a YDB target
// holds as a unique index under the name the renderer gives it, folds the one
// over the key, and leaves a constraint the renderer refuses, and every other
// kind, as it was. The input is not changed.
func TestUniqueConstraintsAsIndexesFor_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := uniqueAccounts()

	lowered := schemaprep.UniqueConstraintsAsIndexesFor(database, platform.YDB, capability.YDB262())

	c.Assert(lowered.Indexes, qt.DeepEquals, []schemamodel.Index{
		{StructName: "Account", Name: "uq_tenant_login", Fields: []string{"tenant", "login"}, Unique: true,
			IncludeColumns: []string{"email"}},
		{StructName: "Account", Name: "accounts_login_key", Fields: []string{"login"}, Unique: true},
		{StructName: "Account", Name: "accounts_email_key", Fields: []string{"email"}, Unique: true},
	})
	c.Assert(lowered.Constraints, qt.DeepEquals, []schemamodel.Constraint{database.Constraints[3], database.Constraints[4]})
	c.Assert(lowered.Fields[0].Unique, qt.IsFalse)
	c.Assert(lowered.Fields[1].Unique, qt.IsFalse)
	c.Assert(database.Fields[1].Unique, qt.IsTrue, qt.Commentf("the input must not change"))
	c.Assert(database.Constraints, qt.HasLen, 5)
	c.Assert(database.Indexes, qt.HasLen, 0)
}

// TestUniqueConstraintsAsIndexesFor_LeavesOtherTargets returns the input
// itself on every target that takes a UNIQUE constraint or is not YDB.
func TestUniqueConstraintsAsIndexesFor_LeavesOtherTargets(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "postgres", dialect: platform.Postgres, caps: capability.Postgres17()},
		{name: "spanner, which refuses the constraint itself", dialect: platform.Spanner, caps: capability.SpannerPostgres()},
		{name: "a YDB line that took the constraint", dialect: platform.YDB,
			caps: capability.YDB262().With(capability.UniqueConstraints, true)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := uniqueAccounts()
			c.Assert(schemaprep.UniqueConstraintsAsIndexesFor(database, test.dialect, test.caps), qt.Equals, database)
		})
	}
}
