package uniquelowering_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/uniquelowering"
)

// uniqueAccounts is a table with a named and commented UNIQUE, an unnamed
// one, a column's own UNIQUE, a UNIQUE over the key and one no index can hold.
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
				IncludeColumns: []string{"email"}, Comment: "One login per tenant"},
			{StructName: "Account", Type: "unique", Columns: []string{"login"}},
			{StructName: "Account", Name: "uq_id", Type: "UNIQUE", Columns: []string{"id"}},
			{StructName: "Account", Name: "uq_later", Type: "UNIQUE", Columns: []string{"tenant"}, Deferrable: true},
			{StructName: "Account", Name: "ck", Type: "CHECK", CheckExpression: "tenant > 0"},
		},
	}
}

// TestConstraintsAsIndexes_HappyPath writes each UNIQUE an index can hold as
// a unique index under the name IndexName gives it, with the constraint's
// comment, folds the one over the key, and leaves a deferrable constraint,
// and every other kind, as it was. The input is not changed.
func TestConstraintsAsIndexes_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := uniqueAccounts()

	lowered := uniquelowering.ConstraintsAsIndexes(database)

	c.Assert(lowered.Indexes, qt.DeepEquals, []schemamodel.Index{
		{StructName: "Account", Name: "uq_tenant_login", Fields: []string{"tenant", "login"}, Unique: true,
			IncludeColumns: []string{"email"}, Comment: "One login per tenant"},
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

// TestConstraintsAsIndexes_Nil returns a nil database as it is.
func TestConstraintsAsIndexes_Nil(t *testing.T) {
	c := qt.New(t)
	c.Assert(uniquelowering.ConstraintsAsIndexes(nil), qt.IsNil)
}

// TestIndexName names the index a UNIQUE becomes: the constraint's own name,
// or the table's and the columns' for one that has none.
func TestIndexName(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		declared string
		columns  []string
		want     string
	}{
		{name: "a named constraint", table: "users", declared: "uq_users_email", columns: []string{"email"}, want: "uq_users_email"},
		{name: "a column", table: "users", columns: []string{"email"}, want: "users_email_key"},
		{name: "an unnamed constraint over two columns", table: "users", declared: "  ", columns: []string{"tenant", "login"},
			want: "users_tenant_login_key"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(uniquelowering.IndexName(test.table, test.declared, test.columns), qt.Equals, test.want)
		})
	}
}

// TestIsTheKey folds a UNIQUE over exactly the key columns, in any order, and
// nothing narrower or wider.
func TestIsTheKey(t *testing.T) {
	tests := []struct {
		name    string
		columns []string
		key     []string
		want    bool
	}{
		{name: "the key", columns: []string{"id"}, key: []string{"id"}, want: true},
		{name: "the key in another order", columns: []string{"b", "a"}, key: []string{"a", "b"}, want: true},
		{name: "part of the key", columns: []string{"a"}, key: []string{"a", "b"}, want: false},
		{name: "the key and more", columns: []string{"a", "b"}, key: []string{"a"}, want: false},
		{name: "a table without a key", columns: []string{"a"}, key: nil, want: false},
		{name: "a column repeated", columns: []string{"a", "a"}, key: []string{"a", "b"}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(uniquelowering.IsTheKey(test.columns, test.key), qt.Equals, test.want)
		})
	}
}
