package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/readscope"
)

// appRead is a read of database app on a connection that selected it: two
// tables, a view, a trigger on each table and two routines.
var appRead = readscope.Read{Dialect: "mysql", Current: "app", Objects: []readscope.Object{
	{Kind: readscope.Triggers, Table: "orders", Name: "orders_bi"},
	{Kind: readscope.Triggers, Table: "audit", Name: "audit_bi"},
	{Kind: readscope.Routines, Name: "total"},
	{Kind: readscope.Routines, Name: "audit_purge"},
	{Kind: readscope.Tables, Name: "orders"},
	{Kind: readscope.Tables, Name: "audit"},
	{Kind: readscope.Views, Name: "recent"},
}}

// ObjectSelector keeps an object of a read where the projection the run
// applies after the read keeps it: a trigger leaves with its table, and an
// include selector keeps the triggers of the tables it names.
func TestObjectSelector_KeepsWhatTheProjectionKeeps(t *testing.T) {
	tests := []struct {
		name    string
		read    readscope.Read
		exclude []string
		include []string
		want    []bool
	}{
		{
			name:    "a table and its trigger",
			read:    appRead,
			exclude: []string{"audit"},
			want:    []bool{true, false, true, true, true, false, true},
		},
		{
			name:    "a trigger named by its table",
			read:    appRead,
			exclude: []string{"orders.orders_bi[type=trigger]"},
			want:    []bool{false, true, true, true, true, true, true},
		},
		{
			name:    "a routine",
			read:    appRead,
			exclude: []string{"audit_*[type=function]"},
			want:    []bool{true, true, true, false, true, true, true},
		},
		{
			name:    "a table qualified by its database",
			read:    appRead,
			exclude: []string{"app.audit"},
			want:    []bool{true, false, true, true, true, false, true},
		},
		{
			// A run bound to the database refuses a third part, and a run
			// describing the realm reads it as the database: the read keeps
			// what the second leaves.
			name:    "a pattern only a realm run counts",
			read:    appRead,
			exclude: []string{"app.audit.audit_bi"},
			want:    []bool{true, false, true, true, true, true, true},
		},
		{
			name:    "an include keeps the triggers of the tables it names",
			read:    appRead,
			include: []string{"orders"},
			want:    []bool{true, false, false, false, true, false, false},
		},
		{
			name:    "an include and an exclude",
			read:    appRead,
			include: []string{"orders", "audit"},
			exclude: []string{"audit"},
			want:    []bool{true, false, false, false, true, false, false},
		},
		{
			name: "a server read names each object's database",
			read: readscope.Read{Dialect: "mysql", Objects: []readscope.Object{
				{Kind: readscope.Triggers, Schema: "app", Table: "audit", Name: "audit_bi"},
				{Kind: readscope.Triggers, Schema: "ops", Table: "audit", Name: "audit_bi"},
				{Kind: readscope.Tables, Schema: "app", Name: "audit"},
				{Kind: readscope.Tables, Schema: "ops", Name: "audit"},
			}},
			exclude: []string{"ops.audit"},
			want:    []bool{true, false, true, false},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selector := atlasfilter.ObjectSelector(selectext.None(), test.exclude, test.include)

			c.Assert(selector(test.read), qt.DeepEquals, test.want)
		})
	}
}

// Where no count of a pattern can be projected, as for an include that
// selects nothing the read holds, every object is kept: leaving out one the
// run keeps would describe it wrong.
func TestObjectSelector_KeepsEveryObjectWhereItCannotTell(t *testing.T) {
	c := qt.New(t)
	selector := atlasfilter.ObjectSelector(selectext.None(), nil, []string{"elsewhere"})

	c.Assert(selector(appRead), qt.DeepEquals, []bool{true, true, true, true, true, true, true})
}

// Without a selector there is nothing to ask.
func TestObjectSelector_NamesNothing(t *testing.T) {
	tests := []struct {
		name       string
		selections selectext.Set
		exclude    []string
		include    []string
	}{
		{name: "no selector", selections: selectext.None()},
		{name: "blank selectors", selections: selectext.None(), exclude: []string{" , "}, include: []string{""}},
		{name: "no selections", exclude: []string{"audit"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasfilter.ObjectSelector(test.selections, test.exclude, test.include), qt.IsNil)
		})
	}
}

// ReadScope marks a read context with both answers: a kind the selectors
// leave out whole, and each object they leave out.
func TestReadScope(t *testing.T) {
	c := qt.New(t)

	ctx := atlasfilter.ReadScope(t.Context(), selectext.None(), []string{"*[type=function]", "audit"}, nil)

	c.Assert(readscope.Needs(ctx, readscope.Routines), qt.IsFalse)
	c.Assert(readscope.Needs(ctx, readscope.Triggers), qt.IsTrue)
	c.Assert(readscope.Keeps(ctx, appRead), qt.DeepEquals, []bool{true, false, false, false, true, false, true})
}
