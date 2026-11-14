package lintkit_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

// states is one baseline over three versions: version 1 has public.users and
// a users table in another schema; version 2 has users with no schema named,
// beside rows no lookup can use; version 4 was read and holds nothing.
// Version 3 was never read.
func states() map[int64]lintkit.State {
	return lintkit.NewStates(lintext.Baseline{
		Columns: []lintext.BaselineColumn{
			{Version: 1, Schema: "public", Table: "users", Name: "id", DataType: "integer", NotNull: true, TablePartitioned: true},
			{Version: 1, Schema: "public", Table: "users", Name: "email", DataType: "text", TablePartitioned: true},
			{Version: 1, Schema: "audit", Table: "users", Name: "id", DataType: "bigint"},
			{Version: 2, Table: "users", Name: "id", DataType: "integer"},
			{Version: 2, Table: "", Name: "orphan"},
			// No lookup can use a nameless row, so it must not make version 2
			// name a schema or the bare users table ambiguous.
			{Version: 2, Schema: "other", Table: "users", Name: ""},
		},
		Indexes: []lintext.BaselineIndex{
			{Version: 1, Schema: "public", Table: "users", Name: "users_pkey", Unique: true, Primary: true,
				Parts: []lintext.BaselineIndexPart{{Column: "id"}}},
			{Version: 1, Schema: "public", Table: "users", Name: "users_email_prefix",
				Parts: []lintext.BaselineIndexPart{{Column: "email", Prefix: 4}}},
			{Version: 1, Schema: "public", Table: "users", Name: ""},
		},
		Hypertables: []lintext.BaselineHypertable{{Version: 1, Schema: "public", Table: "users"}},
	}, []int64{1, 4})
}

// A qualified reference resolves; a bare name two schemas carry resolves to
// neither; a state that names no schema drops the qualifier; a row no lookup
// can use is left out.
func TestState_Column(t *testing.T) {
	for _, test := range []struct {
		name    string
		version int64
		table   string
		column  string
		want    lintext.BaselineColumn
		found   bool
	}{
		{"qualified", 1, "PUBLIC.USERS", "ID",
			lintext.BaselineColumn{Version: 1, Schema: "public", Table: "users", Name: "id", DataType: "integer", NotNull: true, TablePartitioned: true}, true},
		{"ambiguous bare name", 1, "USERS", "ID", lintext.BaselineColumn{}, false},
		{"another schema", 1, "AUDIT.USERS", "ID", lintext.BaselineColumn{Version: 1, Schema: "audit", Table: "users", Name: "id", DataType: "bigint"}, true},
		{"a qualifier the state cannot contradict", 2, "PUBLIC.USERS", "ID", lintext.BaselineColumn{Version: 2, Table: "users", Name: "id", DataType: "integer"}, true},
		{"a row without a table", 2, "", "ORPHAN", lintext.BaselineColumn{}, false},
		{"a version never read", 3, "USERS", "ID", lintext.BaselineColumn{}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, found := states()[test.version].Column(test.table, test.column)
			c.Assert(found, qt.Equals, test.found)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// The table-level lookups place the table the way Column does.
func TestState_TableLookups(t *testing.T) {
	c := qt.New(t)
	state := states()[1]

	c.Assert(state.Unread(), qt.IsFalse)
	c.Assert(states()[2].Unread(), qt.IsFalse)
	c.Assert(states()[3].Unread(), qt.IsTrue)
	c.Assert(states()[4].Unread(), qt.IsFalse)
	c.Assert(state.TableColumns("PUBLIC.USERS"), qt.HasLen, 2)
	c.Assert(state.TableColumns("USERS"), qt.IsNil)
	c.Assert(state.Hypertable("PUBLIC.USERS"), qt.IsTrue)
	c.Assert(state.Hypertable("AUDIT.USERS"), qt.IsFalse)
	c.Assert(state.Partitioned("PUBLIC.USERS"), qt.IsTrue)
	c.Assert(state.Partitioned("AUDIT.USERS"), qt.IsFalse)
	c.Assert(state.TableIndexes("PUBLIC.USERS"), qt.HasLen, 2)
	c.Assert(state.ColumnIndexes("PUBLIC.USERS", "EMAIL"), qt.HasLen, 1)
	named, found := state.IndexNamed("PUBLIC.USERS", "USERS_PKEY")
	c.Assert(found, qt.IsTrue)
	c.Assert(named.Name, qt.Equals, "users_pkey")
	covering, found := state.UniqueCovering("PUBLIC.USERS", []string{"ID", "EMAIL"})
	c.Assert(found, qt.IsTrue)
	c.Assert(covering.Name, qt.Equals, "users_pkey")
	_, found = state.UniqueCovering("PUBLIC.USERS", []string{"EMAIL"})
	c.Assert(found, qt.IsFalse)
}

// LacksTable answers true only where the state was read and no table it holds
// can be the one named. A schema the state does not hold, and any qualifier
// against a state that names no schema, leave the answer open.
func TestState_LacksTable(t *testing.T) {
	// dotted names no schema and holds a table whose name carries a dot, so
	// only the state's lack of schemas keeps OTHER. from reading as a schema
	// it holds.
	dotted := lintkit.NewStates(lintext.Baseline{
		Columns: []lintext.BaselineColumn{{Version: 1, Table: "other.x", Name: "id"}},
	}, nil)[1]
	for _, test := range []struct {
		name  string
		state lintkit.State
		table string
		want  bool
	}{
		{"absent, qualified by a held schema", states()[1], "PUBLIC.MISSING", true},
		{"absent, bare", states()[1], "MISSING", true},
		{"present, qualified", states()[1], "PUBLIC.USERS", false},
		{"present, bare in two schemas", states()[1], "USERS", false},
		{"a schema the state does not hold", states()[1], "OTHER.MISSING", false},
		{"no reference", states()[1], "", false},
		{"absent, bare, against no schema", states()[2], "MISSING", true},
		{"qualified, against no schema", states()[2], "PUBLIC.MISSING", false},
		{"qualified, against no schema and a dotted table name", dotted, "OTHER.MISSING", false},
		{"a version never read", states()[3], "MISSING", false},
		{"absent, bare, from a state read empty", states()[4], "MISSING", true},
		{"qualified, from a state read empty", states()[4], "PUBLIC.MISSING", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.state.LacksTable(test.table), qt.Equals, test.want)
		})
	}
}

// An index is placed by its own name, in any table, the way LacksTable places
// a table: present under that name anywhere is not absent, and a state that
// cannot say answers false.
func TestState_LacksIndex(t *testing.T) {
	for _, test := range []struct {
		name  string
		state lintkit.State
		index string
		want  bool
	}{
		{"absent, bare", states()[1], "USERS_NAME_IDX", true},
		{"absent, qualified by a held schema", states()[1], "PUBLIC.USERS_NAME_IDX", true},
		{"present, bare", states()[1], "USERS_PKEY", false},
		{"present, qualified", states()[1], "PUBLIC.USERS_PKEY", false},
		{"present in a schema the reference does not name", states()[1], "AUDIT.USERS_PKEY", true},
		{"a schema the state does not hold", states()[1], "OTHER.USERS_NAME_IDX", false},
		{"no reference", states()[1], "", false},
		{"absent, bare, against no schema", states()[2], "USERS_NAME_IDX", true},
		{"qualified, against no schema", states()[2], "PUBLIC.USERS_NAME_IDX", false},
		{"a version never read", states()[3], "USERS_NAME_IDX", false},
		{"absent, bare, from a state read empty", states()[4], "USERS_NAME_IDX", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.state.LacksIndex(test.index), qt.Equals, test.want)
		})
	}
}

// A state is read when the caller says so or when it has rows, and only then;
// a version neither names has the zero state.
func TestNewStates_Read(t *testing.T) {
	for _, test := range []struct {
		name     string
		baseline lintext.Baseline
		read     []int64
		version  int64
		unread   bool
	}{
		{"named as read, no rows", lintext.Baseline{}, []int64{2}, 2, false},
		{"rows, not named", lintext.Baseline{Columns: []lintext.BaselineColumn{{Version: 2, Table: "t", Name: "id"}}}, nil, 2, false},
		{"only an index row", lintext.Baseline{Indexes: []lintext.BaselineIndex{{Version: 2, Table: "t", Name: "i"}}}, nil, 2, false},
		{"another version named", lintext.Baseline{}, []int64{1}, 2, true},
		{"nothing at all", lintext.Baseline{}, nil, 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintkit.NewStates(test.baseline, test.read)[test.version].Unread(), qt.Equals, test.unread)
		})
	}
}

// A state names schemas when it was read and a row names one. Version 1 does;
// version 2 was read from one schema; version 4 was read and holds nothing;
// version 3 was never read.
func TestState_NamesSchemas(t *testing.T) {
	for _, test := range []struct {
		version int64
		want    bool
	}{
		{version: 1, want: true},
		{version: 2, want: false},
		{version: 3, want: false},
		{version: 4, want: false},
	} {
		t.Run(fmt.Sprintf("version %d", test.version), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(states()[test.version].NamesSchemas(), qt.Equals, test.want)
		})
	}
}

// A key over whole columns names them; a prefix, an expression or a part the
// reader could not name keys something else.
func TestWholeKeyColumns(t *testing.T) {
	for _, test := range []struct {
		name  string
		index lintext.BaselineIndex
		want  []string
	}{
		{"whole columns", lintext.BaselineIndex{Parts: []lintext.BaselineIndexPart{{Column: "a"}, {Column: `"B"`}}}, []string{"A", "B"}},
		{"a prefix", lintext.BaselineIndex{Parts: []lintext.BaselineIndexPart{{Column: "a", Prefix: 3}}}, nil},
		{"an expression", lintext.BaselineIndex{Parts: []lintext.BaselineIndexPart{{}}}, nil},
		{"incomplete", lintext.BaselineIndex{Incomplete: true, Parts: []lintext.BaselineIndexPart{{Column: "a"}}}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintkit.WholeKeyColumns(test.index), qt.DeepEquals, test.want)
		})
	}
}
