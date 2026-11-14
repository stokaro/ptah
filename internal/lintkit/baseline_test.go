package lintkit_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

// states is one baseline over four versions. Version 1 has public.users and a
// users table in another schema, types, views and a trigger, and its read lists
// types, views and triggers in full; reporting is a schema only a view names.
// Version 2 has users and an enum with no schema named, beside rows no lookup
// can use, and its read lists no kind. Version 4 was read, lists every kind and
// holds nothing. Version 3 was never read.
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
		Objects: []lintext.BaselineObject{
			{Version: 1, Kind: lintext.BaselineType, Schema: "public", Name: "mood", Enum: true, Values: []string{"happy", "sad"}},
			{Version: 1, Kind: lintext.BaselineType, Schema: "audit", Name: "mood", Enum: true, Values: []string{"x"}},
			{Version: 1, Kind: lintext.BaselineType, Schema: "public", Name: "address"},
			{Version: 1, Kind: lintext.BaselineView, Schema: "public", Name: "active_users"},
			{Version: 1, Kind: lintext.BaselineView, Schema: "reporting", Name: "daily"},
			{Version: 1, Kind: lintext.BaselineTrigger, Schema: "public", Table: "users", Name: "audit"},
			{Version: 1, Kind: lintext.BaselineSequence, Schema: "public", Name: "users_id_seq"},
			// A nameless object is a row no lookup can use.
			{Version: 1, Kind: lintext.BaselineView, Schema: "public"},
			{Version: 2, Kind: lintext.BaselineType, Name: "mood", Enum: true, Values: []string{"a"}},
		},
		Reads: []lintext.BaselineRead{
			{Version: 1, Listed: []lintext.BaselineObjectKind{lintext.BaselineType, lintext.BaselineView, lintext.BaselineTrigger}},
			{Version: 4, Listed: lintext.BaselineObjectKinds()},
		},
	})
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
	})[1]
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
		{"absent, qualified by a schema only an object names", states()[1], "REPORTING.MISSING", true},
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
		version  int64
		unread   bool
	}{
		{"named as read, no rows", lintext.Baseline{Reads: []lintext.BaselineRead{{Version: 2}}}, 2, false},
		{"rows, not named", lintext.Baseline{Columns: []lintext.BaselineColumn{{Version: 2, Table: "t", Name: "id"}}}, 2, false},
		{"only an index row", lintext.Baseline{Indexes: []lintext.BaselineIndex{{Version: 2, Table: "t", Name: "i"}}}, 2, false},
		{"only an object row", lintext.Baseline{Objects: []lintext.BaselineObject{{Version: 2, Kind: lintext.BaselineView, Name: "v"}}}, 2, false},
		{"only a row no lookup can use", lintext.Baseline{Objects: []lintext.BaselineObject{{Version: 2, Kind: lintext.BaselineView}}}, 2, true},
		{"another version named", lintext.Baseline{Reads: []lintext.BaselineRead{{Version: 1}}}, 2, true},
		{"nothing at all", lintext.Baseline{}, 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintkit.NewStates(test.baseline)[test.version].Unread(), qt.Equals, test.unread)
		})
	}
}

// An object is placed by its kind and its own name, or by its table and its
// name for a trigger written ON a table, the way LacksTable places a table.
// Only a read that lists the kind in full can show one absent.
func TestState_LacksObject(t *testing.T) {
	for _, test := range []struct {
		name  string
		state lintkit.State
		kind  lintext.BaselineObjectKind
		ref   string
		table string
		want  bool
	}{
		{"absent type, bare", states()[1], lintext.BaselineType, "GONE", "", true},
		{"absent type, qualified by a held schema", states()[1], lintext.BaselineType, "PUBLIC.GONE", "", true},
		{"present type, bare in two schemas", states()[1], lintext.BaselineType, "MOOD", "", false},
		{"present type, qualified", states()[1], lintext.BaselineType, "PUBLIC.ADDRESS", "", false},
		{"present in a schema the reference does not name", states()[1], lintext.BaselineType, "AUDIT.ADDRESS", "", true},
		{"a schema the state does not hold", states()[1], lintext.BaselineType, "OTHER.GONE", "", false},
		{"no reference", states()[1], lintext.BaselineType, "", "", false},
		{"absent view", states()[1], lintext.BaselineView, "GONE", "", true},
		{"a view named like a type", states()[1], lintext.BaselineView, "MOOD", "", true},
		{"present view", states()[1], lintext.BaselineView, "PUBLIC.ACTIVE_USERS", "", false},
		{"a kind the read does not list", states()[1], lintext.BaselineSequence, "GONE", "", false},
		{"a trigger on its table", states()[1], lintext.BaselineTrigger, "AUDIT", "USERS", false},
		{"a trigger on its qualified table", states()[1], lintext.BaselineTrigger, "AUDIT", "PUBLIC.USERS", false},
		{"a trigger another table has", states()[1], lintext.BaselineTrigger, "AUDIT", "PUBLIC.ORDERS", true},
		{"a trigger on a table of a schema the state does not hold", states()[1], lintext.BaselineTrigger, "AUDIT", "OTHER.USERS", false},
		{"a trigger named on its own", states()[1], lintext.BaselineTrigger, "AUDIT", "", false},
		{"an absent trigger named on its own", states()[1], lintext.BaselineTrigger, "GONE", "", true},
		{"rows, from a read that lists no kind", states()[2], lintext.BaselineType, "GONE", "", false},
		{"a version never read", states()[3], lintext.BaselineType, "GONE", "", false},
		{"absent, from a state read empty", states()[4], lintext.BaselineSequence, "GONE", "", true},
		{"qualified, from a state read empty", states()[4], lintext.BaselineSequence, "PUBLIC.GONE", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.state.LacksObject(test.kind, test.ref, test.table), qt.Equals, test.want)
		})
	}
}

// An enum is placed the way TableColumns places a table, and a type that is
// no enum has no labels.
func TestState_EnumValues(t *testing.T) {
	for _, test := range []struct {
		name   string
		state  lintkit.State
		ref    string
		want   []string
		isEnum bool
	}{
		{"qualified", states()[1], "PUBLIC.MOOD", []string{"happy", "sad"}, true},
		{"bare in two schemas", states()[1], "MOOD", nil, false},
		{"not an enum", states()[1], "PUBLIC.ADDRESS", nil, false},
		{"absent", states()[1], "PUBLIC.GONE", nil, false},
		{"a qualifier the state cannot contradict", states()[2], "PUBLIC.MOOD", []string{"a"}, true},
		{"a version never read", states()[3], "MOOD", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, isEnum := test.state.EnumValues(test.ref)
			c.Assert(isEnum, qt.Equals, test.isEnum)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// An object resolves by its kind and its name the way a table does, a
// non-enum type included, and a trigger by its name alone.
func TestState_Object_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name  string
		state lintkit.State
		kind  lintext.BaselineObjectKind
		ref   string
		want  lintext.BaselineObject
	}{
		{"a composite type", states()[1], lintext.BaselineType, "PUBLIC.ADDRESS",
			lintext.BaselineObject{Version: 1, Kind: lintext.BaselineType, Schema: "public", Name: "address"}},
		{"an enum", states()[1], lintext.BaselineType, "AUDIT.MOOD", lintext.BaselineObject{
			Version: 1, Kind: lintext.BaselineType, Schema: "audit", Name: "mood", Enum: true, Values: []string{"x"},
		}},
		{"a trigger by its name", states()[1], lintext.BaselineTrigger, "AUDIT", lintext.BaselineObject{
			Version: 1, Kind: lintext.BaselineTrigger, Schema: "public", Table: "users", Name: "audit",
		}},
		{"a qualifier the state cannot contradict", states()[2], lintext.BaselineType, "PUBLIC.MOOD", lintext.BaselineObject{
			Version: 2, Kind: lintext.BaselineType, Name: "mood", Enum: true, Values: []string{"a"},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := test.state.Object(test.kind, test.ref)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// An object of another kind, a bare name two schemas carry, an absent object
// and a version never read resolve to nothing.
func TestState_Object_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		state lintkit.State
		kind  lintext.BaselineObjectKind
		ref   string
	}{
		{"another kind", states()[1], lintext.BaselineView, "PUBLIC.ADDRESS"},
		{"bare in two schemas", states()[1], lintext.BaselineType, "MOOD"},
		{"absent", states()[1], lintext.BaselineType, "PUBLIC.GONE"},
		{"no name", states()[1], lintext.BaselineView, ""},
		{"a version never read", states()[3], lintext.BaselineType, "MOOD"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := test.state.Object(test.kind, test.ref)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.DeepEquals, lintext.BaselineObject{})
		})
	}
}

// The labels Object and EnumValues return are the caller's: changing them
// leaves the state as it was.
func TestState_Object_ReturnsACopy(t *testing.T) {
	c := qt.New(t)
	state := states()[1]
	object, _ := state.Object(lintext.BaselineType, "PUBLIC.MOOD")
	object.Values[0] = "changed"
	labels, _ := state.EnumValues("PUBLIC.MOOD")
	labels[1] = "changed"
	again, _ := state.EnumValues("PUBLIC.MOOD")
	c.Assert(again, qt.DeepEquals, []string{"happy", "sad"})
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
