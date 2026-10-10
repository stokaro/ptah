package ydbfamily_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbfamily"
)

// A declaration is read as YDB keeps it: a compression or cache mode in lower
// case whatever case it was written in, a pool kind as written, and the
// columns in the order the declaration lists them.
func TestParseDeclaration_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ydbschema.ColumnFamily
	}{
		{
			name:   "every setting",
			values: map[string]string{"name": "cold", "data": "hdd", "compression": "LZ4", "cache_mode": "IN_MEMORY", "fields": "b, a"},
			want:   ydbschema.ColumnFamily{Name: "cold", Data: "hdd", Compression: "lz4", CacheMode: "in_memory", Columns: []string{"b", "a"}},
		},
		{
			name:   "a name alone",
			values: map[string]string{"name": " cold ", "table": "events"},
			want:   ydbschema.ColumnFamily{Name: "cold"},
		},
		{
			name:   "the default family's settings",
			values: map[string]string{"name": "default", "compression": "off", "cache_mode": "regular"},
			want:   ydbschema.ColumnFamily{Name: "default", Compression: "off", CacheMode: "regular"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbfamily.ParseDeclaration(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A value YDB would refuse, or keep as something else, is refused where it was
// written, naming the attribute.
func TestParseDeclaration_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		values        map[string]string
		wantAttribute string
		wantErr       string
	}{
		{name: "no name", values: map[string]string{"data": "hdd"}, wantAttribute: "name",
			wantErr: `invalid name "": a column family needs a name`},
		{name: "an empty pool", values: map[string]string{"name": "cold", "data": " "}, wantAttribute: "data",
			wantErr: `invalid data " ": names the kind of storage pool .*`},
		{name: "zstd", values: map[string]string{"name": "cold", "compression": "ZSTD"}, wantAttribute: "compression",
			wantErr: "invalid compression \"ZSTD\": takes off or lz4: YDB keeps zstd for column-oriented tables, .*"},
		{name: "a compression YDB does not know", values: map[string]string{"name": "cold", "compression": "gzip"},
			wantAttribute: "compression", wantErr: `invalid compression "gzip": takes off or lz4`},
		{name: "a cache mode YDB does not know", values: map[string]string{"name": "cold", "cache_mode": "hot"},
			wantAttribute: "cache_mode", wantErr: `invalid cache_mode "hot": takes regular or in_memory`},
		{name: "an empty column", values: map[string]string{"name": "cold", "fields": "a,,b"}, wantAttribute: "fields",
			wantErr: `invalid fields "a,,b": takes a comma-separated list of column names, none of them empty`},
		{name: "a column twice", values: map[string]string{"name": "cold", "fields": "a,b,a"}, wantAttribute: "fields",
			wantErr: `invalid fields "a,b,a": names column "a" twice`},
		{name: "columns in the default family", values: map[string]string{"name": "default", "fields": "a"},
			wantAttribute: "fields", wantErr: `invalid fields "a": the default family holds the key and every column .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbfamily.ParseDeclaration(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var declared *ydbfamily.DeclarationError
			c.Assert(err, qt.ErrorAs, &declared)
			c.Assert(declared.Attribute, qt.Equals, test.wantAttribute)
			c.Assert(got, qt.DeepEquals, ydbschema.ColumnFamily{})
		})
	}
}

// Families a table can carry raise no refusal, the default family's settings
// and families listing no column included.
func TestRefusal_HappyPath(t *testing.T) {
	c := qt.New(t)
	families := []ydbschema.ColumnFamily{
		{Name: "default", Compression: "lz4"},
		{Name: "cold", Columns: []string{"a", "b"}},
		{Name: "empty"},
	}
	c.Assert(ydbfamily.Refusal(families, []string{"id", "a", "b"}, []string{"id"}), qt.Equals, "")
}

// What one family cannot see on its own is refused with YDB's own words where
// YDB has them.
func TestRefusal_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		families []ydbschema.ColumnFamily
		want     string
	}{
		{
			name:     "a family twice",
			families: []ydbschema.ColumnFamily{{Name: "cold"}, {Name: "cold"}},
			want:     "it declares column family \"cold\" twice (`Family cold specified more than once`)",
		},
		{
			name:     "a column in two families",
			families: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}, {Name: "warm", Columns: []string{"a"}}},
			want:     `column "a" is in two column families, "cold" and "warm"`,
		},
		{
			name:     "a column the table does not declare",
			families: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"z"}}},
			want:     `column family "cold" names column "z", which the table does not declare`,
		},
		{
			name:     "a key column",
			families: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"id"}}},
			want: "column family \"cold\" names key column \"id\", and YDB keeps every key column in the default " +
				"family (`Key column 'id' must belong to the default family`)",
		},
		{
			name:     "columns in the default family",
			families: []ydbschema.ColumnFamily{{Name: "default", Columns: []string{"a"}}},
			want: "its default column family lists columns, and the default family holds every column no other " +
				"family names",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbfamily.Refusal(test.families, []string{"id", "a"}, []string{"id"}), qt.Equals, test.want)
		})
	}
}

// A CREATE TABLE needs a key for any family it writes and one more for a
// cache mode it writes, `regular` too, since 25.1 refuses CACHE_MODE whatever
// its value. A default family stating nothing writes nothing.
func TestRequirements(t *testing.T) {
	tests := []struct {
		name     string
		families []ydbschema.ColumnFamily
		want     []capability.Capability
	}{
		{name: "none", families: nil, want: nil},
		{name: "the default family stating nothing", families: []ydbschema.ColumnFamily{{Name: "default"}}, want: nil},
		{name: "the default family stating compression",
			families: []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}},
			want:     []capability.Capability{capability.ColumnFamilies}},
		{name: "a family", families: []ydbschema.ColumnFamily{{Name: "cold"}},
			want: []capability.Capability{capability.ColumnFamilies}},
		{name: "a regular cache", families: []ydbschema.ColumnFamily{{Name: "cold", CacheMode: "regular"}},
			want: []capability.Capability{capability.ColumnFamilies, capability.ColumnFamilyCacheMode}},
		{name: "a cache mode", families: []ydbschema.ColumnFamily{{Name: "default", CacheMode: "in_memory"}},
			want: []capability.Capability{capability.ColumnFamilies, capability.ColumnFamilyCacheMode}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var got []capability.Capability
			for _, requirement := range ydbfamily.Requirements(test.families) {
				got = append(got, requirement.Key)
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A change needs the keys of what it writes, not of what either side holds:
// a cache mode the table holds and the declaration does not state is not
// written, so it asks for no cache mode key.
func TestChangeRequirements(t *testing.T) {
	tests := []struct {
		name             string
		desired, current []ydbschema.ColumnFamily
		want             []capability.Capability
	}{
		{name: "nothing written", desired: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}},
			current: []ydbschema.ColumnFamily{{Name: "cold", CacheMode: "in_memory", Columns: []string{"a"}}}, want: nil},
		{name: "a column moved, the held cache mode kept",
			desired: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}},
			current: []ydbschema.ColumnFamily{{Name: "cold", CacheMode: "in_memory"}},
			want:    []capability.Capability{capability.ColumnFamilies}},
		{name: "a cache mode set", desired: []ydbschema.ColumnFamily{{Name: "cold", CacheMode: "regular"}},
			current: []ydbschema.ColumnFamily{{Name: "cold", CacheMode: "in_memory"}},
			want:    []capability.Capability{capability.ColumnFamilies, capability.ColumnFamilyCacheMode}},
		{name: "a family added with a cache mode",
			desired: []ydbschema.ColumnFamily{{Name: "hot", CacheMode: "in_memory"}}, current: nil,
			want: []capability.Capability{capability.ColumnFamilies, capability.ColumnFamilyCacheMode}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var got []capability.Capability
			for _, requirement := range ydbfamily.ChangeRequirements(test.desired, test.current) {
				got = append(got, requirement.Key)
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A table satisfies a declaration when it holds what the declaration states:
// the order of families and of columns and the case of a setting do not
// matter, and neither does a setting, a family or a keep_in_memory the table
// holds and the declaration does not state, which a table profile can give
// every new table.
func TestSatisfied_HappyPath(t *testing.T) {
	tests := []struct {
		name             string
		desired, current []ydbschema.ColumnFamily
	}{
		{
			name:    "order and case",
			desired: []ydbschema.ColumnFamily{{Name: "b", Columns: []string{"y", "x"}}, {Name: "a", Compression: "LZ4"}},
			current: []ydbschema.ColumnFamily{{Name: "a", Compression: "lz4"}, {Name: "b", Columns: []string{"x", "y"}}},
		},
		{
			name:    "settings the declaration does not state",
			desired: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}},
			current: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "lz4", CacheMode: "in_memory",
				Columns: []string{"a"}}},
		},
		{
			name:    "the default family a profile compresses and keeps in memory",
			desired: nil,
			current: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", KeepInMemory: true}},
		},
		{
			name:    "a family a profile adds to every table",
			desired: []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}},
			current: []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}, {Name: "extra", Compression: "lz4"}},
		},
		{
			name:    "keep_in_memory on both sides",
			desired: []ydbschema.ColumnFamily{{Name: "default", KeepInMemory: true}},
			current: []ydbschema.ColumnFamily{{Name: "default", Compression: "off", KeepInMemory: true}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbfamily.Satisfied(test.desired, test.current), qt.IsTrue)
		})
	}
}

// Each setting a declaration states counts, `off` and `regular` too, and so do
// a column's family, a family the table lacks, a pool's case and a family's
// name, which YDB keeps as written.
func TestSatisfied_FailurePath(t *testing.T) {
	held := []ydbschema.ColumnFamily{
		{Name: "cold", Data: "hdd", Compression: "lz4", CacheMode: "in_memory", Columns: []string{"a"}},
		{Name: "default", Compression: "lz4"},
	}
	tests := []struct {
		name    string
		desired []ydbschema.ColumnFamily
	}{
		{name: "the name's case", desired: []ydbschema.ColumnFamily{{Name: "Cold", Columns: []string{"a"}}}},
		{name: "the pool", desired: []ydbschema.ColumnFamily{{Name: "cold", Data: "HDD", Columns: []string{"a"}}}},
		{name: "compression off", desired: []ydbschema.ColumnFamily{{Name: "cold", Compression: "off", Columns: []string{"a"}}}},
		{name: "the regular cache", desired: []ydbschema.ColumnFamily{{Name: "cold", CacheMode: "regular", Columns: []string{"a"}}}},
		{name: "a column moved out", desired: []ydbschema.ColumnFamily{{Name: "cold"}}},
		{name: "a column moved in", desired: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a", "b"}}}},
		{name: "a column left in no family", desired: nil},
		{name: "a family the table lacks", desired: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}, {Name: "warm"}}},
		{name: "the default family's compression",
			desired: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}, {Name: "default", Compression: "off"}}},
		{name: "keep_in_memory the table lacks",
			desired: []ydbschema.ColumnFamily{{Name: "cold", KeepInMemory: true, Columns: []string{"a"}}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbfamily.Satisfied(test.desired, held), qt.IsFalse)
		})
	}
}

// What a table holds once a declaration is applied keeps every family it
// held and each setting the declaration does not state, takes each setting
// the declaration states, and puts each column where the declaration does.
func TestApplied(t *testing.T) {
	c := qt.New(t)
	held := []ydbschema.ColumnFamily{
		{Name: "default", Compression: "lz4", CacheMode: "in_memory", KeepInMemory: true},
		{Name: "extra", Compression: "lz4"},
		{Name: "cold", Data: "hdd", Compression: "off", Columns: []string{"a", "b"}},
	}
	desired := []ydbschema.ColumnFamily{
		{Name: "cold", Compression: "LZ4", Columns: []string{"b"}},
		{Name: "default", Compression: "off"},
		{Name: "hot", CacheMode: "in_memory", Columns: []string{"c"}},
	}

	got := ydbfamily.Applied(desired, held)

	c.Assert(got, qt.DeepEquals, []ydbschema.ColumnFamily{
		{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"b"}},
		{Name: "default", Compression: "off", CacheMode: "in_memory", KeepInMemory: true},
		{Name: "extra", Compression: "lz4"},
		{Name: "hot", CacheMode: "in_memory", Columns: []string{"c"}},
	})
	c.Assert(ydbfamily.Satisfied(desired, got), qt.IsTrue)
	c.Assert(ydbfamily.Satisfied(got, held), qt.IsFalse)
	c.Assert(held[2].Columns, qt.DeepEquals, []string{"a", "b"}, qt.Commentf("held must not be changed"))
	c.Assert(desired[0].Compression, qt.Equals, "LZ4", qt.Commentf("desired must not be changed"))
}

// A CREATE TABLE declares each family with the settings it states, `off` and
// `regular` included, an empty list for one stating none, and the default
// family only where it states a setting. A column joins a family by naming it
// after its type, and joins the default family by naming none.
func TestCreateEntries(t *testing.T) {
	c := qt.New(t)
	families := []ydbschema.ColumnFamily{
		{Name: "warm", Columns: []string{"b"}},
		{Name: "default"},
		{Name: "cold", Data: "hdd", Compression: "LZ4", CacheMode: "in_memory", Columns: []string{"a"}},
		{Name: "plain", Compression: "off", CacheMode: "regular"},
		{Name: "it's", Data: `o'k\`},
	}

	c.Assert(ydbfamily.CreateEntries(families), qt.DeepEquals, []string{
		"FAMILY `cold` (DATA = 'hdd', COMPRESSION = 'lz4', CACHE_MODE = 'in_memory')",
		"FAMILY `it's` (DATA = 'o\\'k\\\\')",
		"FAMILY `plain` (COMPRESSION = 'off', CACHE_MODE = 'regular')",
		"FAMILY `warm` ()",
	})
	c.Assert(ydbfamily.CreateEntries([]ydbschema.ColumnFamily{{Name: "default", Compression: "off"}}), qt.DeepEquals,
		[]string{"FAMILY `default` (COMPRESSION = 'off')"})
	c.Assert(ydbfamily.FamilyOf(families, "a"), qt.Equals, "cold")
	c.Assert(ydbfamily.FamilyOf(families, "id"), qt.Equals, "default")
	c.Assert(ydbfamily.ColumnClause("cold"), qt.Equals, " FAMILY `cold`")
	c.Assert(ydbfamily.ColumnClause("default"), qt.Equals, "")
	c.Assert(ydbfamily.ColumnClause(""), qt.Equals, "")
}

// A CREATE TABLE writes every family stating no keep_in_memory.
func TestCreateRefusal_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbfamily.CreateRefusal([]ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}, {Name: "cold"}}),
		qt.Equals, "")
}

// keep_in_memory is no family setting YQL takes, so a CREATE TABLE that has
// to carry it is refused, the default family's too.
func TestCreateRefusal_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbfamily.CreateRefusal([]ydbschema.ColumnFamily{{Name: "cold"}, {Name: "default", KeepInMemory: true}}),
		qt.Equals, `column family "default" keeps its columns in memory (keep_in_memory), and YQL has no family `+
			"setting for it (`Unknown table setting: KEEP_IN_MEMORY`), so the new table would not keep them there")
}

// The one ALTER TABLE a change takes adds a family before any column moves
// into it, sets only the settings the declaration states and the table holds
// otherwise -- setting one resets no other -- and moves only the columns
// whose family differs. A setting or a family the declaration leaves out is
// not written: the table keeps what it holds.
func TestAlterActions(t *testing.T) {
	tests := []struct {
		name             string
		desired, current []ydbschema.ColumnFamily
		want             []string
	}{
		{
			name:    "a new family and a column moved into it",
			desired: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"b", "a"}}},
			current: nil,
			want: []string{
				"ADD FAMILY `cold` (DATA = 'hdd', COMPRESSION = 'lz4')",
				"ALTER COLUMN `a` SET FAMILY `cold`",
				"ALTER COLUMN `b` SET FAMILY `cold`",
			},
		},
		{
			name:    "one setting changed",
			desired: []ydbschema.ColumnFamily{{Name: "cold", Data: "ssd", Compression: "lz4", Columns: []string{"a"}}},
			current: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"a"}}},
			want:    []string{"ALTER FAMILY `cold` SET DATA 'ssd'"},
		},
		{
			name:    "settings stated as YDB's own",
			desired: []ydbschema.ColumnFamily{{Name: "cold", Compression: "off", CacheMode: "regular"}},
			current: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "lz4", CacheMode: "in_memory"}},
			want: []string{
				"ALTER FAMILY `cold` SET COMPRESSION 'off'",
				"ALTER FAMILY `cold` SET CACHE_MODE 'regular'",
			},
		},
		{
			name:    "settings left out",
			desired: []ydbschema.ColumnFamily{{Name: "cold"}},
			current: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "lz4", CacheMode: "in_memory"}},
			want:    nil,
		},
		{
			name:    "the default family declared for the first time",
			desired: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}},
			current: nil,
			want:    []string{"ALTER FAMILY `default` SET COMPRESSION 'lz4'"},
		},
		{
			name:    "the default family and another left out",
			desired: nil,
			current: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", CacheMode: "in_memory"}, {Name: "extra"}},
			want:    nil,
		},
		{
			name:    "columns moved between families and back to the default one",
			desired: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"b"}}, {Name: "warm", Columns: []string{"c"}}},
			current: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a", "c"}}, {Name: "warm", Columns: []string{"b"}}},
			want: []string{
				"ALTER COLUMN `a` SET FAMILY `default`",
				"ALTER COLUMN `b` SET FAMILY `cold`",
				"ALTER COLUMN `c` SET FAMILY `warm`",
			},
		},
		{
			name:    "a column out of a family the declaration leaves out",
			desired: nil,
			current: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}},
			want:    []string{"ALTER COLUMN `a` SET FAMILY `default`"},
		},
		{
			name:    "nothing to do",
			desired: []ydbschema.ColumnFamily{{Name: "cold", Compression: "LZ4", Columns: []string{"a"}}},
			current: []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"a"}}},
			want:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbfamily.AlterActions(test.desired, test.current), qt.DeepEquals, test.want)
		})
	}
}

// A family left out, a pool left out and a keep_in_memory the table holds
// and the declaration does not state are kept, not refused.
func TestChangeRefusal_HappyPath(t *testing.T) {
	tests := []struct {
		name             string
		desired, current []ydbschema.ColumnFamily
	}{
		{name: "a new family", desired: []ydbschema.ColumnFamily{{Name: "cold"}}, current: nil},
		{name: "a family left out", desired: nil,
			current: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}}},
		{name: "a pool left out", desired: []ydbschema.ColumnFamily{{Name: "cold"}},
			current: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd"}}},
		{name: "keep_in_memory held", desired: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}},
			current: []ydbschema.ColumnFamily{{Name: "default", KeepInMemory: true}}},
		{name: "keep_in_memory on both sides", desired: []ydbschema.ColumnFamily{{Name: "cold", KeepInMemory: true}},
			current: []ydbschema.ColumnFamily{{Name: "cold", KeepInMemory: true}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbfamily.ChangeRefusal(test.desired, test.current), qt.Equals, "")
		})
	}
}

// keep_in_memory is no setting YQL writes, so a declaration stating it for a
// family that does not keep its columns in memory, or that the table lacks,
// is refused.
func TestChangeRefusal_FailurePath(t *testing.T) {
	tests := []struct {
		name             string
		desired, current []ydbschema.ColumnFamily
	}{
		{name: "a held family", desired: []ydbschema.ColumnFamily{{Name: "cold", KeepInMemory: true}},
			current: []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4"}}},
		{name: "a new family", desired: []ydbschema.ColumnFamily{{Name: "cold", KeepInMemory: true}}, current: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbfamily.ChangeRefusal(test.desired, test.current), qt.Equals,
				`column family "cold" keeps its columns in memory (keep_in_memory) on one side only, and YQL has no `+
					"family setting for it (`Unknown table setting: KEEP_IN_MEMORY`)")
		})
	}
}

// Leaving columns out keeps every family and each other column, and changes
// nothing it was given.
func TestWithoutColumns(t *testing.T) {
	c := qt.New(t)
	families := []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a", "b"}}, {Name: "warm", Columns: []string{"c"}}}

	c.Assert(ydbfamily.WithoutColumns(families, []string{"a", "c"}), qt.DeepEquals,
		[]ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"b"}}, {Name: "warm"}})
	c.Assert(ydbfamily.OnlyColumns(families, func(column string) bool { return column == "c" }), qt.DeepEquals,
		[]ydbschema.ColumnFamily{{Name: "cold"}, {Name: "warm", Columns: []string{"c"}}})
	c.Assert(families[0].Columns, qt.DeepEquals, []string{"a", "b"})
}

// A family is plain when it holds what YDB gives a family stating nothing,
// in any case; a pool, a compression, a cache mode or keep_in_memory each
// make it stated. Only the default family is left out for being plain.
func TestStated(t *testing.T) {
	c := qt.New(t)
	families := []ydbschema.ColumnFamily{
		{Name: "default", Compression: "OFF", CacheMode: "Regular"},
		{Name: "spare", Compression: "off"},
	}
	c.Assert(ydbfamily.Stated(families), qt.DeepEquals, []ydbschema.ColumnFamily{{Name: "spare", Compression: "off"}})
	for _, stated := range []ydbschema.ColumnFamily{
		{Name: "default", Data: "hdd"},
		{Name: "default", Compression: "lz4"},
		{Name: "default", CacheMode: "in_memory"},
		{Name: "default", KeepInMemory: true},
	} {
		c.Assert(ydbfamily.Plain(stated), qt.IsFalse, qt.Commentf("%+v", stated))
		c.Assert(ydbfamily.Stated([]ydbschema.ColumnFamily{stated}), qt.HasLen, 1, qt.Commentf("%+v", stated))
	}
}
