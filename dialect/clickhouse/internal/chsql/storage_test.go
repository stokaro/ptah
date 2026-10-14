package chsql_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chsql"
)

func TestSortingKey_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, before, after, primary string
		want                         chsql.SortingKeyChange
	}{
		{name: "append", before: "a", after: "(a, k)", primary: "a", want: chsql.SortingKeyChange{Added: []string{"k"}}},
		{name: "append an expression", before: "a, u", after: "a, u, lower(s)", primary: "a", want: chsql.SortingKeyChange{Added: []string{"lower(s)"}}},
		{name: "insert after the primary key", before: "a, u", after: "a, k, u", primary: "a", want: chsql.SortingKeyChange{Added: []string{"k"}}},
		{name: "drop the last", before: "(a, u, b)", after: "a", primary: "a", want: chsql.SortingKeyChange{Dropped: 2}},
		{name: "from an empty key", before: "tuple()", after: "k", want: chsql.SortingKeyChange{Added: []string{"k"}}},
		{name: "a comma inside an expression", before: "a", after: "a, concat(s, ',')", primary: "a", want: chsql.SortingKeyChange{Added: []string{"concat(s, ',')"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := chsql.SortingKey(test.before, test.after, test.primary)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestSortingKey_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, before, after, primary, want string
	}{
		{name: "reordered", before: "a, u", after: "u, a", want: "u is already in the key a, u"},
		{name: "a middle expression dropped", before: "a, u, b", after: "a, b", primary: "a", want: "b is already in the key"},
		{name: "added before the primary key ends", before: "a, u", after: "a, k, u", primary: "a, u", want: "the primary key a, u a prefix of the sorting key"},
		{name: "primary key expression dropped", before: "a, u", after: "a", primary: "a, u", want: "a prefix of the sorting key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := chsql.SortingKey(test.before, test.after, test.primary)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want)+".*")
			c.Assert(got, qt.DeepEquals, chsql.SortingKeyChange{})
		})
	}
}

func TestSampleKey_HappyPath(t *testing.T) {
	for _, test := range []struct{ name, sample, primary string }{
		{name: "a primary key expression", sample: "intHash32(a)", primary: "a, intHash32(a)"},
		{name: "parenthesized", sample: "(u)", primary: "(a, u)"},
		{name: "removed", sample: "", primary: "a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chsql.SampleKey(test.sample, test.primary), qt.IsNil)
		})
	}
}

func TestSampleKey_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, sample, primary string }{
		{name: "a column only the sorting key holds", sample: "u", primary: "a"},
		{name: "part of a primary key expression", sample: "a", primary: "intHash32(a)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := chsql.SampleKey(test.sample, test.primary)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, ".*requires the sampling key "+regexp.QuoteMeta(test.sample)+" to be an expression of the primary key.*")
		})
	}
}

func TestSettingsChange_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, before, after string
		set                 []chsql.Setting
		reset               []string
	}{
		{name: "set, change and reset", before: "a = 1, b = 2, index_granularity = 8192", after: "b = 3, c = 'x,y'",
			set: []chsql.Setting{{Name: "b", Value: "3"}, {Name: "c", Value: "'x,y'"}}, reset: []string{"a"}},
		{name: "reordered", before: "a = 1, b = 2", after: "b=2, a=1"},
		{name: "index granularity only the current clause names", before: "index_granularity = 8192", after: ""},
		{name: "an equal fixed setting", before: "table_disk = 1, a = 1", after: "table_disk = 1, a = 2", set: []chsql.Setting{{Name: "a", Value: "2"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, reset, err := chsql.SettingsChange(test.before, test.after)
			c.Assert(err, qt.IsNil)
			c.Assert(set, qt.DeepEquals, test.set)
			c.Assert(reset, qt.DeepEquals, test.reset)
		})
	}
}

func TestSettingsChange_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, before, after string
		sentinel            error
		want                string
	}{
		{name: "fixed setting changed", before: "index_granularity = 8192", after: "index_granularity = 4096", sentinel: ptaherr.ErrUnsupportedFeature, want: "fixes table setting index_granularity"},
		{name: "fixed setting reset", before: "table_disk = 1", after: "", sentinel: ptaherr.ErrUnsupportedFeature, want: "fixes table setting table_disk"},
		{name: "fixed setting set", before: "", after: "index_granularity_bytes = 0", sentinel: ptaherr.ErrUnsupportedFeature, want: "fixes table setting index_granularity_bytes"},
		{name: "index granularity only the declaration names", before: "a = 1", after: "a = 1, index_granularity = 8192", sentinel: ptaherr.ErrUnsupportedFeature, want: "fixes table setting index_granularity"},
		{name: "name written twice", before: "", after: "a = 1, a = 2", sentinel: schemaext.ErrInvalidValue, want: "names a twice"},
		{name: "not an assignment", before: "a 1", after: "", sentinel: schemaext.ErrInvalidValue, want: "is not `name = value`"},
		{name: "empty entry", before: "a = 1,, b = 2", after: "", sentinel: schemaext.ErrInvalidValue, want: "is not `name = value`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, reset, err := chsql.SettingsChange(test.before, test.after)
			c.Assert(err, qt.ErrorIs, test.sentinel)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want)+".*")
			c.Assert(set, qt.IsNil)
			c.Assert(reset, qt.IsNil)
		})
	}
}

func TestKeyExpression(t *testing.T) {
	for _, test := range []struct{ key, want string }{
		{key: "", want: "tuple()"},
		{key: "tuple()", want: "tuple()"},
		{key: "a", want: "a"},
		{key: "a, b", want: "(a, b)"},
		{key: " (a, b) ", want: "(a, b)"},
		{key: "lower(s)", want: "lower(s)"},
	} {
		t.Run(test.key, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chsql.KeyExpression(test.key), qt.Equals, test.want)
		})
	}
}

func TestMergesByKey(t *testing.T) {
	for _, test := range []struct {
		engine string
		want   bool
	}{
		{engine: "MergeTree", want: false},
		{engine: "ReplicatedMergeTree('/t/{shard}', '{replica}')", want: false},
		{engine: "SharedMergeTree", want: false},
		{engine: "ReplacingMergeTree(version)", want: true},
		{engine: "ReplicatedSummingMergeTree('/t', 'r')", want: true},
		{engine: "Log", want: false},
	} {
		t.Run(test.engine, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chsql.MergesByKey(test.engine), qt.Equals, test.want)
		})
	}
}

// ValidateStorageChange refuses a clause by name before any stage could
// acknowledge it, and needs a change to validate.
func TestValidateStorageChange_FailurePath(t *testing.T) {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a", PrimaryKey: "a", Settings: "index_granularity = 8192"}
	for _, test := range []struct {
		name     string
		after    chschema.ObservedTable
		sentinel error
		want     string
	}{
		{name: "no change", after: chschema.ObservedTable{Engine: "MergeTree", OrderBy: "(a)", PrimaryKey: "a"}, sentinel: schemaext.ErrInvalidValue, want: "contain no change"},
		{name: "engine", after: chschema.ObservedTable{Engine: "ReplacingMergeTree", OrderBy: "a", PrimaryKey: "a"}, sentinel: ptaherr.ErrUnsupportedFeature, want: "changes a table's ENGINE"},
		{name: "partition", after: chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a", PrimaryKey: "a", PartitionBy: "a"}, sentinel: ptaherr.ErrUnsupportedFeature, want: "changes a table's PARTITION BY"},
		{name: "primary key", after: chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a, b", PrimaryKey: "a, b"}, sentinel: ptaherr.ErrUnsupportedFeature, want: "changes a table's PRIMARY KEY"},
		{name: "whitespace TTL", after: chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a", PrimaryKey: "a", TTL: " "}, sentinel: schemaext.ErrInvalidValue, want: "only whitespace"},
		{name: "sampling key", after: chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a, u", PrimaryKey: "a", SampleBy: "u"}, sentinel: ptaherr.ErrUnsupportedFeature, want: "sampling key u"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := chsql.ValidateStorageChange(&chdiff.Table{Before: before, After: test.after.Desired()})
			c.Assert(err, qt.ErrorIs, test.sentinel)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want)+".*")
		})
	}
}

func TestChanged(t *testing.T) {
	c := qt.New(t)
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a", PrimaryKey: "a", TTL: "t + toIntervalDay(1)", Settings: "a = 1, index_granularity = 8192"}
	after := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "(a, k)", PrimaryKey: "a", SampleBy: "a", Settings: "a = 2"}
	c.Assert(chsql.Changed(before, after), qt.DeepEquals, []chsql.Clause{chsql.OrderBy, chsql.SampleBy, chsql.TTL, chsql.Settings})
	c.Assert(chsql.Changed(before, before), qt.HasLen, 0)
}

// A prediction keeps the index_granularity the server recorded when the
// declaration leaves it out, and the declaration's own value otherwise.
func TestPredicted(t *testing.T) {
	for _, test := range []struct{ name, current, declared, want string }{
		{name: "left out", current: "a = 1, index_granularity = 8192", declared: "b = 2", want: "b = 2, index_granularity = 8192"},
		{name: "no settings declared", current: "index_granularity = 4096", declared: "", want: "index_granularity = 4096"},
		{name: "declared", current: "index_granularity = 8192", declared: "index_granularity = 4096", want: "index_granularity = 4096"},
		{name: "none recorded", current: "a = 1", declared: "b = 2", want: "b = 2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a", Settings: test.current}
			declared := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "(a, b)", Settings: test.declared}
			got := chsql.Predicted(current, declared)
			c.Assert(got, qt.DeepEquals, &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "(a, b)", Settings: test.want})
			c.Assert(declared.Settings, qt.Equals, test.declared)
		})
	}
}
