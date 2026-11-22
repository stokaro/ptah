package clickhouse

// White-box testing required: the element parse is package-local, and a read
// through the exported reader needs a server for every fixture below, which
// are create_table_query values ClickHouse 26.10 printed, measured.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

func TestParseTableElements_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name            string
		createQuery     string
		wantProjections []catalog.Projection
		wantClauses     map[string][]string
	}{
		{
			name: "every projection form and codecs",
			createQuery: "CREATE TABLE rv5.c (`id` UInt64, `name` String CODEC(ZSTD(3)), `v` UInt64 CODEC(Delta(4), ZSTD(1)), " +
				"PROJECTION p_agg (SELECT name, count(), sum(v) GROUP BY name), PROJECTION `p q` (SELECT id, name ORDER BY name), " +
				"PROJECTION p_s (SELECT * ORDER BY name) WITH SETTINGS (index_granularity = 128), PROJECTION p_i INDEX name TYPE basic) " +
				"ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
			wantProjections: []catalog.Projection{
				{Name: "p_agg", Definition: "(SELECT name, count(), sum(v) GROUP BY name)"},
				{Name: "p q", Definition: "(SELECT id, name ORDER BY name)"},
				{Name: "p_s", Definition: "(SELECT * ORDER BY name) WITH SETTINGS (index_granularity = 128)"},
				{Name: "p_i", Definition: "INDEX name TYPE basic"},
			},
			wantClauses: map[string][]string{"name": {"CODEC(ZSTD(3))"}, "v": {"CODEC(Delta(4), ZSTD(1))"}},
		},
		{
			name: "column TTL, statistics and settings",
			createQuery: "CREATE TABLE rv5.x (`id` UInt64, `d` DateTime, `s` String TTL d + toIntervalDay(1), `n` UInt32 STATISTICS(tdigest), " +
				"`m` String SETTINGS (max_compress_block_size = 1048576)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
			wantClauses: map[string][]string{
				"s": {"TTL d + toIntervalDay(1)"}, "n": {"STATISTICS(tdigest)"}, "m": {"SETTINGS (max_compress_block_size = 1048576)"},
			},
		},
		{
			name: "keywords inside a literal, a default and a lower-case column named ttl",
			createQuery: "CREATE TABLE rv5.lit (`id` UInt64 DEFAULT 7 COMMENT 'a, (b) TTL CODEC(' CODEC(Delta(8), LZ4), `ttl` DateTime, " +
				"`s` String DEFAULT concat('x', 'y') CODEC(ZSTD(1)) TTL ttl + toIntervalDay(1), `settings` UInt8 MATERIALIZED 1, " +
				"INDEX i s TYPE bloom_filter GRANULARITY 1, CONSTRAINT c CHECK id > 0) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
			wantClauses: map[string][]string{
				"id": {"CODEC(Delta(8), LZ4)"}, "s": {"CODEC(ZSTD(1))", "TTL ttl + toIntervalDay(1)"},
			},
		},
		{
			name: "names the server escapes and a flattened Nested column",
			createQuery: "CREATE TABLE rv5.weird (`sl\\\\x` UInt64 CODEC(ZSTD(1)), `a\\`b` String, `new\\nline` UInt8, `é` UInt8, " +
				"`n.a` Array(UInt8), `n.b` Array(String), PROJECTION `p\\`q` (SELECT * ORDER BY `a\\`b`)) " +
				"ENGINE = MergeTree ORDER BY `sl\\\\x` SETTINGS index_granularity = 8192",
			wantProjections: []catalog.Projection{{Name: "p`q", Definition: "(SELECT * ORDER BY `a\\`b`)"}},
			wantClauses:     map[string][]string{`sl\x`: {"CODEC(ZSTD(1))"}},
		},
		{
			name:        "a Log table",
			createQuery: "CREATE TABLE rv2.t_Log (`id` UInt64) ENGINE = Log",
			wantClauses: make(map[string][]string),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := parseTableElements(test.createQuery)
			c.Assert(err, qt.IsNil)
			c.Assert(got.projections, qt.DeepEquals, test.wantProjections)
			c.Assert(got.clauses, qt.DeepEquals, test.wantClauses)
		})
	}
}

func TestParseTableElements_ReadsConstraints(t *testing.T) {
	for _, test := range []struct {
		name            string
		createQuery     string
		wantChecks      []catalog.Constraint
		wantAssumptions []catalog.Assumption
	}{
		{
			name: "CHECK and ASSUME as the server formats them",
			createQuery: "CREATE TABLE a6336c.m (`id` UInt64, `v` Int32, `s` String, CONSTRAINT c1 CHECK (v > 0) AND (v != 5), " +
				"CONSTRAINT a1 ASSUME length(s) < 10, CONSTRAINT c2 CHECK (s IN ('a', 'b')) OR (s LIKE 'x%')) " +
				"ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
			wantChecks: []catalog.Constraint{
				{Name: "c1", Type: "CHECK", CheckClause: new("(v > 0) AND (v != 5)")},
				{Name: "c2", Type: "CHECK", CheckClause: new("(s IN ('a', 'b')) OR (s LIKE 'x%')")},
			},
			wantAssumptions: []catalog.Assumption{{Name: "a1", Expression: "length(s) < 10"}},
		},
		{
			name: "a name the server quotes",
			createQuery: "CREATE TABLE a6336c.d (`id` UInt64, `v` Int32, CONSTRAINT c1 CHECK v > 0, CONSTRAINT `we\\'ird` CHECK id >= 0) " +
				"ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192",
			wantChecks: []catalog.Constraint{
				{Name: "c1", Type: "CHECK", CheckClause: new("v > 0")},
				{Name: "we'ird", Type: "CHECK", CheckClause: new("id >= 0")},
			},
		},
		{
			name:        "no constraint",
			createQuery: "CREATE TABLE rv2.t_Log (`id` UInt64) ENGINE = Log",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := parseTableElements(test.createQuery)
			c.Assert(err, qt.IsNil)
			c.Assert(got.checks, qt.DeepEquals, test.wantChecks)
			c.Assert(got.assumptions, qt.DeepEquals, test.wantAssumptions)
		})
	}
}

func TestWithTableElements_NamesTheTableOfEachCheck(t *testing.T) {
	c := qt.New(t)
	table := catalog.Table{Name: "m", Columns: []catalog.Column{{Name: "id"}, {Name: "v"}}}
	checks, err := withTableElements(&table, "CREATE TABLE a6336c.m (`id` UInt64, `v` Int32, CONSTRAINT c1 CHECK v > 0, "+
		"CONSTRAINT a1 ASSUME v < 10) ENGINE = MergeTree ORDER BY id")
	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.DeepEquals, []catalog.Constraint{{Name: "c1", TableName: "m", Type: "CHECK", CheckClause: new("v > 0")}})
	c.Assert(table.Assumptions, qt.DeepEquals, []catalog.Assumption{{Name: "a1", Expression: "v < 10"}})
}

func TestParseTableElements_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, createQuery, wantErr string
	}{
		{name: "no element list", createQuery: "CREATE TABLE t ENGINE = Memory", wantErr: ".*it has no element list"},
		{name: "unclosed list", createQuery: "CREATE TABLE t (`id` UInt64", wantErr: ".*the element list does not close"},
		{name: "empty element", createQuery: "CREATE TABLE t (`id` UInt64, ) ENGINE = Memory", wantErr: ".*an element is empty"},
		{name: "projection without a definition", createQuery: "CREATE TABLE t (`id` UInt64, PROJECTION p) ENGINE = MergeTree ORDER BY id", wantErr: ".*a projection has no name and definition"},
		{name: "malformed column name", createQuery: "CREATE TABLE t (`id\\x4` UInt64) ENGINE = Memory", wantErr: ".*column name .*"},
		{name: "constraint without an expression", createQuery: "CREATE TABLE t (`id` UInt64, CONSTRAINT c CHECK) ENGINE = Memory", wantErr: ".*a constraint has no name, kind and expression"},
		{name: "constraint of another kind", createQuery: "CREATE TABLE t (`id` UInt64, CONSTRAINT c UNIQUE (id)) ENGINE = Memory", wantErr: ".*constraint c is UNIQUE, neither CHECK nor ASSUME"},
		{name: "malformed constraint name", createQuery: "CREATE TABLE t (`id` UInt64, CONSTRAINT `c\\x4` CHECK id > 0) ENGINE = Memory", wantErr: ".*constraint name .*"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := parseTableElements(test.createQuery)
			c.Assert(err, qt.ErrorIs, errTableElements)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got.projections, qt.IsNil)
			c.Assert(got.checks, qt.IsNil)
			c.Assert(got.clauses, qt.IsNil)
		})
	}
}
