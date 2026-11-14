package lint_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// sequenceDefault is a column of the starting state of version 2 whose
// default calls nextval on the given sequences, spelled as PostgreSQL prints
// the default.
func sequenceDefault(table, column string, sequences ...string) lintext.BaselineColumn {
	calls := make([]string, 0, len(sequences))
	for _, sequence := range sequences {
		calls = append(calls, "nextval('"+sequence+"'::regclass)")
	}
	return lintext.BaselineColumn{Version: 2, Table: table, Name: column, HasDefault: true, Default: strings.Join(calls, " + ")}
}

// cascadeChanges lints a directory whose second file is drop, under the
// community count on PostgreSQL, and returns the changes that file counts.
func cascadeChanges(c *qt.C, drop string, count lint.ChangeCount, columns []lintext.BaselineColumn, scope string) []changeProjection {
	c.Helper()
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_init.up.sql": "CREATE SEQUENCE s;\n",
		"0000000002_drop.up.sql": drop + "\n",
	}), lint.Options{Dialect: "postgres", ChangeCount: count, Baseline: columns, BaselineRead: []lintext.BaselineRead{{Version: 2}}, SchemaScope: scope})
	c.Assert(err, qt.IsNil)
	return projectChanges(fileByName(c, analysis, "0000000002_drop.up.sql").Changes)
}

// A DROP SEQUENCE ... CASCADE changes every table whose column defaults name
// the sequence, one change per table and statement, and the community count
// counts those changes and not the sequence. Each row's count is the one the
// pinned community binary v1.3.0 reports on PostgreSQL 18 for the same
// directory, except where the row says the count is Ptah's own.
func TestAnalyzeFS_ChangeCountOfASequenceCascade(t *testing.T) {
	tests := []struct {
		name    string
		drop    string
		columns []lintext.BaselineColumn
		scope   string
		want    []changeProjection
	}{
		{
			name: "one table", drop: "DROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")},
			want:    []changeProjection{{Kind: lint.SchemaChangeModify, Object: "t"}},
		},
		{
			name: "two tables", drop: "DROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t1", "id", "s"), sequenceDefault("t2", "id", "s")},
			want: []changeProjection{
				{Kind: lint.SchemaChangeModify, Object: "t1"},
				{Kind: lint.SchemaChangeModify, Object: "t2"},
			},
		},
		{
			name: "two columns of one table", drop: "DROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "a", "s"), sequenceDefault("t", "b", "s")},
			want:    []changeProjection{{Kind: lint.SchemaChangeModify, Object: "t"}},
		},
		{
			name: "no default names the sequence", drop: "DROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{{Version: 2, Table: "t", Name: "id"}},
			want:    make([]changeProjection, 0),
		},
		{
			name: "a default naming another sequence", drop: "DROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "other")},
			want:    make([]changeProjection, 0),
		},
		{
			name: "IF EXISTS", drop: "DROP SEQUENCE IF EXISTS s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")},
			want:    []changeProjection{{Kind: lint.SchemaChangeModify, Object: "t"}},
		},
		{
			name: "without CASCADE, which the engine refuses; Ptah's own count", drop: "DROP SEQUENCE s;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")},
			want:    make([]changeProjection, 0),
		},
		{
			name: "two sequences of two tables in one statement", drop: "DROP SEQUENCE s1, s2 CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t1", "id", "s1"), sequenceDefault("t2", "id", "s2")},
			want: []changeProjection{
				{Kind: lint.SchemaChangeModify, Object: "t1"},
				{Kind: lint.SchemaChangeModify, Object: "t2"},
			},
		},
		{
			name: "two sequences of one table in one statement", drop: "DROP SEQUENCE s1, s2 CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "a", "s1"), sequenceDefault("t", "b", "s2")},
			want:    []changeProjection{{Kind: lint.SchemaChangeModify, Object: "t"}},
		},
		{
			name: "two sequences of one table in two statements", drop: "DROP SEQUENCE s1 CASCADE;\nDROP SEQUENCE s2 CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "a", "s1"), sequenceDefault("t", "b", "s2")},
			want: []changeProjection{
				{Kind: lint.SchemaChangeModify, Object: "t"},
				{Kind: lint.SchemaChangeModify, Object: "t"},
			},
		},
		{
			name: "two sequences of one default in two statements", drop: "DROP SEQUENCE s1 CASCADE;\nDROP SEQUENCE s2 CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s1", "s2")},
			want:    []changeProjection{{Kind: lint.SchemaChangeModify, Object: "t"}},
		},
		{
			name: "a default an earlier statement drops", drop: "ALTER TABLE t ALTER COLUMN id DROP DEFAULT;\nDROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")},
			want:    []changeProjection{{Kind: lint.SchemaChangeModify, Object: "t"}},
		},
		{
			// The community binary counts 2: the cascade changes t again.
			name: "a table an earlier statement changes; Ptah's own count", drop: "ALTER TABLE t ADD COLUMN x int;\nDROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")},
			want:    []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "x"}},
		},
		{
			name:    "a sequence the file renamed away and created again; Ptah's own count",
			drop:    "ALTER SEQUENCE s RENAME TO s_old;\nCREATE SEQUENCE s;\nDROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")},
			want:    make([]changeProjection, 0),
		},
		{
			name:    "after a cascade that may have changed the table; Ptah's own count",
			drop:    "DROP TYPE mood CASCADE;\nDROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")},
			want:    []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "mood"}},
		},
		{
			name: "a qualified drop of a bare constant, qualified by the reviewed schema", drop: "DROP SEQUENCE public.s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")}, scope: "public",
			want: []changeProjection{{Kind: lint.SchemaChangeModify, Object: "t"}},
		},
		{
			// The community binary counts 1: without a reviewed schema
			// nothing says which schema the search path found the bare
			// constant in, so the drop is not matched to it.
			name: "a qualified drop of a bare constant with no reviewed schema; Ptah's own count", drop: "DROP SEQUENCE public.s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")},
			want:    make([]changeProjection, 0),
		},
		{
			name: "a drop qualified by another schema of a bare constant", drop: "DROP SEQUENCE app.s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")}, scope: "public",
			want: make([]changeProjection, 0),
		},
		{
			name: "a qualified drop of a qualified constant", drop: "DROP SEQUENCE app.s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "app.s")}, scope: "public",
			want: []changeProjection{{Kind: lint.SchemaChangeModify, Object: "t"}},
		},
		{
			name: "a bare drop of a qualified constant", drop: "DROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "app.s")}, scope: "public",
			want: make([]changeProjection, 0),
		},
		{
			name: "a quoted drop", drop: `DROP SEQUENCE "s" CASCADE;`,
			columns: []lintext.BaselineColumn{sequenceDefault("t", "id", "s")},
			want:    []changeProjection{{Kind: lint.SchemaChangeModify, Object: "t"}},
		},
		{
			name: "a table of a state that names its schema", drop: "DROP SEQUENCE s CASCADE;",
			columns: []lintext.BaselineColumn{{Version: 2, Schema: "public", Table: "t", Name: "id", HasDefault: true, Default: "nextval('s'::regclass)"}},
			want:    []changeProjection{{Kind: lint.SchemaChangeModify, Object: "public.t"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := cascadeChanges(c, test.drop, lint.ChangeCountAtlasCommunity, test.columns, test.scope)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// The default Atlas count counts the sequence and the tables it cascades to,
// and the statement count counts the sequence alone: it reads the changes the
// statement's words express. A sequence outside the reviewed schema counts
// nothing itself, and the Atlas count still counts the table it cascades to,
// as the pinned community binary v1.3.0 counts 1 on PostgreSQL 18 under a
// search_path of public.
func TestAnalyzeFS_SequenceCascadeUnderTheOtherCounts(t *testing.T) {
	tests := []struct {
		name     string
		count    lint.ChangeCount
		drop     string
		sequence string
		scope    string
		want     []changeProjection
	}{
		{
			name: "the Atlas count", count: lint.ChangeCountAtlas, drop: "DROP SEQUENCE s CASCADE;", sequence: "s",
			want: []changeProjection{
				{Kind: lint.SchemaChangeDrop, Object: "s"},
				{Kind: lint.SchemaChangeModify, Object: "t"},
			},
		},
		{
			name: "the statement count", count: lint.ChangeCountStatements, drop: "DROP SEQUENCE s CASCADE;", sequence: "s",
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "s"}},
		},
		{
			name: "the Atlas count of a sequence outside the scope", count: lint.ChangeCountAtlas,
			drop: "DROP SEQUENCE app.s CASCADE;", sequence: "app.s", scope: "public",
			want: []changeProjection{{Kind: lint.SchemaChangeModify, Object: "t"}},
		},
		{
			name: "the statement count of a sequence outside the scope", count: lint.ChangeCountStatements,
			drop: "DROP SEQUENCE app.s CASCADE;", sequence: "app.s", scope: "public",
			want: make([]changeProjection, 0),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			columns := []lintext.BaselineColumn{sequenceDefault("t", "id", test.sequence)}
			c.Assert(cascadeChanges(c, test.drop, test.count, columns, test.scope), qt.DeepEquals, test.want)
		})
	}
}

// Without the starting state the cascade counts the sequence alone, and the
// Atlas counts ask for that state.
func TestAnalyzeFS_SequenceCascadeWithoutAStartingState(t *testing.T) {
	c := qt.New(t)
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_init.up.sql": "CREATE SEQUENCE s;\n",
		"0000000002_drop.up.sql": "DROP SEQUENCE s CASCADE;\n",
	}), lint.Options{Dialect: "postgres", ChangeCount: lint.ChangeCountAtlas})
	c.Assert(err, qt.IsNil)
	c.Assert(projectChanges(fileByName(c, analysis, "0000000002_drop.up.sql").Changes), qt.DeepEquals, []changeProjection{
		{Kind: lint.SchemaChangeDrop, Object: "s"},
	})
	c.Assert(analysis.BaselineVersions(), qt.DeepEquals, []int64{2})
}

// A count asks for the state of a version whose sequence drop cascades, and
// only where no earlier statement of the file names the sequence.
func TestAnalysis_BaselineVersionsOfASequenceDrop(t *testing.T) {
	tests := []struct {
		name  string
		count lint.ChangeCount
		drop  string
		want  []int64
	}{
		{name: "a cascade under the community count", count: lint.ChangeCountAtlasCommunity, drop: "DROP SEQUENCE s CASCADE;", want: []int64{2}},
		{name: "a cascade under the statement count", count: lint.ChangeCountStatements, drop: "DROP SEQUENCE s CASCADE;", want: nil},
		{name: "a drop without CASCADE", count: lint.ChangeCountAtlasCommunity, drop: "DROP SEQUENCE s;", want: nil},
		{
			name: "a cascade of a sequence the file created", count: lint.ChangeCountAtlasCommunity,
			drop: "CREATE SEQUENCE s2;\nDROP SEQUENCE s2 CASCADE;", want: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_init.up.sql": "CREATE SEQUENCE s;\n",
				"0000000002_drop.up.sql": test.drop + "\n",
			}), lint.Options{Dialect: "postgres", ChangeCount: test.count})
			c.Assert(err, qt.IsNil)
			c.Assert(analysis.BaselineVersions(), qt.DeepEquals, test.want)
		})
	}
}
