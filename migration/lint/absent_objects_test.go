package lint_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// The counts and findings below are the ones the pinned community binary
// v1.3.0 reports for the same directory under `migrate lint --dev-url ...
// --latest 1`, where the binary reads the kind of object: an enum type on
// PostgreSQL, and a table's columns. It reads no view, trigger or sequence, so
// the Atlas count of a change to one of them is Ptah's own, and so is DS107.

// headFile is the version under test, after a first version that creates users.
const headFile = "0000000002_head.up.sql"

// objectState is the starting state of the version under test.
type objectState struct {
	// columns are the columns the state holds.
	columns []lintext.BaselineColumn
	// objects are the objects it holds.
	objects []lintext.BaselineObject
	// reads lists the version as read, with the kinds its read lists in
	// full; nil for a run without a dev database.
	reads []lintext.BaselineRead
}

// options are the lint options that hand the state over.
func (s objectState) options(dialect, scope string, count lint.ChangeCount) lint.Options {
	return lint.Options{
		Dialect:         dialect,
		SchemaScope:     scope,
		ChangeCount:     count,
		Baseline:        s.columns,
		BaselineObjects: s.objects,
		BaselineRead:    s.reads,
	}
}

// listing is a state holding users and objects, whose read lists the kinds.
func listing(objects []lintext.BaselineObject, kinds ...lintext.BaselineObjectKind) objectState {
	return objectState{
		columns: usersAt(""),
		objects: objects,
		reads:   []lintext.BaselineRead{{Version: 2, Listed: kinds}},
	}
}

// object is one object of the state version 2 starts from.
func object(kind lintext.BaselineObjectKind, table, name string) lintext.BaselineObject {
	return lintext.BaselineObject{Version: 2, Kind: kind, Table: table, Name: name}
}

// mood is an enum type holding the labels.
func mood(labels ...string) lintext.BaselineObject {
	return lintext.BaselineObject{Version: 2, Kind: lintext.BaselineType, Name: "mood", Enum: true, Values: labels}
}

// every lists every kind of object.
var every = lintext.BaselineObjectKinds()

// headChanges analyzes a directory whose second version holds statement and
// returns the changes that version counts.
func headChanges(c *qt.C, statement string, opts lint.Options) []changeProjection {
	c.Helper()
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
		headFile:                  statement + "\n",
	}), opts)
	c.Assert(err, qt.IsNil)
	return projectChanges(fileByName(c, analysis, headFile).Changes)
}

// An IF EXISTS drop of a type, a view, a materialized view, a trigger or a
// sequence that the starting state lists in full and does not hold drops
// nothing, so it counts no change. Where the state is missing, does not list
// the kind, cannot place the reference, or an earlier statement may have made
// the object, the drop is counted.
func TestIfExistsObjectDrop_Counts(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		scope     string
		statement string
		state     objectState
		want      []changeProjection
	}{
		{
			name: "an absent view", dialect: "sqlite", statement: "DROP VIEW IF EXISTS gone;",
			state: listing(nil, every...), want: make([]changeProjection, 0),
		},
		{
			name: "a present view", dialect: "sqlite", statement: "DROP VIEW IF EXISTS v;",
			state: listing([]lintext.BaselineObject{object(lintext.BaselineView, "", "v")}, every...),
			want:  []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "v"}},
		},
		{
			name: "a view without a starting state", dialect: "sqlite", statement: "DROP VIEW IF EXISTS gone;",
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "gone"}},
		},
		{
			name: "a view, from a read that does not list views", dialect: "sqlite", statement: "DROP VIEW IF EXISTS gone;",
			state: listing(nil, lintext.BaselineTrigger),
			want:  []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "gone"}},
		},
		{
			name: "a view without IF EXISTS", dialect: "sqlite", statement: "DROP VIEW gone;",
			state: listing(nil, every...), want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "gone"}},
		},
		{
			name: "a view the file created", dialect: "sqlite", statement: "CREATE VIEW gone AS SELECT 1;\nDROP VIEW IF EXISTS gone;",
			state: listing(nil, every...),
			want: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "gone"},
				{Kind: lint.SchemaChangeDrop, Object: "gone"},
			},
		},
		{
			name: "an absent trigger", dialect: "sqlite", statement: "DROP TRIGGER IF EXISTS gone;",
			state: listing(nil, every...), want: make([]changeProjection, 0),
		},
		{
			name: "an absent trigger qualified by SQLite's one schema", dialect: "sqlite", statement: "DROP TRIGGER IF EXISTS main.gone;",
			state: listing(nil, every...), want: make([]changeProjection, 0),
		},
		{
			name: "a present trigger", dialect: "sqlite", statement: "DROP TRIGGER IF EXISTS audit;",
			state: listing([]lintext.BaselineObject{object(lintext.BaselineTrigger, "users", "audit")}, every...),
			want:  []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "audit"}},
		},
		{
			name: "a trigger another table has", dialect: "postgres", statement: "DROP TRIGGER IF EXISTS audit ON users;",
			state: listing([]lintext.BaselineObject{object(lintext.BaselineTrigger, "orders", "audit")}, every...),
			want:  make([]changeProjection, 0),
		},
		{
			name: "a trigger its table has", dialect: "postgres", statement: "DROP TRIGGER IF EXISTS audit ON users;",
			state: listing([]lintext.BaselineObject{object(lintext.BaselineTrigger, "users", "audit")}, every...),
			want:  []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "audit"}},
		},
		{
			name: "a trigger on a table of a schema the state was not read from", dialect: "postgres",
			statement: "DROP TRIGGER IF EXISTS audit ON app.users;", state: listing(nil, every...),
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "audit"}},
		},
		{
			name: "a trigger on a table outside the reviewed schema", dialect: "postgres", scope: "public",
			statement: "DROP TRIGGER audit ON app.users;", want: make([]changeProjection, 0),
		},
		{
			name: "an absent type", dialect: "postgres", statement: "DROP TYPE IF EXISTS gone;",
			state: listing(nil, every...), want: make([]changeProjection, 0),
		},
		{
			name: "a present type", dialect: "postgres", statement: "DROP TYPE IF EXISTS mood;",
			state: listing([]lintext.BaselineObject{mood("a")}, every...),
			want:  []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "mood"}},
		},
		{
			name: "an absent type and a present one", dialect: "postgres", statement: "DROP TYPE IF EXISTS gone, mood;",
			state: listing([]lintext.BaselineObject{mood("a")}, every...),
			want:  []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "mood"}},
		},
		{
			name: "an absent type qualified by the reviewed schema", dialect: "postgres", scope: "public",
			statement: "DROP TYPE IF EXISTS public.gone;", state: listing(nil, every...), want: make([]changeProjection, 0),
		},
		{
			name: "an absent type after a statement that cascades", dialect: "postgres",
			statement: "DROP TABLE users CASCADE;\nDROP TYPE IF EXISTS gone;", state: listing(nil, every...),
			want: []changeProjection{
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeDrop, Object: "gone"},
			},
		},
		{
			name: "an absent materialized view", dialect: "postgres", statement: "DROP MATERIALIZED VIEW IF EXISTS gone;",
			state: listing(nil, every...), want: make([]changeProjection, 0),
		},
		{
			name: "an absent sequence qualified by the reviewed schema", dialect: "postgres", scope: "public",
			statement: "DROP SEQUENCE IF EXISTS public.gone;", state: listing(nil, every...), want: make([]changeProjection, 0),
		},
		{
			name: "a present sequence qualified by the reviewed schema", dialect: "postgres", scope: "public",
			statement: "DROP SEQUENCE IF EXISTS public.s;",
			state:     listing([]lintext.BaselineObject{object(lintext.BaselineSequence, "", "s")}, every...),
			want:      []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "public.s"}},
		},
		{
			name: "a sequence outside the reviewed schema", dialect: "postgres", scope: "public",
			statement: "DROP SEQUENCE app.s;", want: make([]changeProjection, 0),
		},
		{
			name: "an absent routine, a kind no read lists", dialect: "postgres", statement: "DROP FUNCTION IF EXISTS gone();",
			state: listing(nil, every...), want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "gone"}},
		},
		{
			name: "an absent view on MySQL", dialect: "mysql", statement: "DROP VIEW IF EXISTS gone;",
			state: listing(nil, every...), want: make([]changeProjection, 0),
		},
		{
			name: "an absent trigger on MySQL", dialect: "mysql", statement: "DROP TRIGGER IF EXISTS gone;",
			state: listing(nil, every...), want: make([]changeProjection, 0),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := headChanges(c, test.statement, test.state.options(test.dialect, test.scope, lint.ChangeCountAtlas))
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// An ALTER TYPE ... ADD VALUE or RENAME VALUE is one change to the type under
// both Atlas counts, as the community binary counts it, and an ADD VALUE IF
// NOT EXISTS of a label the type already has counts none.
func TestEnumLabelStatement_Counts(t *testing.T) {
	tests := []struct {
		name      string
		scope     string
		statement string
		state     objectState
		want      []changeProjection
	}{
		{
			name: "a label added", statement: "ALTER TYPE mood ADD VALUE 'c';",
			state: listing([]lintext.BaselineObject{mood("a", "b")}, every...),
			want:  []changeProjection{{Kind: lint.SchemaChangeModify, Object: "mood"}},
		},
		{
			name: "a label added before another", statement: "ALTER TYPE mood ADD VALUE 'c' BEFORE 'a';",
			state: listing([]lintext.BaselineObject{mood("a", "b")}, every...),
			want:  []changeProjection{{Kind: lint.SchemaChangeModify, Object: "mood"}},
		},
		{
			name: "a label added IF NOT EXISTS that the type has", statement: "ALTER TYPE mood ADD VALUE IF NOT EXISTS 'a';",
			state: listing([]lintext.BaselineObject{mood("a", "b")}, every...), want: make([]changeProjection, 0),
		},
		{
			name: "a quoted label added IF NOT EXISTS that the type has", statement: "ALTER TYPE mood ADD VALUE IF NOT EXISTS 'it''s';",
			state: listing([]lintext.BaselineObject{mood("it's")}, every...), want: make([]changeProjection, 0),
		},
		{
			name: "a label added IF NOT EXISTS that the type lacks", statement: "ALTER TYPE mood ADD VALUE IF NOT EXISTS 'c';",
			state: listing([]lintext.BaselineObject{mood("a", "b")}, every...),
			want:  []changeProjection{{Kind: lint.SchemaChangeModify, Object: "mood"}},
		},
		{
			name: "a label added IF NOT EXISTS without a starting state", statement: "ALTER TYPE mood ADD VALUE IF NOT EXISTS 'a';",
			want: []changeProjection{{Kind: lint.SchemaChangeModify, Object: "mood"}},
		},
		{
			name:      "a label added IF NOT EXISTS after a statement naming the type",
			statement: "ALTER TYPE mood ADD VALUE 'c';\nALTER TYPE mood ADD VALUE IF NOT EXISTS 'a';",
			state:     listing([]lintext.BaselineObject{mood("a", "b")}, every...),
			want: []changeProjection{
				{Kind: lint.SchemaChangeModify, Object: "mood"},
				{Kind: lint.SchemaChangeModify, Object: "mood"},
			},
		},
		{
			name: "a label added IF NOT EXISTS to a type qualified by the reviewed schema", scope: "public",
			statement: "ALTER TYPE public.mood ADD VALUE IF NOT EXISTS 'a';",
			state:     listing([]lintext.BaselineObject{mood("a", "b")}, every...), want: make([]changeProjection, 0),
		},
		{
			name: "a label added to a type outside the reviewed schema", scope: "public",
			statement: "ALTER TYPE app.mood ADD VALUE 'c';", want: make([]changeProjection, 0),
		},
		{
			name: "a label renamed", statement: "ALTER TYPE mood RENAME VALUE 'a' TO 'z';",
			state: listing([]lintext.BaselineObject{mood("a", "b")}, every...),
			want:  []changeProjection{{Kind: lint.SchemaChangeModify, Object: "mood"}},
		},
	}
	for _, test := range tests {
		for _, count := range []lint.ChangeCount{lint.ChangeCountAtlas, lint.ChangeCountAtlasCommunity} {
			t.Run(test.name+"/"+string(count), func(t *testing.T) {
				c := qt.New(t)
				got := headChanges(c, test.statement, test.state.options("postgres", test.scope, count))
				c.Assert(got, qt.DeepEquals, test.want)
			})
		}
	}
}

// pair is a composite type, which the community binary does not read.
var pair = lintext.BaselineObject{Version: 2, Kind: lintext.BaselineType, Name: "pair"}

// The community count passes over the drop of a type the starting state holds
// as anything but an enum, written IF EXISTS or not, because the binary reads
// only enum types; the Atlas count counts it, as it counts every type Ptah
// models. A type the state holds is there whether or not the read lists types
// in full. Where the state is missing, holds the type as an enum, or an
// earlier statement names the type, the drop is counted.
func TestTypeDrop_CommunityCounts(t *testing.T) {
	tests := []struct {
		name      string
		scope     string
		statement string
		state     objectState
		community []changeProjection
		atlas     []changeProjection
	}{
		{
			name: "a composite type", statement: "DROP TYPE pair;",
			state: listing([]lintext.BaselineObject{pair}, every...), community: make([]changeProjection, 0),
			atlas: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "pair"}},
		},
		{
			name: "a composite type IF EXISTS", statement: "DROP TYPE IF EXISTS pair;",
			state: listing([]lintext.BaselineObject{pair}, every...), community: make([]changeProjection, 0),
			atlas: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "pair"}},
		},
		{
			name: "a composite type qualified by the reviewed schema", scope: "public", statement: "DROP TYPE public.pair;",
			state: listing([]lintext.BaselineObject{pair}, every...), community: make([]changeProjection, 0),
			atlas: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "public.pair"}},
		},
		{
			name: "an enum type", statement: "DROP TYPE mood;",
			state:     listing([]lintext.BaselineObject{mood("a")}, every...),
			community: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "mood"}},
			atlas:     []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "mood"}},
		},
		{
			name: "an enum type and a composite type", statement: "DROP TYPE mood, pair;",
			state:     listing([]lintext.BaselineObject{mood("a"), pair}, every...),
			community: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "mood"}},
			atlas: []changeProjection{
				{Kind: lint.SchemaChangeDrop, Object: "mood"},
				{Kind: lint.SchemaChangeDrop, Object: "pair"},
			},
		},
		{
			name: "a type without a starting state", statement: "DROP TYPE pair;",
			community: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "pair"}},
			atlas:     []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "pair"}},
		},
		{
			name: "a composite type from a read that lists no types in full", statement: "DROP TYPE pair;",
			state: listing([]lintext.BaselineObject{pair}, lintext.BaselineView), community: make([]changeProjection, 0),
			atlas: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "pair"}},
		},
		{
			name: "a composite type an earlier statement named", statement: "DROP TYPE pair;\nCREATE TYPE pair AS ENUM ('a');\nDROP TYPE pair;",
			state: listing([]lintext.BaselineObject{pair}, every...),
			community: []changeProjection{
				{Kind: lint.SchemaChangeAdd, Object: "pair"},
				{Kind: lint.SchemaChangeDrop, Object: "pair"},
			},
			atlas: []changeProjection{
				{Kind: lint.SchemaChangeDrop, Object: "pair"},
				{Kind: lint.SchemaChangeAdd, Object: "pair"},
				{Kind: lint.SchemaChangeDrop, Object: "pair"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name+"/community", func(t *testing.T) {
			c := qt.New(t)
			got := headChanges(c, test.statement, test.state.options("postgres", test.scope, lint.ChangeCountAtlasCommunity))
			c.Assert(got, qt.DeepEquals, test.community)
		})
		t.Run(test.name+"/atlas", func(t *testing.T) {
			c := qt.New(t)
			got := headChanges(c, test.statement, test.state.options("postgres", test.scope, lint.ChangeCountAtlas))
			c.Assert(got, qt.DeepEquals, test.atlas)
		})
	}
}

// The community count asks for the starting state of a version holding a
// type drop, written IF EXISTS or not, so that it can tell an enum from a type
// the binary does not read. The Atlas count needs the state only for the IF
// EXISTS drop, and no count asks for it on a dialect whose read lists no
// types.
func TestTypeDrop_BaselineVersions(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		count     lint.ChangeCount
		want      []int64
	}{
		{name: "community", dialect: "postgres", statement: "DROP TYPE pair;", count: lint.ChangeCountAtlasCommunity, want: []int64{2}},
		{name: "Atlas", dialect: "postgres", statement: "DROP TYPE pair;", count: lint.ChangeCountAtlas},
		{name: "Atlas IF EXISTS", dialect: "postgres", statement: "DROP TYPE IF EXISTS pair;", count: lint.ChangeCountAtlas, want: []int64{2}},
		{name: "a dialect that lists no types", dialect: "cockroachdb", statement: "DROP TYPE pair;", count: lint.ChangeCountAtlasCommunity},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
				headFile:                  test.statement + "\n",
			}), lint.Options{Dialect: test.dialect, ChangeCount: test.count})
			c.Assert(err, qt.IsNil)
			c.Assert(analysis.BaselineVersions(), qt.DeepEquals, test.want)
		})
	}
}

// headSites lints a directory whose second version holds statement and keeps
// the findings of the rules named.
func headSites(c *qt.C, statement string, opts lint.Options, rules ...string) []string {
	c.Helper()
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
		headFile:                  statement + "\n",
	}), opts)
	c.Assert(err, qt.IsNil)
	sites := slices.DeleteFunc(findingSites(findings), func(site string) bool {
		return !slices.ContainsFunc(rules, func(rule string) bool { return strings.HasSuffix(site, ":"+rule) })
	})
	slices.Sort(sites)
	return sites
}

// An IF EXISTS drop of an object the starting state shows is absent removes
// nothing, so DS107 does not report it.
func TestIfExistsDropOfAnAbsentObject_IsNotDS107(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		state     objectState
	}{
		{name: "an absent type", dialect: "postgres", statement: "DROP TYPE IF EXISTS gone;", state: listing(nil, every...)},
		{name: "two absent types", dialect: "postgres", statement: "DROP TYPE IF EXISTS gone, other;", state: listing(nil, every...)},
		{
			name: "a trigger another table has", dialect: "postgres", statement: "DROP TRIGGER IF EXISTS audit ON users;",
			state: listing([]lintext.BaselineObject{object(lintext.BaselineTrigger, "orders", "audit")}, every...),
		},
		{name: "an absent trigger on SQLite", dialect: "sqlite", statement: "DROP TRIGGER IF EXISTS gone;", state: listing(nil, every...)},
		{
			name: "an absent trigger qualified by SQLite's one schema", dialect: "sqlite",
			statement: "DROP TRIGGER IF EXISTS main.gone;", state: listing(nil, every...),
		},
		{name: "an absent trigger on MySQL", dialect: "mysql", statement: "DROP TRIGGER IF EXISTS gone;", state: listing(nil, every...)},
		{
			name: "an absent type, from a state read empty", dialect: "postgres", statement: "DROP TYPE IF EXISTS gone;",
			state: objectState{reads: []lintext.BaselineRead{{Version: 2, Listed: every}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(headSites(c, test.statement, test.state.options(test.dialect, "", lint.ChangeCountStatements), "DS107"), qt.HasLen, 0)
		})
	}
}

// Each row is a drop that may remove an object, so DS107 reports it: the
// object is there, the drop has no IF EXISTS, the run read no state, the read
// does not list the kind, or an earlier statement may have made the object.
func TestDropOfAnObjectThatMayExist_IsDS107(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		state     objectState
		want      []string
	}{
		{
			name: "a present type", dialect: "postgres", statement: "DROP TYPE IF EXISTS mood;",
			state: listing([]lintext.BaselineObject{mood("a")}, every...), want: []string{headFile + ":1:DS107"},
		},
		{
			name: "an absent type and a present one", dialect: "postgres", statement: "DROP TYPE IF EXISTS gone, mood;",
			state: listing([]lintext.BaselineObject{mood("a")}, every...), want: []string{headFile + ":1:DS107"},
		},
		{
			name: "no IF EXISTS", dialect: "postgres", statement: "DROP TYPE gone;",
			state: listing(nil, every...), want: []string{headFile + ":1:DS107"},
		},
		{
			name: "no starting state", dialect: "postgres", statement: "DROP TYPE IF EXISTS gone;",
			want: []string{headFile + ":1:DS107"},
		},
		{
			name: "a read that does not list types", dialect: "postgres", statement: "DROP TYPE IF EXISTS gone;",
			state: listing(nil, lintext.BaselineView), want: []string{headFile + ":1:DS107"},
		},
		{
			name: "a type the file created", dialect: "postgres", statement: "CREATE TYPE gone AS ENUM ('a');\nDROP TYPE IF EXISTS gone;",
			state: listing(nil, every...), want: []string{headFile + ":2:DS107"},
		},
		{
			name: "a trigger its table has", dialect: "postgres", statement: "DROP TRIGGER IF EXISTS audit ON users;",
			state: listing([]lintext.BaselineObject{object(lintext.BaselineTrigger, "users", "audit")}, every...),
			want:  []string{headFile + ":1:DS107"},
		},
		{
			name: "a trigger in SQLite's temp schema", dialect: "sqlite", statement: "DROP TRIGGER IF EXISTS temp.gone;",
			state: listing(nil, every...), want: []string{headFile + ":1:DS107"},
		},
		{
			name: "an absent routine, a kind no read lists", dialect: "postgres", statement: "DROP FUNCTION IF EXISTS gone();",
			state: listing(nil, every...), want: []string{headFile + ":1:DS107"},
		},
		{
			name: "an absent extension, a kind no read lists", dialect: "postgres", statement: "DROP EXTENSION IF EXISTS gone;",
			state: listing(nil, every...), want: []string{headFile + ":1:DS107"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := headSites(c, test.statement, test.state.options(test.dialect, "", lint.ChangeCountStatements), "DS107")
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// DS107 asks for the starting state of a version holding an IF EXISTS drop of
// a kind the dialect's read lists, under every count, and names itself as
// missing that input on a run without one. A drop of a kind no read lists
// asks for nothing.
func TestDS107_BaselineInput(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		versions  []int64
		unmet     []string
	}{
		{name: "an IF EXISTS type drop", dialect: "postgres", statement: "DROP TYPE IF EXISTS gone;", versions: []int64{2}, unmet: []string{"DS107"}},
		{name: "an IF EXISTS trigger drop", dialect: "sqlite", statement: "DROP TRIGGER IF EXISTS gone;", versions: []int64{2}, unmet: []string{"DS107"}},
		{name: "a type drop without IF EXISTS", dialect: "postgres", statement: "DROP TYPE gone;"},
		{name: "an IF EXISTS routine drop", dialect: "postgres", statement: "DROP FUNCTION IF EXISTS gone();"},
		{name: "an IF EXISTS type drop on a dialect that lists no types", dialect: "cockroachdb", statement: "DROP TYPE IF EXISTS gone;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
				headFile:                  test.statement + "\n",
			}), lint.Options{Dialect: test.dialect})
			c.Assert(err, qt.IsNil)
			c.Assert(analysis.BaselineVersions(), qt.DeepEquals, test.versions)
			var unmet []string
			for _, input := range analysis.UnmetInputs() {
				unmet = append(unmet, input.Rule)
			}
			c.Assert(unmet, qt.DeepEquals, test.unmet)
		})
	}
}

// DS102 and BC104 ask for the starting state of a version holding a DROP
// COLUMN IF EXISTS, under the statement count too, which reads no state of its
// own, and name themselves as missing that input on a run without one. A drop
// without IF EXISTS asks for nothing. DS110P asks for the state of every
// column drop, for what reads the column, so it is disabled to leave the two
// rules the only ones asking.
func TestColumnDrop_BaselineInput(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		versions  []int64
		unmet     []string
	}{
		{name: "an IF EXISTS column drop", statement: "ALTER TABLE users DROP COLUMN IF EXISTS gone;", versions: []int64{2}, unmet: []string{"BC104", "DS102"}},
		{name: "a column drop without IF EXISTS", statement: "ALTER TABLE users DROP COLUMN id;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
				headFile:                  test.statement + "\n",
			}), lint.Options{Dialect: "postgres", Disabled: []string{"DS110P"}})
			c.Assert(err, qt.IsNil)
			c.Assert(analysis.BaselineVersions(), qt.DeepEquals, test.versions)
			var unmet []string
			for _, input := range analysis.UnmetInputs() {
				unmet = append(unmet, input.Rule)
			}
			slices.Sort(unmet)
			c.Assert(unmet, qt.DeepEquals, test.unmet)
		})
	}
}

// A sequence is scoped by the schema it is written with, as a table is, so a
// run that reviews public counts the creation of public.s and of a bare s, and
// not of app.s.
func TestSequenceCreation_IsScopedByItsSchema(t *testing.T) {
	tests := []struct {
		statement string
		want      []changeProjection
	}{
		{statement: "CREATE SEQUENCE app.s;", want: make([]changeProjection, 0)},
		{statement: "CREATE SEQUENCE public.s;", want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "public.s"}}},
		{statement: "CREATE SEQUENCE s;", want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "s"}}},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			got := headChanges(c, test.statement, objectState{}.options("postgres", "public", lint.ChangeCountAtlas))
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A DROP COLUMN IF EXISTS of a column the starting state shows its table does
// not have deletes nothing and retires no name, so neither DS102 nor BC104
// reports it. The community binary reports nothing for it either, where it
// reports DS103 for a column that is there.
func TestDropColumnIfExistsOfAnAbsentColumn_IsNotADrop(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		state     objectState
		want      []string
	}{
		{
			name: "an absent column", statement: "ALTER TABLE users DROP COLUMN IF EXISTS gone;",
			state: listing(nil), want: make([]string, 0),
		},
		{
			name: "an absent column qualified by the reviewed schema", statement: "ALTER TABLE public.users DROP COLUMN IF EXISTS gone;",
			state: listing(nil), want: make([]string, 0),
		},
		{
			name: "a present column", statement: "ALTER TABLE users DROP COLUMN IF EXISTS id;",
			state: listing(nil), want: []string{headFile + ":1:BC104", headFile + ":1:DS102"},
		},
		{
			name: "an absent column without a starting state", statement: "ALTER TABLE users DROP COLUMN IF EXISTS gone;",
			want: []string{headFile + ":1:BC104", headFile + ":1:DS102"},
		},
		{
			name: "an absent column without IF EXISTS", statement: "ALTER TABLE users DROP COLUMN gone;",
			state: listing(nil), want: []string{headFile + ":1:BC104", headFile + ":1:DS102"},
		},
		{
			name: "a column of a table the state does not hold", statement: "ALTER TABLE other DROP COLUMN IF EXISTS gone;",
			state: listing(nil), want: []string{headFile + ":1:BC104", headFile + ":1:DS102"},
		},
		{
			name:      "an absent column after a statement that may have added it",
			statement: "DO $$ BEGIN ALTER TABLE users ADD COLUMN gone int; END $$;\nALTER TABLE users DROP COLUMN IF EXISTS gone;",
			state:     listing(nil), want: []string{headFile + ":2:BC104", headFile + ":2:DS102"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := test.state.options("postgres", "public", lint.ChangeCountStatements)
			c.Assert(headSites(c, test.statement, opts, "DS102", "BC104"), qt.DeepEquals, test.want)
		})
	}
}

// A statement that drops an absent column and a present one reports the
// present one alone.
func TestDropColumnIfExistsOfAnAbsentAndAPresentColumn_NamesThePresentOne(t *testing.T) {
	c := qt.New(t)
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_users.up.sql": "CREATE TABLE users (id int);\n",
		headFile:                  "ALTER TABLE users DROP COLUMN IF EXISTS gone, DROP COLUMN IF EXISTS id;\n",
	}), listing(nil).options("postgres", "", lint.ChangeCountStatements))
	c.Assert(err, qt.IsNil)
	drops := slices.DeleteFunc(findings, func(finding lint.Finding) bool {
		return finding.Rule != "DS102" && finding.Rule != "BC104"
	})
	var dropped []string
	for _, finding := range drops {
		for _, subject := range finding.Context.Subjects {
			dropped = append(dropped, finding.Rule+":"+subject.Name)
		}
	}
	slices.Sort(dropped)
	c.Assert(dropped, qt.DeepEquals, []string{"BC104:id", "DS102:id"})
}

// A large first file counts each object it leaves behind once, and a qualifier
// naming the schema an unqualified name resolves to names the same object as
// none: the pinned binary counts ten for ten tables and a table created and
// dropped under the two spellings. A run that reviews every schema of an
// engine with several cannot say which schema the bare name lands in, so it
// keeps the two apart; that count is Ptah's own.
func TestAnalyzeFS_LeftBehindReadsTheRunSchema(t *testing.T) {
	const opening = "0000000001_init.up.sql"
	tests := []struct {
		name    string
		dialect string
		scope   string
		pair    string
		want    int
	}{
		{name: "created qualified, dropped bare", dialect: "postgres", scope: "public", pair: "CREATE TABLE public.a (id int);\nDROP TABLE a;\n", want: 10},
		{name: "created bare, dropped qualified", dialect: "postgres", scope: "public", pair: "CREATE TABLE a (id int);\nDROP TABLE public.a;\n", want: 10},
		{name: "created qualified by SQLite's one schema", dialect: "sqlite", pair: "CREATE TABLE main.a (id int);\nDROP TABLE a;\n", want: 10},
		{name: "dropped qualified by SQLite's one schema", dialect: "sqlite", pair: "CREATE TABLE a (id int);\nDROP TABLE main.a;\n", want: 10},
		{name: "a run that reviews every schema", dialect: "postgres", pair: "CREATE TABLE public.a (id int);\nDROP TABLE a;\n", want: 11},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			files := map[string]string{opening: tables(1, 10) + test.pair}
			got := changeCountOf(c, files, lint.Options{Dialect: test.dialect, SchemaScope: test.scope, ChangeCount: lint.ChangeCountAtlas}, opening)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
