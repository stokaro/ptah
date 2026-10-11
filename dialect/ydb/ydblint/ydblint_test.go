package ydblint_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/core/ptaherr"
	"ptah.run/dialect/ydb/ydblint"
	"ptah.run/internal/lintscan"
)

// Rules publishes the YD family in code order, each rule valid as data and
// judging YDB alone, and a call's rules share nothing with the next call's.
func TestRules(t *testing.T) {
	c := qt.New(t)
	rules := ydblint.Rules()
	rules[0].Targets[0] = "postgres"

	again := ydblint.Rules()
	codes := make([]string, 0, len(again))
	for _, rule := range again {
		c.Assert(rule.Validate(), qt.IsNil)
		c.Assert(rule.Targets, qt.DeepEquals, []string{"ydb"})
		codes = append(codes, rule.Code)
	}
	c.Assert(codes, qt.DeepEquals, []string{
		"YD101", "YD102", "YD103", "YD104", "YD105", "YD106", "YD107", "YD108", "YD109", "YD110", "YD111",
		"YD113", "YD114", "YD115", "YD116", "YD118", "YD119", "YD120", "YD121", "YD122", "YD130", "YD131",
		"YD140", "YD141", "YD150", "YD160",
	})
}

// file is a YDB migration file holding sql, split as YQL.
func file(name string, version int64, up bool, sql string) lintext.File {
	return lintext.File{Name: name, Version: version, Up: up, Down: !up, Statements: lintscan.Split(sql, lintscan.ForDialect("ydb"))}
}

// check runs codes over files on target and returns the findings.
func check(c *qt.C, target string, codes []string, files ...lintext.File) []lintext.Finding {
	c.Helper()
	request := lintext.CheckRequest{Target: lintext.Target{Name: target}, Codes: codes, Files: files}
	result, err := ydblint.Service{}.CheckMigrations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(lintext.ValidateCheck(request, result), qt.IsNil)
	return result.Findings
}

// The rules that read what a table looks like read it from the files before
// the one judged, the down half of a migration included, and say nothing on a
// run that names no target.
func TestService_CheckMigrations_ReadsTheHistory(t *testing.T) {
	const table = "CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX t_v GLOBAL ON (v));\n"
	const message = "DROP COLUMN v: index t_v keys the column, and YDB refuses to drop it " +
		"(Impossible drop column because table has an index with that column); drop the index first"
	subjects := []lintext.Subject{{Kind: lintext.SubjectColumn, Name: "v", Parent: "t"}}
	files := []lintext.File{
		file("0001_t.up.sql", 1, true, table),
		file("0002_v.up.sql", 2, true, "ALTER TABLE t DROP COLUMN v;\n"),
		file("0002_v.down.sql", 2, false, "ALTER TABLE t DROP COLUMN v;\n"),
	}
	for _, test := range []struct {
		name   string
		target string
		want   []lintext.Finding
	}{
		{name: "ydb", target: "ydb", want: []lintext.Finding{
			{Code: "YD104", File: 1, Statement: 0, Message: message, Subjects: subjects},
			{Code: "YD104", File: 2, Statement: 0, Message: message, Subjects: subjects},
		}},
		{name: "no target", target: "", want: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(check(c, test.target, []string{"YD104"}, files...), qt.DeepEquals, test.want)
		})
	}
}

// split splits sources on the ydb target and returns each statement's
// effects.
func split(c *qt.C, sources ...lintext.Source) [][]lintext.Effect {
	c.Helper()
	request := lintext.SplitRequest{Target: "ydb", Sources: sources}
	result, err := ydblint.Splitter{}.SplitStatements(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(lintext.ValidateSplit(request, result), qt.IsNil)
	c.Assert(result.Judged, qt.DeepEquals, []lintext.Fact{
		lintext.FactStatefulDrop, lintext.FactUniqueIndex, lintext.FactRebuildDrop, lintext.FactRebuildRename,
	})
	var effects [][]lintext.Effect
	for _, statements := range result.Files {
		for _, statement := range statements {
			effects = append(effects, statement.Effects)
		}
	}
	return effects
}

// A drop of an object whose state a new one does not get back is reported
// with what it drops; an async replication only with CASCADE, which drops
// its replica tables, and no other object of a word YDB shares.
func TestSplitter_StatefulDrops(t *testing.T) {
	c := qt.New(t)

	effects := split(c, lintext.Source{Name: "0001_drop.up.sql", Version: 1, Up: true, SQL: "DROP TOPIC IF EXISTS `app/events`;\n" +
		"DROP TRANSFER t;\n" +
		"DROP ASYNC REPLICATION r CASCADE;\n" +
		"DROP ASYNC REPLICATION r;\n" +
		"DROP COORDINATION NODE `app/locks`;\n" +
		"DROP COORDINATION SEMAPHORE `app/locks`;\n" +
		"DROP STREAMING QUERY q;\n" +
		"DROP TABLE t;\n"})

	c.Assert(effects, qt.DeepEquals, [][]lintext.Effect{
		{{Fact: lintext.FactStatefulDrop, Object: "`app/events`", Kind: "TOPIC"}},
		{{Fact: lintext.FactStatefulDrop, Object: "t", Kind: "TRANSFER"}},
		{{Fact: lintext.FactStatefulDrop, Object: "r", Kind: "ASYNC REPLICATION"}},
		nil,
		{{Fact: lintext.FactStatefulDrop, Object: "`app/locks`", Kind: "COORDINATION NODE"}},
		nil,
		{{Fact: lintext.FactStatefulDrop, Object: "q", Kind: "STREAMING QUERY"}},
		nil,
	})
}

// YQL's unique index is reported with what MF101 and MF102 name and YDB's
// own words for its failure; an index that is not unique is not.
func TestSplitter_UniqueIndexes(t *testing.T) {
	c := qt.New(t)

	effects := split(c, lintext.Source{Name: "0001_index.up.sql", Version: 1, Up: true,
		SQL: "ALTER TABLE `shop/users` ADD INDEX by_email GLOBAL UNIQUE SYNC ON (email, tenant);\n" +
			"ALTER TABLE `shop/users` ADD INDEX by_name GLOBAL SYNC ON (name);\n"})

	c.Assert(effects, qt.DeepEquals, [][]lintext.Effect{
		{{Fact: lintext.FactUniqueIndex, Object: "by_email", Table: "`shop/users`", Columns: []string{"email", "tenant"},
			Clause: "ADD INDEX", Failure: "YDB: Duplicate key found"}},
		nil,
	})
}

// itemsTable is the up migration that creates the table the rebuilds copy.
var itemsTable = lintext.Source{Name: "0001_items.up.sql", Version: 1, Up: true,
	SQL: "CREATE TABLE items (id Uint64 NOT NULL, n Int64, PRIMARY KEY (id));\n"}

// A table rebuild in YDB's order reports its renames and its drop, reading
// the old table's columns from the files before; a copy that leaves out a
// column the history knows is no rebuild.
func TestSplitter_Rebuilds(t *testing.T) {
	const swap = "ALTER TABLE items RENAME TO items_old;\nALTER TABLE items_new RENAME TO items;\nDROP TABLE items_old;\n"
	const create = "CREATE TABLE items_new (id Uint64 NOT NULL, n Int64, PRIMARY KEY (id));\n"
	rename := []lintext.Effect{{Fact: lintext.FactRebuildRename}}
	drop := []lintext.Effect{{Fact: lintext.FactRebuildDrop}}
	for _, test := range []struct {
		name string
		copy string
		want [][]lintext.Effect
	}{
		{name: "every column copied", copy: "INSERT INTO items_new (id, n) SELECT id, n FROM items;\n",
			want: [][]lintext.Effect{nil, nil, nil, rename, rename, drop}},
		{name: "a column left out", copy: "INSERT INTO items_new (id) SELECT id FROM items;\n",
			want: [][]lintext.Effect{nil, nil, nil, nil, nil, nil}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			effects := split(c, itemsTable, lintext.Source{Name: "0002_rebuild.up.sql", Version: 2, Up: true, SQL: create + test.copy + swap})
			c.Assert(effects, qt.DeepEquals, test.want)
		})
	}
}

func TestSplitter_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name    string
		ctx     context.Context
		target  string
		wantIs  error
		wantErr string
	}{
		{name: "another target", ctx: context.Background(), target: "postgres", wantIs: ptaherr.ErrUnsupportedDialect,
			wantErr: `.*the "ydb" statement splitter on "postgres"`},
		{name: "no context", target: "ydb", wantIs: lintext.ErrInvalid, wantErr: `.*splitting requires a context`},
		{name: "canceled", ctx: canceled, target: "ydb", wantIs: context.Canceled, wantErr: `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := ydblint.Splitter{}.SplitStatements(test.ctx, lintext.SplitRequest{Target: test.target,
				Sources: []lintext.Source{{Name: "1.up.sql", SQL: "DROP TOPIC t;"}}})
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.SplitResult{})
		})
	}
}

// annotate annotates sql, split the way a run that names no dialect splits
// it, for facts.
func annotate(c *qt.C, facts []lintext.Fact, sql string) []lintext.Annotation {
	c.Helper()
	request := lintext.AnnotateRequest{Facts: facts, Sources: []lintext.Source{{Name: "0001_t.up.sql", SQL: sql}},
		Files: [][]lintext.Statement{lintscan.Split(sql, lintscan.ForDialect(""))}}
	result, err := ydblint.Annotator{}.AnnotateStatements(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(lintext.ValidateAnnotate(request, result), qt.IsNil)
	c.Assert(result.Declined, qt.IsNil)
	return result.Files[0]
}

// The annotator judges a drop where YQL splits the text as the dialect-blind
// scanner did, and declines one where it splits it otherwise: a backslash
// escape YQL reads inside a string, or a dollar quote YQL does not read at all.
// A statement neither reading drops anything is judged, however the readings
// split it, and a request that does not ask about drops is answered with
// nothing.
func TestAnnotator_AnnotateStatements(t *testing.T) {
	topic := lintext.Annotation{Effects: []lintext.Effect{{Fact: lintext.FactStatefulDrop, Object: "t", Kind: "TOPIC"}}}
	declined := lintext.Annotation{Declined: true}
	asked := []lintext.Fact{lintext.FactStatefulDrop}
	for _, test := range []struct {
		name  string
		facts []lintext.Fact
		sql   string
		want  []lintext.Annotation
	}{
		{name: "a drop both readings split alike", facts: asked, sql: "SELECT 1;\nDROP TOPIC t;\n",
			want: []lintext.Annotation{{}, topic}},
		{name: "a drop YQL reads inside a string", facts: asked, sql: "SELECT 'a\\';\nDROP TOPIC t;\nSELECT '';\n",
			want: []lintext.Annotation{{}, declined, {}}},
		{name: "a drop the scanner reads inside a dollar quote", facts: asked, sql: "SELECT $$;\nDROP TOPIC t;\n$$;\n",
			want: []lintext.Annotation{declined}},
		{name: "no drop in either reading", facts: asked, sql: "SELECT $$;\nDROP TABLE t;\n$$;\n",
			want: []lintext.Annotation{{}}},
		{name: "no drop asked about", facts: []lintext.Fact{lintext.FactUniqueIndex}, sql: "DROP TOPIC t;\n",
			want: []lintext.Annotation{{}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(annotate(c, test.facts, test.sql), qt.DeepEquals, test.want)
		})
	}
}

func TestAnnotator_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	statements := [][]lintext.Statement{lintscan.Split("DROP TOPIC t;", lintscan.ForDialect(""))}
	for _, test := range []struct {
		name    string
		ctx     context.Context
		sources []lintext.Source
		wantIs  error
		wantErr string
	}{
		{name: "statements without their source", ctx: context.Background(), wantIs: lintext.ErrInvalid,
			wantErr: `.*an annotation request holds 0 sources and 1 statement lists`},
		{name: "no context", sources: []lintext.Source{{SQL: "DROP TOPIC t;"}}, wantIs: lintext.ErrInvalid,
			wantErr: `.*annotating requires a context`},
		{name: "canceled", ctx: canceled, sources: []lintext.Source{{SQL: "DROP TOPIC t;"}}, wantIs: context.Canceled,
			wantErr: `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := ydblint.Annotator{}.AnnotateStatements(test.ctx, lintext.AnnotateRequest{
				Facts: []lintext.Fact{lintext.FactStatefulDrop}, Sources: test.sources, Files: statements})
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.AnnotateResult{})
		})
	}
}

// A repeatable migration is no step of the history: the table it creates is
// unknown to the versions after it, so a drop of the column its index keys
// reports nothing, where the same table from an ordered version does.
func TestService_CheckMigrations_LeavesRepeatablesOutOfTheHistory(t *testing.T) {
	const table = "CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX t_v GLOBAL ON (v));\n"
	for _, test := range []struct {
		name       string
		repeatable bool
		want       []string
	}{
		{name: "an ordered version", want: []string{"YD104"}},
		{name: "a repeatable migration", repeatable: true, want: make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			created := file("0001_t.up.sql", 1, true, table)
			created.Repeatable = test.repeatable

			findings := check(c, "ydb", []string{"YD104"}, created, file("0002_v.up.sql", 2, true, "ALTER TABLE t DROP COLUMN v;\n"))

			codes := make([]string, 0)
			for _, finding := range findings {
				codes = append(codes, finding.Code)
			}
			c.Assert(codes, qt.DeepEquals, test.want)
		})
	}
}

// The steps of a rebuild are passed over on Ptah's own surface only: on the
// Atlas-compatible one, which reports what the analyzer it is compatible with
// reports, the rename that moves a table a view reads aside is YD106's.
func TestService_CheckMigrations_RebuildsOnPtahsSurfaceOnly(t *testing.T) {
	files := []lintext.File{
		file("0001_v.up.sql", 1, true, "CREATE TABLE base (id Uint64 NOT NULL, n Int32, PRIMARY KEY (id));\n"+
			"CREATE VIEW v WITH (security_invoker = TRUE) AS SELECT id FROM base;\n"),
		file("0002_rebuild.up.sql", 2, true, "CREATE TABLE __ptah_rebuild_base (id Uint64 NOT NULL, n Int64, PRIMARY KEY (id));\n"+
			"INSERT INTO __ptah_rebuild_base (id, n) SELECT id, CAST(n AS Int64) AS n FROM base;\n"+
			"ALTER TABLE base RENAME TO __ptah_replaced_base;\n"+
			"ALTER TABLE __ptah_rebuild_base RENAME TO base;\n"+
			"DROP TABLE __ptah_replaced_base;\n"),
	}
	for _, test := range []struct {
		name          string
		compatibility string
		want          []int
	}{
		{name: "Ptah's surface", want: make([]int, 0)},
		{name: "the Atlas-compatible surface", compatibility: "atlas", want: []int{2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := lintext.CheckRequest{Target: lintext.Target{Name: "ydb"}, Codes: []string{"YD106"}, Files: files,
				Compatibility: test.compatibility}

			result, err := ydblint.Service{}.CheckMigrations(c.Context(), request)

			c.Assert(err, qt.IsNil)
			statements := make([]int, 0)
			for _, finding := range result.Findings {
				statements = append(statements, finding.Statement)
			}
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
