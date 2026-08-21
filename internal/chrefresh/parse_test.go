package chrefresh_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/chrefresh"
)

// storedStatements are CREATE MATERIALIZED VIEW statements captured verbatim
// from system.tables.create_table_query on live clickhouse-server 26.7.3.19.
//
// They are the server's own output rather than statements written for a test,
// which is the point: the parser's job is to read what ClickHouse prints, and a
// fixture an author wrote could agree with the parser and disagree with the
// server.
func storedStatements(c *qt.C) []string {
	c.Helper()
	raw, err := os.ReadFile("testdata/create_table_query.txt")
	c.Assert(err, qt.IsNil)
	var statements []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		if strings.TrimSpace(line) != "" {
			statements = append(statements, line)
		}
	}
	c.Assert(statements, qt.Not(qt.HasLen), 0)
	return statements
}

// storedView is one captured statement with the clause the server printed for
// it, empty for the plain view.
type storedView struct {
	name      string
	statement string
	clause    string
}

// refreshableStatements and plainStatements split the fixture by whether the
// server printed a REFRESH clause, so each test loops over one kind and needs
// no branch inside the assertion.
func refreshableStatements(c *qt.C) []storedView {
	c.Helper()
	var views []storedView
	for _, view := range allStoredViews(c) {
		if view.clause != "" {
			views = append(views, view)
		}
	}
	c.Assert(views, qt.Not(qt.HasLen), 0)
	return views
}

func plainStatements(c *qt.C) []storedView {
	c.Helper()
	var views []storedView
	for _, view := range allStoredViews(c) {
		if view.clause == "" {
			views = append(views, view)
		}
	}
	c.Assert(views, qt.Not(qt.HasLen), 0)
	return views
}

func allStoredViews(c *qt.C) []storedView {
	c.Helper()
	var views []storedView
	for _, statement := range storedStatements(c) {
		view := storedView{
			name:      storedViewName.FindStringSubmatch(statement)[1],
			statement: statement,
		}
		if match := storedRefresh.FindStringSubmatch(statement); match != nil {
			view.clause = match[1]
		}
		views = append(views, view)
	}
	return views
}

var storedViewName = regexp.MustCompile(`MATERIALIZED VIEW ptah_test\.(\S+)`)
var storedRefresh = regexp.MustCompile(` REFRESH (.*?) \(` + "`")

// TestParseCreateQuery_ReadsBackEveryStoredClause is the round trip the read
// side rests on.
//
// For each captured statement the parser must produce a schedule whose rendered
// clause is byte-identical to the one the server printed. Rendering the parse
// rather than comparing fields is deliberate: it exercises the parser and
// [chrefresh.Clause] against each other, so a clause dropped by one and never
// emitted by the other cannot pass.
func TestParseCreateQuery_ReadsBackEveryStoredClause(t *testing.T) {
	c := qt.New(t)

	for _, view := range refreshableStatements(c) {
		t.Run(view.name, func(t *testing.T) {
			c := qt.New(t)

			spec := chrefresh.ParseCreateQuery(view.statement)

			c.Assert(spec, qt.IsNotNil)
			c.Assert(chrefresh.Clause(spec), qt.Equals, view.clause)
		})
	}
}

// TestParseCreateQuery_ReadsNoScheduleFromAPlainStatement is the other half of
// the fixture: a view the server printed without a clause must read as having
// none, or Ptah would plan a change to an object that is already right.
func TestParseCreateQuery_ReadsNoScheduleFromAPlainStatement(t *testing.T) {
	c := qt.New(t)

	for _, view := range plainStatements(c) {
		t.Run(view.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(chrefresh.ParseCreateQuery(view.statement), qt.IsNil)
		})
	}
}

// TestParseCreateQuery_SeparatesPlainFromRefreshable pins the distinction the
// whole feature turns on.
//
// A plain materialized view and a refreshable one report the same engine and
// byte-identical as_select; only the statement text tells them apart. Getting
// this backwards in either direction is a live defect: reading a schedule into
// a plain view plans a change to an unchanged object, and missing one on a
// refreshable view lets a body change recreate it unscheduled.
func TestParseCreateQuery_SeparatesPlainFromRefreshable(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      bool
	}{
		{
			name:      "plain view",
			statement: "CREATE MATERIALIZED VIEW ptah_test.mv (`c` UInt64) ENGINE = MergeTree ORDER BY tuple() AS SELECT 1",
			want:      false,
		},
		{
			name:      "refreshable view",
			statement: "CREATE MATERIALIZED VIEW ptah_test.mv REFRESH EVERY 1 HOUR (`c` UInt64) ENGINE = MergeTree AS SELECT 1",
			want:      true,
		},
		{
			// A body mentioning the word must not be mistaken for a clause: the
			// marker is the REFRESH that precedes the column list, not any
			// occurrence of it in the statement.
			name:      "the word inside the body",
			statement: "CREATE MATERIALIZED VIEW ptah_test.mv (`c` UInt64) ENGINE = MergeTree AS SELECT 'REFRESH EVERY 1 HOUR' AS c",
			want:      false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(chrefresh.ParseCreateQuery(test.statement) != nil, qt.Equals, test.want)
		})
	}
}

// TestParseClause_RefusesWhatItCannotRead is the fail-closed direction.
//
// A clause this parser only half understands must read as no schedule at all. A
// partial one would be worse than none: the comparison would plan a change to
// make the view match a schedule it already has, forever.
func TestParseClause_RefusesWhatItCannotRead(t *testing.T) {
	tests := []struct {
		name   string
		clause string
	}{
		{name: "empty", clause: ""},
		{name: "mode alone", clause: "EVERY"},
		{name: "mode Ptah does not know", clause: "SOMETIMES 1 HOUR"},
		{name: "interval that is not one", clause: "EVERY soon"},
		{name: "a clause added after this was written", clause: "EVERY 1 HOUR SETTINGS x = 1"},
		{name: "unit Ptah does not know", clause: "EVERY 1 FORTNIGHT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(chrefresh.ParseClause(test.clause), qt.IsNil)
		})
	}
}

// TestCanonical_NormalizesADeclarationTheWayTheServerWouldStoreIt is the write
// side of the same round trip: what an operator declares has to land where the
// read side will find it.
func TestCanonical_NormalizesADeclarationTheWayTheServerWouldStoreIt(t *testing.T) {
	tests := []struct {
		name     string
		declared *ast.MatViewRefreshSpec
		schema   string
		want     string
	}{
		{
			name:     "interval is canonicalized",
			declared: &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "60 MINUTE"},
			want:     "EVERY 1 HOUR",
		},
		{
			name:     "mode is upper-cased",
			declared: &ast.MatViewRefreshSpec{Mode: "every", Interval: "1 HOUR"},
			want:     "EVERY 1 HOUR",
		},
		{
			name:     "offset is canonicalized too",
			declared: &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 DAY", Offset: "120 MINUTE"},
			want:     "EVERY 1 DAY OFFSET 2 HOUR",
		},
		{
			name:     "randomize is canonicalized too",
			declared: &ast.MatViewRefreshSpec{Mode: "AFTER", Interval: "1 HOUR", Randomize: "600 SECOND"},
			want:     "AFTER 1 HOUR RANDOMIZE FOR 10 MINUTE",
		},
		{
			// The server stores a dependency schema-qualified, so a comparison
			// against what it stored has to start there.
			name:     "dependencies are qualified",
			declared: &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"mv_every"}},
			schema:   "ptah_test",
			want:     "EVERY 1 HOUR DEPENDS ON ptah_test.mv_every",
		},
		{
			name: "a dependency that already names a schema keeps it",
			declared: &ast.MatViewRefreshSpec{
				Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"other.mv"},
			},
			schema: "ptah_test",
			want:   "EVERY 1 HOUR DEPENDS ON other.mv",
		},
		{
			name: "every clause at once, in the order the server prints",
			declared: &ast.MatViewRefreshSpec{
				Mode: "EVERY", Interval: "1 DAY", Offset: "2 HOUR",
				Randomize: "30 MINUTE", DependsOn: []string{"mv_every"}, Append: true,
			},
			schema: "ptah_test",
			want:   "EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 30 MINUTE DEPENDS ON ptah_test.mv_every APPEND",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			canonical, err := chrefresh.Canonical(test.declared, test.schema)

			c.Assert(err, qt.IsNil)
			c.Assert(chrefresh.Clause(canonical), qt.Equals, test.want)
		})
	}
}

// TestCanonical_RefusesOffsetOnAfter reproduces a server syntax error before a
// statement is sent: measured, `AFTER 1 HOUR OFFSET 5 MINUTE` fails to parse.
func TestCanonical_RefusesOffsetOnAfter(t *testing.T) {
	c := qt.New(t)

	_, err := chrefresh.Canonical(
		&ast.MatViewRefreshSpec{Mode: "AFTER", Interval: "1 HOUR", Offset: "5 MINUTE"}, "")

	c.Assert(err, qt.ErrorMatches, `refresh OFFSET belongs to EVERY and this schedule is AFTER`)
}

// TestCanonical_RoundTripsThroughTheParser is the property that makes a
// declared schedule and a read one comparable at all.
func TestCanonical_RoundTripsThroughTheParser(t *testing.T) {
	c := qt.New(t)

	for _, view := range refreshableStatements(c) {
		t.Run(view.name, func(t *testing.T) {
			c := qt.New(t)

			parsed := chrefresh.ParseCreateQuery(view.statement)
			c.Assert(parsed, qt.IsNotNil)
			canonical, err := chrefresh.Canonical(parsed, "ptah_test")

			// Canonicalizing what the server stored changes nothing, and the
			// two compare equal. Anything else means a view read back from the
			// database would differ from itself.
			c.Assert(err, qt.IsNil)
			c.Assert(chrefresh.Clause(canonical), qt.Equals, view.clause)
			c.Assert(chrefresh.Equal(parsed, canonical), qt.IsTrue)
		})
	}
}

func TestEqual(t *testing.T) {
	tests := []struct {
		name string
		a    *ast.MatViewRefreshSpec
		b    *ast.MatViewRefreshSpec
		want bool
	}{
		{name: "both absent", want: true},
		{
			name: "one absent",
			a:    &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"},
			want: false,
		},
		{
			name: "same",
			a:    &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"},
			b:    &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"},
			want: true,
		},
		{
			name: "different interval",
			a:    &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"},
			b:    &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "2 HOUR"},
			want: false,
		},
		{
			// EVERY and AFTER with the same interval are different schedules:
			// one is wall-clock, the other counts from the previous run.
			name: "different mode",
			a:    &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"},
			b:    &ast.MatViewRefreshSpec{Mode: "AFTER", Interval: "1 HOUR"},
			want: false,
		},
		{
			name: "different dependencies",
			a:    &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"a"}},
			b:    &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"b"}},
			want: false,
		},
		{
			name: "different append",
			a:    &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR", Append: true},
			b:    &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"},
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(chrefresh.Equal(test.a, test.b), qt.Equals, test.want)
			c.Assert(chrefresh.Equal(test.b, test.a), qt.Equals, test.want)
		})
	}
}
