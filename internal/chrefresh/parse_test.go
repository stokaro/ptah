package chrefresh_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chrefresh"
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
// [chschema.Schedule.Clause] against each other, so a clause dropped by one and never
// emitted by the other cannot pass.
func TestParseCreateQuery_ReadsBackEveryStoredClause(t *testing.T) {
	c := qt.New(t)

	for _, view := range refreshableStatements(c) {
		t.Run(view.name, func(t *testing.T) {
			c := qt.New(t)

			spec := chrefresh.ParseCreateQuery(view.statement)

			c.Assert(spec, qt.IsNotNil)
			c.Assert(spec.Clause(), qt.Equals, view.clause)
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

// TestParseClause_RefusesWhatItCannotRead is the fail-closed direction for a
// declaration: a clause this parser only half understands is refused with the
// reason, never read as a different schedule. A partial one would plan a
// change to a view that already has what was declared, forever, or drop a
// clause the author wrote (stokaro/ptah#1802).
func TestParseClause_RefusesWhatItCannotRead(t *testing.T) {
	tests := []struct {
		name   string
		clause string
		want   string
	}{
		{name: "empty", clause: "", want: `refresh clause is empty.*`},
		{name: "mode alone", clause: "EVERY", want: `refresh EVERY needs an interval.*`},
		{name: "mode Ptah does not know", clause: "SOMETIMES 1 HOUR", want: `refresh clause starts with "SOMETIMES".*`},
		{name: "interval that is not one", clause: "EVERY soon", want: `refresh EVERY needs an interval.*`},
		{name: "unit Ptah does not know", clause: "EVERY 1 FORTNIGHT", want: `refresh EVERY needs an interval.*`},
		{name: "settings, which are not modeled", clause: "EVERY 1 HOUR SETTINGS refresh_retries = 3", want: `refresh "EVERY 1 HOUR SETTINGS refresh_retries = 3": refresh SETTINGS is not modeled`},
		{name: "offset with no interval", clause: "every 1 hour offset", want: `refresh OFFSET needs an interval.*`},
		{name: "randomize with no interval", clause: "every 1 hour randomize for", want: `refresh RANDOMIZE FOR needs an interval.*`},
		{name: "depends on nothing", clause: "every 1 hour depends on", want: `refresh DEPENDS ON needs a view name`},
		{name: "depends on a trailing comma", clause: "every 1 hour depends on a,", want: `refresh DEPENDS ON needs a view name`},
		{name: "a repeated offset", clause: "every 1 day offset 1 hour offset 2 hour", want: `refresh OFFSET is repeated or out of order.*`},
		{name: "clauses out of order", clause: "every 1 hour depends on a randomize for 5 minute", want: `refresh RANDOMIZE FOR is repeated or out of order.*`},
		{name: "a repeated append", clause: "every 1 hour append append", want: `refresh APPEND is repeated or out of order.*`},
		{name: "a target table", clause: "every 1 hour to db.t", want: `refresh TO names where a view writes, not when it refreshes`},
		{name: "a word that opens no clause", clause: "every 1 hour hourly", want: `refresh clause has "hourly" where a clause was expected`},
		{name: "an unterminated quote", clause: "every 1 hour depends on `a", want: `refresh clause has an unterminated .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			spec, err := chrefresh.ParseClause(test.clause)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(spec, qt.IsNil)
		})
	}
}

// TestParseClause_ReadsEveryClauseOfTheGrammar is the acceptance control for
// the refusals above: every clause, in either case, reads into the schedule.
func TestParseClause_ReadsEveryClauseOfTheGrammar(t *testing.T) {
	c := qt.New(t)

	spec, err := chrefresh.ParseClause("every 1 day offset 2 hour randomize for 30 minute depends on a, db.`b c` append")

	c.Assert(err, qt.IsNil)
	c.Assert(spec, qt.DeepEquals, &chschema.Schedule{
		Mode: "EVERY", Interval: "1 DAY", Offset: "2 HOUR", Randomize: "30 MINUTE",
		DependsOn: []string{"a", "db.`b c`"}, Append: true,
	})
}

// clauseShape is one statement of testdata/clause_shapes.tsv.
type clauseShape struct {
	version, clause, statement string
}

// clauseShapes reads the statements captured on each declared server line:
// every clause the grammar allows, with a target table, refresh settings and
// a quoted name among them.
func clauseShapes(c *qt.C) []clauseShape {
	c.Helper()
	raw, err := os.ReadFile("testdata/clause_shapes.tsv")
	c.Assert(err, qt.IsNil)
	var shapes []clauseShape
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.SplitN(line, "\t", 3)
		c.Assert(fields, qt.HasLen, 3)
		shapes = append(shapes, clauseShape{version: fields[0], clause: fields[1], statement: fields[2]})
	}
	c.Assert(len(shapes) >= 24, qt.IsTrue, qt.Commentf("%d shapes", len(shapes)))
	return shapes
}

// TestParseCreateQuery_ReadsEveryMeasuredShape reads each captured statement.
// A clause stops where the server's grammar says it does, so a TO target,
// refresh SETTINGS or APPEND are never read into a dependency name; a clause
// with SETTINGS, which Ptah does not model, reads as no schedule, which the
// reader reports as one it could not read (stokaro/ptah#4278).
func TestParseCreateQuery_ReadsEveryMeasuredShape(t *testing.T) {
	c := qt.New(t)
	for _, shape := range clauseShapes(c) {
		t.Run(shape.version+" "+shape.clause, func(t *testing.T) {
			c := qt.New(t)

			spec := chrefresh.ParseCreateQuery(shape.statement)

			c.Assert(scheduleClause(spec), qt.Equals, shape.clause, qt.Commentf("%s", shape.statement))
		})
	}
}

// scheduleClause renders a parsed schedule, or - for none.
func scheduleClause(spec *chschema.Schedule) string {
	if spec == nil {
		return "-"
	}
	return spec.Clause()
}

// TestCanonical_NormalizesADeclarationTheWayTheServerWouldStoreIt is the write
// side of the same round trip: what an operator declares has to land where the
// read side will find it.
func TestCanonical_NormalizesADeclarationTheWayTheServerWouldStoreIt(t *testing.T) {
	tests := []struct {
		name     string
		declared *chschema.Schedule
		schema   string
		want     string
	}{
		{
			name:     "interval is canonicalized",
			declared: &chschema.Schedule{Mode: "EVERY", Interval: "60 MINUTE"},
			want:     "EVERY 1 HOUR",
		},
		{
			name:     "mode is upper-cased",
			declared: &chschema.Schedule{Mode: "every", Interval: "1 HOUR"},
			want:     "EVERY 1 HOUR",
		},
		{
			name:     "offset is canonicalized too",
			declared: &chschema.Schedule{Mode: "EVERY", Interval: "1 DAY", Offset: "120 MINUTE"},
			want:     "EVERY 1 DAY OFFSET 2 HOUR",
		},
		{
			name:     "randomize is canonicalized too",
			declared: &chschema.Schedule{Mode: "AFTER", Interval: "1 HOUR", Randomize: "600 SECOND"},
			want:     "AFTER 1 HOUR RANDOMIZE FOR 10 MINUTE",
		},
		{
			// The server stores a dependency schema-qualified, so a comparison
			// against what it stored has to start there.
			name:     "dependencies are qualified",
			declared: &chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"mv_every"}},
			schema:   "ptah_test",
			want:     "EVERY 1 HOUR DEPENDS ON ptah_test.mv_every",
		},
		{
			name: "a dependency that already names a schema keeps it",
			declared: &chschema.Schedule{
				Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"other.mv"},
			},
			schema: "ptah_test",
			want:   "EVERY 1 HOUR DEPENDS ON other.mv",
		},
		{
			name: "every clause at once, in the order the server prints",
			declared: &chschema.Schedule{
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
			c.Assert(canonical.Clause(), qt.Equals, test.want)
		})
	}
}

// TestCanonical_RefusesOffsetOnAfter reproduces a server syntax error before a
// statement is sent: measured, `AFTER 1 HOUR OFFSET 5 MINUTE` fails to parse.
func TestCanonical_RefusesOffsetOnAfter(t *testing.T) {
	c := qt.New(t)

	_, err := chrefresh.Canonical(
		&chschema.Schedule{Mode: "AFTER", Interval: "1 HOUR", Offset: "5 MINUTE"}, "")

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
			c.Assert(canonical.Clause(), qt.Equals, view.clause)
			c.Assert(parsed.Equal(*canonical), qt.IsTrue)
		})
	}
}
