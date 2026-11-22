package chprobe_test

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chprobe"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/rundiag"
)

// lookupQuery and formatQuery are the queries the service sends: whether the
// server has the formatter, and the formatting of one condition.
const (
	lookupQuery = "SELECT count() FROM system.functions WHERE name = 'formatQuerySingleLineOrNull'"
	formatQuery = "SELECT formatQuerySingleLineOrNull(concat('SELECT (', ?, ')'))"
)

// conditionServer answers the formatting query as a ClickHouse server does:
// with the text printed holds for a condition, with NULL for a condition it
// cannot parse, and with failure for any other. A server that lacks the
// formatter finds none in system.functions and fails each call to it, as a
// release older than the function does; one whose lookup fails fails it.
type conditionServer struct {
	printed        map[string]string
	unparsed       map[string]bool
	lacksFormatter bool
	lookupFails    bool
	queries        []string
	arguments      []any
}

func (s *conditionServer) query(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	s.queries = append(s.queries, query)
	if query == lookupQuery && s.lookupFails {
		return dbtest.QueryResult{}, errors.New("connection reset by peer")
	}
	if query == lookupQuery {
		found := map[bool]uint64{false: 1, true: 0}[s.lacksFormatter]
		return dbtest.QueryResult{Columns: []string{"count()"}, Rows: [][]driver.Value{{found}}}, nil
	}
	if s.lacksFormatter {
		return dbtest.QueryResult{}, errors.New("code: 46, message: Unknown function formatQuerySingleLineOrNull")
	}
	condition := args[0].Value.(string)
	s.arguments = append(s.arguments, condition)
	if s.unparsed[condition] {
		return dbtest.QueryResult{Columns: []string{"formatted"}, Rows: [][]driver.Value{{nil}}}, nil
	}
	printed, found := s.printed[condition]
	if !found {
		return dbtest.QueryResult{}, errors.New("connection reset by peer")
	}
	return dbtest.QueryResult{Columns: []string{"formatted"}, Rows: [][]driver.Value{{"SELECT " + printed}}}, nil
}

// What each line prints for `SELECT (<condition>)`, measured on 26.10.3.7 and
// 24.10.4.191 over each declared spelling and over the text its
// create_table_query holds for it.
var (
	printed2610 = map[string]string{
		"CAST(v AS Int64) > 0": "(CAST(v, 'Int64') > 0)", "v::Int64 > 0": "(CAST(v, 'Int64') > 0)",
		"(CAST(v, 'Int64') > 0)": "(CAST(v, 'Int64') > 0)",
		"s ILIKE 'a%' OR s = ''": "((s ILIKE 'a%') OR (s = ''))", "((s ILIKE 'a%') OR (s = ''))": "((s ILIKE 'a%') OR (s = ''))",
		"equals(v, v)": "(equals(v, v))", "(equals(v, v))": "(equals(v, v))",
		"v > 0": "(v > 0)", "(V > 0)": "(V > 0)",
	}
	printed2410 = map[string]string{
		"CAST(v AS Int64) > 0": "CAST(v, 'Int64') > 0", "v::Int64 > 0": "CAST(v, 'Int64') > 0",
		"CAST(v, 'Int64') > 0":   "CAST(v, 'Int64') > 0",
		"s ILIKE 'a%' OR s = ''": "(s ILIKE 'a%') OR (s = '')", "(s ILIKE 'a%') OR (s = '')": "(s ILIKE 'a%') OR (s = '')",
		"equals(v, v)": "v = v", "v = v": "v = v", "greater(v, 0)": "v > 0", "v > 0": "v > 0",
		"V > 0": "V > 0",
	}
)

func heldCheck(name, clause string) catalog.Constraint {
	return catalog.Constraint{Name: name, TableName: "t", Type: "CHECK", CheckClause: new(clause)}
}

func checksRequest(probe schemaext.ProbeSession, pairs ...schemacomparison.CheckPair) schemacomparison.ChecksRequest {
	return schemacomparison.ChecksRequest{Target: "clickhouse", Identifiers: semantics(), Session: probe, Pairs: pairs}
}

// Each line reads its own spelling of a declared CHECK as the declaration,
// and a column named in another case as another CHECK. The service looks the
// formatter up once, then formats each distinct text once, bound as a
// parameter inside the parentheses the renderer writes.
func TestCheckService_AsksTheServerToFormatBothSides(t *testing.T) {
	for _, test := range []struct {
		name    string
		printed map[string]string
		pairs   []schemacomparison.CheckPair
		want    []bool
		queries int
	}{
		{
			name: "26.10", printed: printed2610,
			pairs: []schemacomparison.CheckPair{
				{Held: heldCheck("c0", "(CAST(v, 'Int64') > 0)"), Declared: "CAST(v AS Int64) > 0"},
				{Held: heldCheck("c1", "(CAST(v, 'Int64') > 0)"), Declared: "v::Int64 > 0"},
				{Held: heldCheck("c2", "((s ILIKE 'a%') OR (s = ''))"), Declared: "s ILIKE 'a%' OR s = ''"},
				{Held: heldCheck("c3", "(equals(v, v))"), Declared: "equals(v, v)"},
				{Held: heldCheck("c4", "(V > 0)"), Declared: "v > 0"},
			},
			want: []bool{true, true, true, true, false}, queries: 10,
		},
		{
			name: "24.10", printed: printed2410,
			pairs: []schemacomparison.CheckPair{
				{Held: heldCheck("c0", "CAST(v, 'Int64') > 0"), Declared: "CAST(v AS Int64) > 0"},
				{Held: heldCheck("c1", "CAST(v, 'Int64') > 0"), Declared: "v::Int64 > 0"},
				{Held: heldCheck("c2", "(s ILIKE 'a%') OR (s = '')"), Declared: "s ILIKE 'a%' OR s = ''"},
				{Held: heldCheck("c3", "v = v"), Declared: "equals(v, v)"},
				{Held: heldCheck("c4", "v > 0"), Declared: "greater(v, 0)"},
				{Held: heldCheck("c5", "V > 0"), Declared: "v > 0"},
			},
			want: []bool{true, true, true, true, true, false}, queries: 11,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scripted := &conditionServer{printed: test.printed}
			db := dbtest.Open(t, scripted.query)

			answers, err := chprobe.CheckService{}.EquivalentChecks(t.Context(), checksRequest(session{db: db.SQL}, test.pairs...))

			c.Assert(err, qt.IsNil)
			c.Assert(answers, qt.DeepEquals, test.want)
			c.Assert(scripted.queries, qt.HasLen, test.queries)
			c.Assert(scripted.queries[:2], qt.DeepEquals, []string{lookupQuery, formatQuery})
		})
	}
}

// A declared CHECK the server cannot parse is another CHECK than the held
// one, so the plan adds it and the server refuses it with its own reason.
func TestCheckService_ADeclarationTheServerCannotParseIsAnotherCheck(t *testing.T) {
	c := qt.New(t)
	scripted := &conditionServer{printed: printed2410, unparsed: map[string]bool{"v >": true}}
	db := dbtest.Open(t, scripted.query)

	answers, err := chprobe.CheckService{}.EquivalentChecks(t.Context(), checksRequest(session{db: db.SQL},
		schemacomparison.CheckPair{Held: heldCheck("c0", "v > 0"), Declared: "v >"}))

	c.Assert(err, qt.IsNil)
	c.Assert(answers, qt.DeepEquals, []bool{false})
}

// A server without formatQuerySingleLineOrNull, which an older release lacks
// and answers with an unknown function, decides nothing, so the comparison
// compares the conditions as text. The run warns once for each held CHECK,
// however many requests name it, and the server is asked nothing past the
// lookup.
func TestCheckService_AServerWithoutTheFormatterDecidesNothing(t *testing.T) {
	c := qt.New(t)
	scripted := &conditionServer{lacksFormatter: true}
	db := dbtest.Open(t, scripted.query)
	report := rundiag.New(rundiag.Options{Stream: new(bytes.Buffer)})
	ctx := rundiag.With(t.Context(), report)
	request := checksRequest(session{db: db.SQL},
		schemacomparison.CheckPair{Held: heldCheck("c0", "v > 0"), Declared: "greater(v, 0)"},
		schemacomparison.CheckPair{Held: heldCheck("c1", "v = v"), Declared: "equals(v, v)"})
	lines := []string{
		"CHECK c0 on t is compared as written, not in the form the server stores: the server has no formatQuerySingleLineOrNull function",
		"CHECK c1 on t is compared as written, not in the form the server stores: the server has no formatQuerySingleLineOrNull function",
	}
	summary := "2 CHECK constraints are compared as written, not in the form the server stores: " +
		"the server has no formatQuerySingleLineOrNull function; " + string(rundiag.ListedAtDebugLevel)

	first, firstErr := chprobe.CheckService{}.EquivalentChecks(ctx, request)
	second, secondErr := chprobe.CheckService{}.EquivalentChecks(ctx, request)

	c.Assert(firstErr, qt.IsNil)
	c.Assert(first, qt.IsNil)
	c.Assert(secondErr, qt.IsNil)
	c.Assert(second, qt.IsNil)
	c.Assert(scripted.queries, qt.DeepEquals, []string{lookupQuery, lookupQuery})
	c.Assert(report.Drain(), qt.DeepEquals, []rundiag.Warning{{Category: "unformatted-check", Count: 2, Message: summary, Objects: lines}})
}

// With no session, with a session that cannot run the queries, and with
// nothing to compare, the service decides nothing and asks the server nothing.
func TestCheckService_DecidesNothingWithoutAServer(t *testing.T) {
	pair := schemacomparison.CheckPair{Held: heldCheck("c0", "v > 0"), Declared: "v > 0"}
	for _, test := range []struct {
		name     string
		declined bool
		noProbe  bool
		pairs    []schemacomparison.CheckPair
	}{
		{name: "no session", noProbe: true, pairs: []schemacomparison.CheckPair{pair}},
		{name: "a declined session", declined: true, pairs: []schemacomparison.CheckPair{pair}},
		{name: "no pairs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scripted := &conditionServer{printed: printed2410}
			db := dbtest.Open(t, scripted.query)
			probes := map[bool]schemaext.ProbeSession{false: session{db: db.SQL, declined: test.declined}, true: nil}

			answers, err := chprobe.CheckService{}.EquivalentChecks(t.Context(), checksRequest(probes[test.noProbe], test.pairs...))

			c.Assert(err, qt.IsNil)
			c.Assert(answers, qt.IsNil)
			c.Assert(scripted.queries, qt.HasLen, 0)
		})
	}
}

// The service runs on ClickHouse only, needs a condition on each held CHECK,
// and fails the comparison when the server fails a query or cannot format a
// CHECK it holds.
func TestCheckService_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		target  string
		pair    schemacomparison.CheckPair
		wantIs  error
		wantMsg string
	}{
		{
			name: "another target", target: "postgres", pair: schemacomparison.CheckPair{Held: heldCheck("c0", "v > 0"), Declared: "v > 0"},
			wantIs: ptaherr.ErrUnsupportedDialect, wantMsg: `.*ClickHouse CHECK comparison on "postgres"`,
		},
		{
			name: "a held CHECK with no condition", target: "clickhouse",
			pair:   schemacomparison.CheckPair{Held: catalog.Constraint{Name: "c0", TableName: "t", Type: "CHECK"}, Declared: "v > 0"},
			wantIs: schemaext.ErrInvalidValue, wantMsg: `.*CHECK c0 on t has no condition to compare`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scripted := &conditionServer{printed: printed2410}
			db := dbtest.Open(t, scripted.query)
			request := checksRequest(session{db: db.SQL}, test.pair)
			request.Target = test.target

			answers, err := chprobe.CheckService{}.EquivalentChecks(t.Context(), request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantMsg)
			c.Assert(answers, qt.IsNil)
		})
	}
}

// A query the server fails for a reason other than the text, a failed
// lookup of the formatter, and a held CHECK the server cannot parse fail the
// comparison rather than read as a changed CHECK.
func TestCheckService_FailsWhenTheServerCannotAnswer(t *testing.T) {
	for _, test := range []struct {
		name        string
		unparsed    map[string]bool
		lookupFails bool
		pair        schemacomparison.CheckPair
		want        string
	}{
		{
			name: "a failed query", pair: schemacomparison.CheckPair{Held: heldCheck("c0", "v > 0"), Declared: "v > 1"},
			want: `format CHECK condition v > 1: connection reset by peer`,
		},
		{
			name: "a failed lookup of the formatter", lookupFails: true,
			pair: schemacomparison.CheckPair{Held: heldCheck("c0", "v > 0"), Declared: "v > 0"},
			want: `look up formatQuerySingleLineOrNull: connection reset by peer`,
		},
		{
			name: "a held CHECK the server cannot parse", unparsed: map[string]bool{"v >>> 0": true},
			pair: schemacomparison.CheckPair{Held: heldCheck("c0", "v >>> 0"), Declared: "v > 0"},
			want: `ClickHouse cannot format CHECK c0 on t, which it holds as v >>> 0`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scripted := &conditionServer{printed: printed2410, unparsed: test.unparsed, lookupFails: test.lookupFails}
			db := dbtest.Open(t, scripted.query)

			answers, err := chprobe.CheckService{}.EquivalentChecks(t.Context(), checksRequest(session{db: db.SQL}, test.pair))

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(answers, qt.IsNil)
		})
	}
}

// A canceled comparison asks the server nothing.
func TestCheckService_StopsOnACanceledContext(t *testing.T) {
	c := qt.New(t)
	scripted := &conditionServer{printed: printed2410}
	db := dbtest.Open(t, scripted.query)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	answers, err := chprobe.CheckService{}.EquivalentChecks(ctx, checksRequest(session{db: db.SQL},
		schemacomparison.CheckPair{Held: heldCheck("c0", "v > 0"), Declared: "v > 0"}))

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(answers, qt.IsNil)
	c.Assert(scripted.queries, qt.HasLen, 0)
}
