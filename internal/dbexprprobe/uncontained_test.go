package dbexprprobe_test

import (
	"bytes"
	"context"
	"database/sql"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/internal/dbexprprobe"
	"ptah.run/internal/probetx"
	"ptah.run/internal/rundiag"
)

// textOnlySession runs a probe body with no transaction. Every probe these
// tests give it is refused before it reaches one, so a probe that sent a
// statement would panic on the nil transaction rather than pass.
type textOnlySession struct{ dialect string }

func (s textOnlySession) Info() catalog.ServerInfo { return catalog.ServerInfo{Dialect: s.dialect} }

func (textOnlySession) WithRolledBackTransaction(ctx context.Context, _ string, body func(context.Context, *sql.Tx) error) (bool, error) {
	return true, body(ctx, nil)
}

// escape closes the parentheses a probe writes a declaration in and commits
// a table of its own, the text a probe must never run.
const escape = "amount + 1)) COMMIT CREATE TABLE dbo.made_by_plan (x int) BEGIN TRANSACTION SELECT ((1"

// closesWarning is the warning for object refused for closing a parenthesis.
func closesWarning(object string) string {
	return "warning: " + object + " is compared as written, not in the form the server stores: " +
		"the probe that spells it does not run, because its text closes a parenthesis it does not open\n"
}

// separatorWarning is the warning for object refused for a semicolon.
func separatorWarning(object string) string {
	return "warning: " + object + " is compared as written, not in the form the server stores: " +
		"the probe that spells it does not run, because its text holds a statement separator\n"
}

// warnedRun is a run whose warnings go to the returned buffer.
func warnedRun() (context.Context, *bytes.Buffer) {
	var stream bytes.Buffer
	report := rundiag.New(rundiag.Options{Stream: &stream, Listing: rundiag.ListedAtDebugLevel})
	return rundiag.With(context.Background(), report), &stream
}

var liveColumns = []dbexprprobe.CheckProbeColumn{{Name: "amount", Type: "int"}}

// TestResolveCheckExpressions_UncontainedTextIsNotProbed leaves a CHECK whose
// text could close its parentheses unresolved, on each engine that probes one.
func TestResolveCheckExpressions_UncontainedTextIsNotProbed(t *testing.T) {
	for _, dialect := range []string{platform.SQLServer, platform.Postgres} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := warnedRun()

			got, err := dbexprprobe.ResolveCheckExpressions(ctx, textOnlySession{dialect: dialect}, []dbexprprobe.CheckExpressionProbe{{
				Key: "k", Table: "t", Columns: liveColumns, Expression: escape, Object: "CHECK ck_amount on t",
			}})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, map[string]config.CheckExpression{"k": {}})
			c.Assert(stream.String(), qt.Equals, closesWarning("CHECK ck_amount on t"))
		})
	}
}

// TestResolveGeneratedColumns_UncontainedTextIsNotProbed leaves a generated
// column whose text could close its parentheses unresolved, and reports no
// server refusal for it, since no server was asked.
func TestResolveGeneratedColumns_UncontainedTextIsNotProbed(t *testing.T) {
	for _, dialect := range []string{platform.SQLServer, platform.Postgres} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := warnedRun()

			got, refusals, err := dbexprprobe.ResolveGeneratedColumns(ctx, textOnlySession{dialect: dialect}, []dbexprprobe.GeneratedColumnProbe{{
				Key: "k", Schema: "app", Table: "t", Column: "g", Type: "int", Columns: liveColumns, Expression: escape,
			}})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, map[string]config.GeneratedExpression{"k": {}})
			c.Assert(refusals, qt.IsNil)
			c.Assert(stream.String(), qt.Equals, closesWarning("generated column app.t.g"))
		})
	}
}

// TestResolveIndexExpressions_UncontainedTextIsNotProbed checks the predicate
// as well as the expression, each with the other harmless.
func TestResolveIndexExpressions_UncontainedTextIsNotProbed(t *testing.T) {
	for _, test := range []struct {
		name                  string
		expression, predicate string
	}{
		{name: "expression", expression: escape, predicate: "amount > 0"},
		{name: "predicate", expression: "lower(name)", predicate: escape},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := warnedRun()

			got, err := dbexprprobe.ResolveIndexExpressions(ctx, textOnlySession{dialect: platform.Postgres}, []dbexprprobe.IndexExpressionProbe{{
				Key: "k", Table: "t", Columns: liveColumns, Expression: test.expression, Predicate: test.predicate, Object: "index idx_amount on t",
			}})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, map[string]config.IndexExpression{"k": {}})
			c.Assert(stream.String(), qt.Equals, closesWarning("index idx_amount on t"))
		})
	}
}

// TestResolveTriggerConditions_UncontainedTextIsNotProbed checks each declared
// part the probe writes into its CREATE TRIGGER, the condition between the
// parentheses of WHEN and the words around it.
func TestResolveTriggerConditions_UncontainedTextIsNotProbed(t *testing.T) {
	for _, test := range []struct {
		name                         string
		timing, event, forEach, when string
	}{
		{name: "timing", timing: escape, event: "INSERT", when: "NEW.amount > 0"},
		{name: "event", timing: "AFTER", event: escape, when: "NEW.amount > 0"},
		{name: "for each", timing: "AFTER", event: "INSERT", forEach: escape, when: "NEW.amount > 0"},
		{name: "condition", timing: "AFTER", event: "INSERT", when: escape},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := warnedRun()

			got, err := dbexprprobe.ResolveTriggerConditions(ctx, textOnlySession{dialect: platform.Postgres}, []dbexprprobe.TriggerConditionProbe{{
				Key: "k", Table: "t", Columns: liveColumns, Timing: test.timing, Event: test.event, ForEach: test.forEach, When: test.when,
				Object: "trigger audit on t",
			}})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, map[string]config.TriggerCondition{"k": {}})
			c.Assert(stream.String(), qt.Equals, closesWarning("trigger audit on t"))
		})
	}
}

// TestResolveViewBodies_UncontainedTextIsNotProbed refuses a semicolon in a
// body, which nothing but a semicolon can end.
func TestResolveViewBodies_UncontainedTextIsNotProbed(t *testing.T) {
	c := qt.New(t)
	ctx, stream := warnedRun()

	got, err := dbexprprobe.ResolveViewBodies(ctx, textOnlySession{dialect: platform.Postgres}, []dbexprprobe.ViewBodyProbe{{
		Key: "k", Body: "SELECT 1 AS x; COMMIT; CREATE TABLE public.made_by_plan (x int); BEGIN; SELECT 1 AS x;", Object: "view v",
	}})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, map[string]config.ViewBody{"k": {}})
	c.Assert(stream.String(), qt.Equals, separatorWarning("view v"))
}

// TestProbeDomainExpressions_UncontainedTextIsNotProbed checks each declared
// part the probe writes into its CREATE DOMAIN: the base type, the default and
// the CHECK.
func TestProbeDomainExpressions_UncontainedTextIsNotProbed(t *testing.T) {
	for _, test := range []struct {
		name                   string
		baseType, def, checked string
	}{
		{name: "base type", baseType: "integer" + escape, checked: "VALUE > 0"},
		{name: "default", baseType: "integer", def: escape, checked: "VALUE > 0"},
		{name: "check", baseType: "integer", checked: escape},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := warnedRun()

			got, err := dbexprprobe.ProbeDomainExpressions(ctx, textOnlySession{dialect: platform.Postgres}, platform.Postgres, []dbexprprobe.DomainExpressionProbe{{
				Key: "k", BaseType: test.baseType, Default: test.def, Check: test.checked, Object: "domain public.positive",
			}})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, map[string]dbexprprobe.DomainExpression{"k": {}})
			c.Assert(stream.String(), qt.Equals, closesWarning("domain public.positive"))
		})
	}
}

// TestResolveRoutineArguments_UncontainedTextIsNotProbed refuses a statement
// that holds a second one, and a signature-only fallback that does. A
// SQL-standard body's semicolons are the server's to place, so its statement
// is checked for what else could leave it before the server is asked.
func TestResolveRoutineArguments_UncontainedTextIsNotProbed(t *testing.T) {
	const separated = "CREATE FUNCTION pg_temp.f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$; COMMIT; CREATE TABLE public.made_by_plan (x int)"
	for _, test := range []struct {
		name      string
		statement string
		bareBody  bool
		fallback  string
		warning   string
	}{
		{name: "a second statement", statement: separated, warning: separatorWarning("routine f")},
		{
			name:      "a body that leaves a parenthesis open",
			statement: "CREATE FUNCTION pg_temp.f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT (1; END",
			bareBody:  true,
			warning: "warning: routine f is compared as written, not in the form the server stores: " +
				"the probe that spells it does not run, because its text leaves a parenthesis open\n",
		},
		{name: "a fallback with a second statement", statement: separated, fallback: separated, warning: separatorWarning("routine f")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := warnedRun()

			got, err := dbexprprobe.ResolveRoutineArguments(ctx, textOnlySession{dialect: platform.Postgres}, []dbexprprobe.RoutineArgumentsProbe{{
				Key: "k", Name: "f", Statement: test.statement, BareBody: test.bareBody, Fallback: test.fallback, Object: "routine f",
			}})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, map[string]config.RoutineArguments{"k": {}})
			c.Assert(stream.String(), qt.Equals, test.warning)
		})
	}
}

// TestResolveColumnSpellings_UncontainedTextIsNotProbed leaves a column whose
// statement holds a second one unresolved, by itself.
func TestResolveColumnSpellings_UncontainedTextIsNotProbed(t *testing.T) {
	c := qt.New(t)
	ctx, stream := warnedRun()
	statement := "CREATE TEMPORARY TABLE t (amount int DEFAULT 1); COMMIT; CREATE TABLE public.made_by_plan (x int)"

	got, refusals, err := dbexprprobe.ResolveColumnSpellings(ctx, textOnlySession{dialect: platform.Postgres}, []dbexprprobe.ColumnSpellingProbe{{
		Table: "t", Schema: "public", Declared: "t", Statement: statement,
		Columns: []dbexprprobe.ColumnSpellingColumn{{Key: "k", Name: "amount", Table: "t", Statement: statement}},
	}})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, map[string]config.ColumnSpelling{"k": {}})
	c.Assert(refusals, qt.IsNil)
	c.Assert(stream.String(), qt.Equals, separatorWarning("column public.t.amount"))
}

// restartingSession is [textOnlySession] whose transaction the server names
// 17 when it begins and 18 every time after, as it names one a probe ended
// and began again.
type restartingSession struct{ textOnlySession }

func (s restartingSession) WithRolledBackTransaction(ctx context.Context, label string, body func(context.Context, *sql.Tx) error) (bool, error) {
	calls := 0
	watched, err := probetx.Watch(ctx, func(context.Context) (string, error) {
		calls++
		return strconv.Itoa(16 + min(calls, 2)), nil
	})
	if err != nil {
		return false, err
	}
	return s.textOnlySession.WithRolledBackTransaction(watched, label, body)
}

// TestResolveCheckExpressions_StopsWhenAProbeEndsTheTransaction names the
// probe after which the transaction changed and runs no probe after it. The
// first CHECK has no live columns, so it answers without a statement and the
// session alone decides.
func TestResolveCheckExpressions_StopsWhenAProbeEndsTheTransaction(t *testing.T) {
	c := qt.New(t)

	got, err := dbexprprobe.ResolveCheckExpressions(context.Background(), restartingSession{textOnlySession{dialect: platform.SQLServer}},
		[]dbexprprobe.CheckExpressionProbe{
			{Key: "a", Expression: "amount > 0", Object: "CHECK ck_a on t"},
			{Key: "b", Table: "t", Columns: liveColumns, Expression: "amount > 1", Object: "CHECK ck_b on t"},
		})

	c.Assert(err, qt.ErrorIs, probetx.ErrTransactionChanged)
	c.Assert(err, qt.ErrorMatches, `resolve check expressions: CHECK ck_a on t: a probe ended the transaction .* \(transaction 17, now 18\)`)
	c.Assert(got, qt.IsNil)
}

// TestResolveColumnSpellings_StopsWhenAProbeEndsTheTransaction names the table
// after whose probe the transaction changed.
func TestResolveColumnSpellings_StopsWhenAProbeEndsTheTransaction(t *testing.T) {
	c := qt.New(t)
	statement := "CREATE TEMPORARY TABLE t (amount int); COMMIT"

	got, refusals, err := dbexprprobe.ResolveColumnSpellings(context.Background(), restartingSession{textOnlySession{dialect: platform.Postgres}},
		[]dbexprprobe.ColumnSpellingProbe{{
			Table: "t", Schema: "public", Declared: "t", Statement: statement,
			Columns: []dbexprprobe.ColumnSpellingColumn{{Key: "k", Name: "amount", Table: "t", Statement: statement}},
		}})

	c.Assert(err, qt.ErrorIs, probetx.ErrTransactionChanged)
	c.Assert(err, qt.ErrorMatches, `resolve column spellings: table public.t: a probe ended the transaction .*`)
	c.Assert(got, qt.IsNil)
	c.Assert(refusals, qt.IsNil)
}

// recordingExecutor is an Oracle dev database that records each statement it
// is sent and refuses it, as a server refuses a declaration it cannot store.
type recordingExecutor struct{ sent *[]string }

func (recordingExecutor) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: platform.Oracle}
}

func (e recordingExecutor) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	*e.sent = append(*e.sent, query)
	return nil, sql.ErrConnDone
}

func (e recordingExecutor) QueryContext(_ context.Context, query string, _ ...any) (*sql.Rows, error) {
	*e.sent = append(*e.sent, query)
	return nil, sql.ErrConnDone
}

// TestResolveGeneratedExpressions_UncontainedTextIsNotProbed sends nothing for
// a CREATE whose declared expression holds a second statement, and leaves its
// columns out of the answer, so they compare as written.
func TestResolveGeneratedExpressions_UncontainedTextIsNotProbed(t *testing.T) {
	c := qt.New(t)
	ctx, stream := warnedRun()
	var sent []string

	got, err := dbexprprobe.ResolveGeneratedExpressions(ctx, recordingExecutor{sent: &sent}, []dbexprprobe.GeneratedExpressionProbe{{
		Schema: "app", Table: "t", ProbeTable: "p", Generated: []string{"g"},
		Create: "CREATE TABLE p (a NUMBER, g NUMBER GENERATED ALWAYS AS (a + 1)); DROP TABLE x; CREATE TABLE y (z NUMBER)",
	}})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, make(map[string]config.GeneratedExpression))
	c.Assert(sent, qt.IsNil)
	c.Assert(stream.String(), qt.Equals, separatorWarning("generated columns of app.t"))
}

// TestResolveGeneratedExpressions_ContainedTextIsProbed is the control: the
// same table without the second statement is sent, and the server's refusal
// leaves its column unresolved.
func TestResolveGeneratedExpressions_ContainedTextIsProbed(t *testing.T) {
	c := qt.New(t)
	ctx, stream := warnedRun()
	var sent []string
	create := "CREATE TABLE p (a NUMBER, g NUMBER GENERATED ALWAYS AS (a + 1))"

	got, err := dbexprprobe.ResolveGeneratedExpressions(ctx, recordingExecutor{sent: &sent}, []dbexprprobe.GeneratedExpressionProbe{{
		Schema: "app", Table: "t", ProbeTable: "p", Generated: []string{"g"}, Create: create,
	}})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 1)
	c.Assert(sent, qt.DeepEquals, []string{create})
	c.Assert(stream.String(), qt.Equals, "")
}
