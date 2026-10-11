package sqllint_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/routineext"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqllint"
)

// countingBodies counts the calls and the bodies it answers, passing each call
// to the bundled runtime.
type countingBodies struct {
	calls  *int
	bodies *int
}

func (b countingBodies) ReadRoutineBodies(ctx context.Context, request routineext.ReadRequest) (routineext.ReadResult, error) {
	*b.calls++
	*b.bodies += len(request.Bodies)
	return builtintest.Runtime().ReadRoutineBodies(ctx, request)
}

// twoRoutines is a file defining a procedure and a function that each compose
// SQL at run time, and a table.
const twoRoutines = "CREATE PROCEDURE p() AS $$\nBEGIN\nEXECUTE 'TRUNCATE a';\nEND;\n$$ LANGUAGE plpgsql;\n" +
	"CREATE TABLE t (id int PRIMARY KEY);\n" +
	"CREATE FUNCTION f() RETURNS void AS $$\nBEGIN\nEXECUTE 'TRUNCATE b';\nEND;\n$$ LANGUAGE plpgsql;\n"

// The routine bodies of a file are read in one call before any rule runs, and
// a file that defines none asks nothing.
func TestLintSource_ReadsEveryBodyInOneCall(t *testing.T) {
	for _, test := range []struct {
		name       string
		sql        string
		wantCalls  int
		wantBodies int
		wantRules  []string
	}{
		{name: "two routines", sql: twoRoutines, wantCalls: 1, wantBodies: 2,
			wantRules: []string{sqllint.RuleUnsupportedStatement, sqllint.RuleDynamicSQL, sqllint.RuleUnsupportedStatement, sqllint.RuleDynamicSQL}},
		{name: "no routine", sql: "CREATE TABLE t (id int PRIMARY KEY);\n", wantCalls: 0, wantBodies: 0, wantRules: make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls, bodies := 0, 0

			findings, err := sqllint.LintSource(c.Context(), sqllint.Source{Name: "m.sql", SQL: test.sql},
				sqllint.Options{SQL: builtintest.Runtime().SQL(), Dialect: platform.Postgres, Bodies: countingBodies{calls: &calls, bodies: &bodies}})

			c.Assert(err, qt.IsNil)
			c.Assert([]int{calls, bodies}, qt.DeepEquals, []int{test.wantCalls, test.wantBodies})
			c.Assert(rulesOf(findings), qt.DeepEquals, test.wantRules)
		})
	}
}

// A target whose owner reads no bodies leaves the dynamic SQL rule nothing to
// read, as a dialect without one always has; the routines are still reported
// as statements the linter does not model.
func TestLintSource_AnOwnerThatReadsNoBodies(t *testing.T) {
	c := qt.New(t)
	withoutReader := must.Must(engine.New(engine.Provider{ID: "example.org/postgres", Targets: []engine.Target{{Name: "postgres"}}}))

	findings, err := sqllint.LintSource(c.Context(), sqllint.Source{Name: "m.sql", SQL: twoRoutines},
		sqllint.Options{SQL: builtintest.Runtime().SQL(), Dialect: platform.Postgres, Bodies: withoutReader})

	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{sqllint.RuleUnsupportedStatement, sqllint.RuleUnsupportedStatement})
}

// rulesOf lists the rules of findings, in order.
func rulesOf(findings []sqllint.Finding) []string {
	rules := make([]string, 0, len(findings))
	for _, finding := range findings {
		rules = append(rules, finding.Rule)
	}
	return rules
}

// A file whose routines need reading is refused without a reader, because the
// dynamic SQL rule would otherwise report nothing, and a routine that composes
// SQL would read as one that does not.
func TestLintSource_RoutinesWithoutAReader_FailurePath(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(c.Context(), sqllint.Source{Name: "m.sql", SQL: twoRoutines},
		sqllint.Options{SQL: builtintest.Runtime().SQL(), Dialect: platform.Postgres})

	c.Assert(err, qt.ErrorMatches, `sql lint reads the routine bodies the file defines and was given no routine body reader`)
	c.Assert(findings, qt.IsNil)
}

// Only a body the parser attached as a PostgreSQL routine body is read. On
// another target a CREATE FUNCTION carries its text and no routine body, and
// a reader that target registers is not handed it, since nothing says the
// text is in its language.
func TestLintSource_AFunctionWithNoRoutineBodyIsNotRead(t *testing.T) {
	c := qt.New(t)
	var asked []routineext.ReadRequest
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/oracle", Targets: []engine.Target{
		{Name: "oracle", RoutineBodies: recordingReader{asked: &asked}}}}))

	findings, err := sqllint.LintSource(c.Context(), sqllint.Source{Name: "m.sql",
		SQL: "CREATE FUNCTION f() RETURNS integer AS $$ EXECUTE 'x' $$ LANGUAGE sql;"},
		sqllint.Options{SQL: builtintest.Runtime().SQL(), Dialect: platform.Oracle, Bodies: runtime})

	c.Assert(err, qt.IsNil)
	c.Assert(asked, qt.HasLen, 0)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{sqllint.RuleUnsupportedStatement})
}

// recordingReader records each request and reads every body as one dynamic
// statement carrying label.
type recordingReader struct {
	asked *[]routineext.ReadRequest
	label string
}

func (r recordingReader) ReadBodies(_ context.Context, request routineext.ReadRequest) (routineext.ReadResult, error) {
	*r.asked = append(*r.asked, request)
	result := routineext.ReadResult{}
	for _, body := range request.Bodies {
		result.Bodies = append(result.Bodies, routineext.Reading{
			Statements: []routineext.Statement{{Kind: routineext.StatementDynamic, Label: r.label, SQL: body.SQL}},
		})
	}
	return result, nil
}

// A reader names the statement that composes SQL when it labels one, and the
// finding names the routine alone when it does not: the label is optional in
// the contract.
func TestLintSource_ADynamicStatementIsNamedByItsLabel(t *testing.T) {
	for _, test := range []struct {
		name  string
		label string
		want  string
	}{
		{name: "labeled", label: "execute", want: "procedure p builds SQL at run time with EXECUTE"},
		{name: "unlabeled", label: "", want: "procedure p builds SQL at run time"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var asked []routineext.ReadRequest
			runtime := must.Must(engine.New(engine.Provider{ID: "example.org/postgres", Targets: []engine.Target{
				{Name: "postgres", RoutineBodies: recordingReader{asked: &asked, label: test.label}}}}))

			findings, err := sqllint.LintSource(c.Context(), sqllint.Source{Name: "m.sql",
				SQL: "CREATE PROCEDURE p() AS $$\nBEGIN\nEXECUTE 'TRUNCATE a';\nEND;\n$$ LANGUAGE plpgsql;\n"},
				sqllint.Options{SQL: builtintest.Runtime().SQL(), Dialect: platform.Postgres, Bodies: runtime})

			c.Assert(err, qt.IsNil)
			c.Assert(asked, qt.HasLen, 1)
			c.Assert(rulesOf(findings), qt.DeepEquals, []string{sqllint.RuleUnsupportedStatement, sqllint.RuleDynamicSQL})
			c.Assert(findings[1].Message, qt.Equals, test.want)
		})
	}
}
