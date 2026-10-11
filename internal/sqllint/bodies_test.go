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
				sqllint.Options{Dialect: platform.Postgres, Bodies: countingBodies{calls: &calls, bodies: &bodies}})

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
		sqllint.Options{Dialect: platform.Postgres, Bodies: withoutReader})

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
		sqllint.Options{Dialect: platform.Postgres})

	c.Assert(err, qt.ErrorMatches, `sql lint reads the routine bodies the file defines and was given no routine body reader`)
	c.Assert(findings, qt.IsNil)
}
