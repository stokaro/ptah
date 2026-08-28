package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrationfile"
)

// TestAnalyzeFS_AnImmutableRoutineThatIsNotIsReported pins the contradiction
// Ptah carried both halves of and related nowhere.
//
// The tree states the hazard twice, in the two packages that had to decide what
// the value means off PostgreSQL: a DETERMINISTIC function is what a
// function-based index may be built from, and IMMUTABLE misstated "is a lie the
// server acts on". internal/mysqlroutine also records that the engine does not
// defend itself (stokaro/ptah#2357).
func TestAnalyzeFS_AnImmutableRoutineThatIsNotIsReported(t *testing.T) {
	rows := []struct {
		name     string
		sql      string
		dialect  string
		wantRule []string
	}{
		{
			name:     "an IMMUTABLE function reading the clock",
			sql:      "CREATE FUNCTION stamp() RETURNS timestamptz LANGUAGE SQL IMMUTABLE AS $$ SELECT now() $$;\n",
			dialect:  "postgres",
			wantRule: []string{"AC101", "DD102"},
		},
		{
			name:     "a DETERMINISTIC function reading the clock",
			sql:      "CREATE FUNCTION f() RETURNS datetime DETERMINISTIC BEGIN RETURN NOW(); END;\n",
			dialect:  "mysql",
			wantRule: []string{"AC101", "DD102"},
		},
		{
			// A dollar-quoted body is ONE word, so a rule scanning Words would
			// answer differently depending on how the author quoted it.
			name:     "the body is dollar-quoted",
			sql:      "CREATE FUNCTION f() RETURNS timestamptz LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN clock_timestamp(); END; $$;\n",
			dialect:  "postgres",
			wantRule: []string{"AC101", "DD102"},
		},
		{
			// NOT DETERMINISTIC is the opposite promise, not a weaker one.
			name:     "NOT DETERMINISTIC promises nothing",
			sql:      "CREATE FUNCTION f() RETURNS datetime NOT DETERMINISTIC BEGIN RETURN NOW(); END;\n",
			dialect:  "mysql",
			wantRule: []string{"AC101"},
		},
		{
			name:     "an immutable routine that calls nothing of the kind",
			sql:      "CREATE FUNCTION double(a int) RETURNS int LANGUAGE SQL IMMUTABLE AS $$ SELECT a * 2 $$;\n",
			dialect:  "postgres",
			wantRule: []string{"AC101"},
		},
		{
			// The literal control. A string literal is one verbatim token, so
			// the call named inside it is not a call.
			name:     "the call appears only inside a string literal",
			sql:      "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN EXECUTE 'SELECT now()'; END; $$;\n",
			dialect:  "postgres",
			wantRule: []string{"AC101"},
		},
		{
			// The comment control. Comments are dropped before the scan.
			name:     "the call appears only in a comment",
			sql:      "CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN -- now()\n RETURN 1; END; $$;\n",
			dialect:  "postgres",
			wantRule: []string{"AC101"},
		},
		{
			// A VOLATILE routine promises nothing, so it may read the clock.
			name:     "a volatile routine reading the clock",
			sql:      "CREATE FUNCTION stamp() RETURNS timestamptz LANGUAGE SQL VOLATILE AS $$ SELECT now() $$;\n",
			dialect:  "postgres",
			wantRule: []string{"AC101"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := lint.AnalyzeFS(routineFS(row.sql), lint.Options{
				DirFormat: migrationfile.DirFormatAtlas,
				Dialect:   row.dialect,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(findingRules(analysis.Findings()), qt.DeepEquals, row.wantRule)
		})
	}
}

// TestAnalyzeFS_WithoutADialectTheRoutineRuleSaysSo pins the reason the routine
// body is a third rule input rather than a detail of the second.
//
// Parsing a body needs a dialect. Without one the body is not parsed, the rule
// finds nothing, and the run exits 0 -- a smaller report that reads as a clean
// one, which is the failure RuleInput exists to prevent.
func TestAnalyzeFS_WithoutADialectTheRoutineRuleSaysSo(t *testing.T) {
	const sql = "CREATE FUNCTION stamp() RETURNS timestamptz LANGUAGE SQL IMMUTABLE AS $$ SELECT now() $$;\n"

	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(routineFS(sql), lint.Options{DirFormat: migrationfile.DirFormatAtlas})

	c.Assert(err, qt.IsNil)
	c.Assert(unmetRules(analysis.UnmetInputs()), qt.Contains, "DD102")
}

// TestAnalyzeFS_WithADialectNothingIsUnmet is the control the test above needs:
// a rule that always reports its input as unmet says nothing about the run.
func TestAnalyzeFS_WithADialectNothingIsUnmet(t *testing.T) {
	const sql = "CREATE FUNCTION stamp() RETURNS timestamptz LANGUAGE SQL IMMUTABLE AS $$ SELECT now() $$;\n"

	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(routineFS(sql), lint.Options{
		DirFormat: migrationfile.DirFormatAtlas,
		Dialect:   "postgres",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(unmetRules(analysis.UnmetInputs()), qt.Not(qt.Contains), "DD102")
}

// unmetRules lists the rules an analysis reported as missing an input.
func unmetRules(unmet []lint.UnmetInput) []string {
	rules := make([]string, 0, len(unmet))
	for _, input := range unmet {
		rules = append(rules, input.Rule)
	}
	return rules
}
