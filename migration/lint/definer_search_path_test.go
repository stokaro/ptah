package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrationfile"
)

// TestAnalyzeFS_ADefinerRoutineWithoutSearchPathIsReported pins the rule and
// the two traps a word scan falls into.
//
// A SECURITY DEFINER routine runs with its owner's privileges and resolves
// unqualified names through whatever the CALLER set, so a caller who puts a
// schema of their own first decides which table the owner's routine writes to
// (stokaro/ptah#2356).
func TestAnalyzeFS_ADefinerRoutineWithoutSearchPathIsReported(t *testing.T) {
	rows := []struct {
		name     string
		sql      string
		wantRule []string
	}{
		{
			name:     "a definer function that pins nothing",
			sql:      "CREATE FUNCTION f() RETURNS int LANGUAGE sql SECURITY DEFINER AS $$ SELECT 1 $$;\n",
			wantRule: []string{"AC101", "PG312P"},
		},
		{
			// The control: the remedy the rule asks for silences it.
			name:     "the same routine, pinned",
			sql:      "CREATE FUNCTION f() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$ SELECT 1 $$;\n",
			wantRule: []string{"AC101"},
		},
		{
			// The trap: a BEGIN ATOMIC body is words, and this one carries its
			// own SET while the header pins nothing.
			name: "a definer routine whose BODY contains SET",
			sql: "CREATE FUNCTION f() RETURNS void LANGUAGE SQL SECURITY DEFINER\n" +
				"BEGIN ATOMIC UPDATE t SET n = 1; END;\n",
			wantRule: []string{"AC101", "PG312P"},
		},
		{
			// The other side of the same trap: a dollar-quoted body carrying
			// SECURITY DEFINER must not make the rule fire on a header that
			// says nothing of the kind.
			name: "an invoker routine whose body mentions SECURITY DEFINER",
			sql: "CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $$\n" +
				"BEGIN RAISE NOTICE 'SECURITY DEFINER'; RETURN 1; END;\n$$;\n",
			wantRule: []string{"AC101"},
		},
		{
			name:     "a procedure, which has the same hazard",
			sql:      "CREATE PROCEDURE p() LANGUAGE sql SECURITY DEFINER AS $$ SELECT 1 $$;\n",
			wantRule: []string{"AC101", "PG312P"},
		},
		{
			name:     "an invoker routine",
			sql:      "CREATE FUNCTION f() RETURNS int LANGUAGE sql SECURITY INVOKER AS $$ SELECT 1 $$;\n",
			wantRule: []string{"AC101"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := lint.AnalyzeFS(routineFS(row.sql), lint.Options{
				DirFormat: migrationfile.DirFormatAtlas,
				Dialect:   "postgres",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(findingRules(analysis.Findings()), qt.DeepEquals, row.wantRule)
		})
	}
}

// TestAnalyzeFS_TheDefinerRuleIsPostgresOnly pins the dialect scope.
//
// Rule.Dialects is never validated, so a misspelling matches nothing and fails
// nothing; a row per dialect is what catches it.
func TestAnalyzeFS_TheDefinerRuleIsPostgresOnly(t *testing.T) {
	const sql = "CREATE PROCEDURE p() LANGUAGE sql SECURITY DEFINER AS $$ SELECT 1 $$;\n"

	rows := []struct {
		dialect  string
		wantRule []string
	}{
		{dialect: "postgres", wantRule: []string{"AC101", "PG312P"}},
		{dialect: "mysql", wantRule: []string{"AC101"}},
	}

	for _, row := range rows {
		t.Run(row.dialect, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := lint.AnalyzeFS(routineFS(sql), lint.Options{
				DirFormat: migrationfile.DirFormatAtlas,
				Dialect:   row.dialect,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(findingRules(analysis.Findings()), qt.DeepEquals, row.wantRule)
		})
	}
}

// TestAnalyzeFS_APinnedRoutineStaysInTheChangeModel pins what the parser
// refusal cost.
//
// statementSchemaChanges drops a statement the parser cannot model, so pinning
// a search_path -- the remedy PRV02 asks for -- deleted the function from the
// change model, silently, while the run exited 0 (stokaro/ptah#2356).
func TestAnalyzeFS_APinnedRoutineStaysInTheChangeModel(t *testing.T) {
	rows := []struct {
		name string
		sql  string
	}{
		{
			name: "a function that pins its search_path",
			sql:  "CREATE FUNCTION f() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$ SELECT 1 $$;\n",
		},
		{
			// The control: the same function without the clause was always
			// modeled, so a green row above must not come from both being zero.
			name: "the same function without one",
			sql:  "CREATE FUNCTION f() RETURNS int LANGUAGE sql SECURITY DEFINER AS $$ SELECT 1 $$;\n",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := lint.AnalyzeFS(routineFS(row.sql), lint.Options{
				DirFormat: migrationfile.DirFormatAtlas,
				Dialect:   "postgres",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(analysis.Files()[0].Changes, qt.HasLen, 1,
				qt.Commentf("changes: %#v", analysis.Files()[0].Changes))
		})
	}
}
