package lint_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrationfile"
)

// TestAnalyzeFS_ACompoundRoutineBodyIsOneStatement pins the first half of
// stokaro/ptah#2069.
//
// A routine whose body carries its own statement terminators was cut at the
// first one. Neither fragment parsed -- `CREATE PROCEDURE p() BEGIN SELECT 1`
// is not a statement and neither is `END` -- so the routine contributed no
// change at all, while the same routine written with a single-statement body
// contributed one. A migration that creates a procedure reported nothing
// created.
func TestAnalyzeFS_ACompoundRoutineBodyIsOneStatement(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		sql     string
		object  string
	}{
		{
			name:    "mysql procedure",
			dialect: "mysql",
			sql:     "CREATE PROCEDURE purge()\nBEGIN\n  DELETE FROM audit;\n  DELETE FROM sessions;\nEND;\n",
			object:  "purge",
		},
		{
			name:    "mysql function with a declaration",
			dialect: "mysql",
			sql:     "CREATE FUNCTION total() RETURNS INT DETERMINISTIC\nBEGIN\n  DECLARE n INT;\n  SET n = 1;\n  RETURN n;\nEND;\n",
			object:  "total",
		},
		{
			name:    "sqlserver procedure with an AS body",
			dialect: "sqlserver",
			sql:     "CREATE PROCEDURE purge AS\nBEGIN\n  DELETE FROM audit;\n  DELETE FROM sessions;\nEND;\n",
			object:  "purge",
		},
		{
			name:    "postgres dollar-quoted body, which was never cut",
			dialect: "postgres",
			sql:     "CREATE FUNCTION total() RETURNS int AS $$\nBEGIN\n  PERFORM 1;\n  RETURN 1;\nEND;\n$$ LANGUAGE plpgsql;\n",
			object:  "total",
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
			file := analysis.Files()[0]
			c.Assert(file.Statements, qt.HasLen, 1,
				qt.Commentf("statements: %#v", file.Statements))
			c.Assert(changeObjects(file.Changes), qt.DeepEquals, []string{row.object})
		})
	}
}

// TestAnalyzeFS_ARoutineBodyIsNotAMigrationStatement pins the second half, and
// it is the half that reported something untrue rather than nothing.
//
// Cutting the body left every statement after the first standing alone, where
// the rules read it as a step the migration performs. A DROP TABLE inside a
// procedure body -- which runs when somebody calls the procedure, and never at
// migration time -- was reported as a destructive migration.
func TestAnalyzeFS_ARoutineBodyIsNotAMigrationStatement(t *testing.T) {
	rows := []struct {
		name     string
		sql      string
		wantRule []string
	}{
		{
			name:     "a drop inside a routine body",
			sql:      "CREATE PROCEDURE purge()\nBEGIN\n  DELETE FROM audit;\n  DROP TABLE users;\nEND;\n",
			wantRule: nil,
		},
		{
			name:     "a drop the migration really performs, after the routine",
			sql:      "CREATE PROCEDURE purge()\nBEGIN\n  DELETE FROM audit;\n  DROP TABLE sessions;\nEND;\n\nDROP TABLE users;\n",
			wantRule: []string{"DS101"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(routineFS(row.sql), lint.Options{
				DirFormat: migrationfile.DirFormatAtlas,
				Dialect:   "mysql",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(findingRules(analysis.Findings()), qt.DeepEquals, row.wantRule)
		})
	}
}

// routineFS is one up migration holding the given SQL.
func routineFS(sql string) fstest.MapFS {
	return fstest.MapFS{"1_routine.sql": &fstest.MapFile{Data: []byte(sql)}}
}

// changeObjects lists the objects a file's changes name.
func changeObjects(changes []lint.SchemaChange) []string {
	objects := make([]string, 0, len(changes))
	for _, change := range changes {
		objects = append(objects, change.Object)
	}
	return objects
}

// findingRules lists the rules a run reported, nil when it reported none.
func findingRules(findings []lint.Finding) []string {
	if len(findings) == 0 {
		return nil
	}
	rules := make([]string, 0, len(findings))
	for _, finding := range findings {
		rules = append(rules, finding.Rule)
	}
	return rules
}

// TestAnalyzeFS_DS106ReadsTheStatementAndNotTheBody pins the anchor.
//
// DS106 was the one built-in scan with no statement anchor: it walked every
// word a statement carried, and a routine body is words. Creating a function
// whose body deletes from pg_enum was reported as an enum-value removal, at
// error severity, in the one family the apply gate refuses on -- while the
// `$$`-quoted spelling of the same body reported nothing, because a
// dollar-quoted body is one token. The rule's answer depended on how the author
// quoted a body it had no business reading (stokaro/ptah#2358).
func TestAnalyzeFS_DS106ReadsTheStatementAndNotTheBody(t *testing.T) {
	rows := []struct {
		name     string
		sql      string
		wantRule []string
	}{
		{
			// The row the issue is about: tokenized body, unanchored scan.
			name:     "a pg_enum delete inside a BEGIN ATOMIC body",
			sql:      "CREATE FUNCTION f() RETURNS void LANGUAGE SQL\nBEGIN ATOMIC DELETE FROM pg_enum; END;\n",
			wantRule: nil,
		},
		{
			// The same body the parser hands over as one token. It reported
			// nothing before and must still report nothing, so that the fix is
			// an agreement between the two spellings rather than a new
			// disagreement.
			name:     "the same body dollar-quoted",
			sql:      "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN DELETE FROM pg_enum; END; $$;\n",
			wantRule: nil,
		},
		{
			// The control that keeps the anchor from being a blanket silence.
			name:     "a pg_enum delete the migration really performs",
			sql:      "DELETE FROM pg_enum WHERE enumlabel = 'legacy';\n",
			wantRule: []string{"DS106"},
		},
		{
			// The second form, which belongs to ALTER TYPE and is read there.
			name:     "an enum value dropped by ALTER TYPE",
			sql:      "ALTER TYPE mood DROP VALUE 'legacy';\n",
			wantRule: []string{"DS106"},
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

// TestAnalyzeFS_DS107CoversEveryObjectItClaims pins the keyword list against
// the summary the reference page publishes.
//
// Dropping a procedure or a trigger removes behavior a caller depends on
// exactly as dropping a function does, and DS107 is the family that decides
// whether an apply proceeds. Both went unreported (stokaro/ptah#2358).
//
// Every keyword the rule claims has a row, so widening the list cannot silently
// drop one of the six it already covered.
func TestAnalyzeFS_DS107CoversEveryObjectItClaims(t *testing.T) {
	rows := []struct {
		name     string
		sql      string
		wantRule []string
	}{
		{name: "procedure", sql: "DROP PROCEDURE p();\n", wantRule: []string{"DS107"}},
		{name: "trigger", sql: "DROP TRIGGER t ON users;\n", wantRule: []string{"DS107"}},
		{name: "function", sql: "DROP FUNCTION f();\n", wantRule: []string{"DS107"}},
		{name: "type", sql: "DROP TYPE mood;\n", wantRule: []string{"DS107"}},
		{name: "extension", sql: "DROP EXTENSION hstore;\n", wantRule: []string{"DS107"}},
		{name: "role", sql: "DROP ROLE reporting;\n", wantRule: []string{"DS107"}},
		{name: "policy", sql: "DROP POLICY p ON users;\n", wantRule: []string{"DS107"}},
		{name: "schema", sql: "DROP SCHEMA staging;\n", wantRule: []string{"DS107"}},
		{
			// Not asked for by the issue, and measured as silent rather than
			// assumed: whether a view joins the list is a decision.
			name: "view, which the rule does not claim", sql: "DROP VIEW v;\n", wantRule: nil,
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
