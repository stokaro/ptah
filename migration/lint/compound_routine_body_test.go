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
