package schema_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/testutils"
	"ptah.run/migration/safety"
)

const (
	destructivePlanOutput = "Planned schema changes:\n" +
		"DROP TABLE \"legacy\";\n" +
		"Plan rating: destructive\n" +
		"Statement 1 of 1, destructive: DROP TABLE removes the table and all rows\n" +
		"  DROP TABLE \"legacy\"\n" +
		"Apply these schema changes? Type 'YES' to confirm: "
	safePlanOutput = "Planned schema changes:\n" +
		"CREATE TABLE \"orders\" (\n  \"id\" INTEGER PRIMARY KEY\n);\n" +
		"Apply these schema changes? Type 'YES' to confirm: "
)

// promptFixture is a database holding one table beyond users, and two desired
// schemas: one that drops that table and one that adds another.
type promptFixture struct {
	dir, dbPath, dbURL string
	dropping, adding   string
}

func newPromptFixture(c *qt.C) promptFixture {
	c.Helper()
	dir := c.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE legacy (id INTEGER PRIMARY KEY);")
	return promptFixture{
		dir:      dir,
		dbPath:   dbPath,
		dbURL:    "sqlite://" + dbPath,
		dropping: writeSchemaSQLFile(c, dir, "dropping.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		adding: writeSchemaSQLFile(c, dir, "adding.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE legacy (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n"),
	}
}

func (f promptFixture) savePlan(c *qt.C, schemaPath string) string {
	c.Helper()
	planPath := filepath.Join(f.dir, "saved.plan.json")
	out, err := runSchema("", "plan", "--db-url", f.dbURL, "--schema-file", schemaPath, "--output", planPath)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return planPath
}

// TestSchemaApplyPromptCarriesTheRating is the person who runs only `schema apply`.
// The rating stands between the plan and the prompt, and a declined run changes
// nothing, as a declined run does without one.
func TestSchemaApplyPromptCarriesTheRating(t *testing.T) {
	tests := []struct {
		name   string
		schema func(promptFixture) string
		want   string
	}{
		{
			name:   "a destructive plan",
			schema: func(f promptFixture) string { return f.dropping },
			want:   destructivePlanOutput + "Schema apply canceled.\n",
		},
		{
			name:   "a safe plan keeps the prompt as it is",
			schema: func(f promptFixture) string { return f.adding },
			want:   safePlanOutput + "Schema apply canceled.\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newPromptFixture(c)

			stdout, stderr, err := runSchemaStreamsWithInput("no\n", "apply", "--db-url", f.dbURL, "--schema-file", test.schema(f))

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, test.want)
			c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"legacy", "users"})
		})
	}
}

// TestSchemaApplyPromptRatingFromAPlanFile rates a plan file as it stands.
func TestSchemaApplyPromptRatingFromAPlanFile(t *testing.T) {
	tests := []struct {
		name   string
		schema func(promptFixture) string
		want   string
	}{
		{
			name:   "a destructive plan",
			schema: func(f promptFixture) string { return f.dropping },
			want:   destructivePlanOutput,
		},
		{
			name:   "a safe plan",
			schema: func(f promptFixture) string { return f.adding },
			want:   safePlanOutput,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newPromptFixture(c)
			planPath := f.savePlan(c, test.schema(f))

			stdout, stderr, err := runSchemaStreamsWithInput("no\n", "apply", "--db-url", f.dbURL, "--plan", planPath)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, test.want+"Schema apply canceled.\n")
			c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"legacy", "users"})
		})
	}
}

// TestSchemaApplyPromptRatingOfAPlanRecordedAsSafe lowers the verdict in the file, as
// a hand might. The prompt stays loud, since the statement says what it does.
func TestSchemaApplyPromptRatingOfAPlanRecordedAsSafe(t *testing.T) {
	c := qt.New(t)
	f := newPromptFixture(c)
	planPath := f.savePlan(c, f.dropping)
	contents, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	var plan atlasschema.PlanFile
	c.Assert(json.Unmarshal(contents, &plan), qt.IsNil)
	for i := range plan.Statements {
		plan.Statements[i].Severity = safety.Safe
		plan.Statements[i].Reason = "edited down"
	}
	plan.Destructive = false
	rewritten, err := json.Marshal(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(planPath, rewritten, 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaStreamsWithInput("no\n", "apply", "--db-url", f.dbURL, "--plan", planPath)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Equals, destructivePlanOutput+"Schema apply canceled.\n")
}

// TestSchemaApplyRatingLeavesTheRestOfTheRunAlone holds what the prompt's rating must
// not move: an auto-approved run prints none and applies, `YES` still applies, a dry
// run has no prompt to stand beside, and a run with no input fails as it does without
// a rating, with the rating already on the screen.
func TestSchemaApplyRatingLeavesTheRestOfTheRunAlone(t *testing.T) {
	const (
		planned = "Planned schema changes:\nDROP TABLE \"legacy\";\n"
		done    = "Schema apply completed successfully.\n"
	)
	tests := []struct {
		name       string
		input      string
		flags      []string
		wantStdout string
		wantTables []string
	}{
		{
			name:       "auto-approval",
			flags:      []string{"--auto-approve"},
			wantStdout: planned + "Auto-approval enabled; applying schema changes.\n" + done,
			wantTables: []string{"users"},
		},
		{
			name:       "YES",
			input:      "YES\n",
			wantStdout: destructivePlanOutput + "\n" + done,
			wantTables: []string{"users"},
		},
		{
			name:       "a dry run",
			flags:      []string{"--dry-run"},
			wantStdout: planned,
			wantTables: []string{"legacy", "users"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newPromptFixture(c)

			stdout, stderr, err := runSchemaStreamsWithInput(test.input, append([]string{"apply", "--db-url", f.dbURL, "--schema-file", f.dropping}, test.flags...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, test.wantStdout)
			c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, test.wantTables)
		})
	}
}

func TestSchemaApplyRatingOnARunWithNoInput(t *testing.T) {
	c := qt.New(t)
	f := newPromptFixture(c)

	stdout, stderr, err := runSchemaStreamsWithInput("", "apply", "--db-url", f.dbURL, "--schema-file", f.dropping)

	c.Assert(err, qt.ErrorMatches, `read schema apply confirmation: EOF`, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Equals, destructivePlanOutput)
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"legacy", "users"})
}

// TestSchemaApplyJSONKeepsTheRatingOffTheDocument keeps standard output one document:
// the rating goes where the prompt goes, and the document holds what it holds.
func TestSchemaApplyJSONKeepsTheRatingOffTheDocument(t *testing.T) {
	c := qt.New(t)
	f := newPromptFixture(c)

	stdout, stderr, err := runSchemaStreamsWithInput("no\n", "apply", "--db-url", f.dbURL, "--schema-file", f.dropping, "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeCanceled)
	c.Assert(stdout, qt.Not(qt.Contains), "Plan rating")
	c.Assert(stderr, qt.Equals, destructivePlanOutput+"Schema apply canceled.\n")
}

// TestSchemaApplyPromptRatesTheEditedText rates what the editor returned, since that
// is what `YES` approves.
func TestSchemaApplyPromptRatesTheEditedText(t *testing.T) {
	testutils.SkipWithoutPOSIXShell(t)
	c := qt.New(t)
	f := newPromptFixture(c)
	editor := filepath.Join(c.TempDir(), "editor.sh")
	c.Assert(os.WriteFile(editor, []byte("#!/bin/sh\nprintf 'DROP TABLE users;\\n' >> \"$1\"\n"), 0o700), qt.IsNil) // #nosec G306 -- the test's editor must be executable
	c.Setenv("VISUAL", "")
	c.Setenv("EDITOR", editor)

	stdout, stderr, err := runSchemaStreamsWithInput("no\n", "apply", "--db-url", f.dbURL, "--schema-file", f.adding, "--edit")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "Plan rating: destructive\n")
	c.Assert(stdout, qt.Contains, "Statement 2 of 2, destructive: DROP TABLE removes the table and all rows\n  DROP TABLE users\n")
}
