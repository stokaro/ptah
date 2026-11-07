//go:build integration

package safety_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

const (
	promptLine = "Apply these schema changes? Type 'YES' to confirm: "
	canceled   = "Schema apply canceled.\n"
	strictMode = "PTAH_ATLAS_STRICT_COMPAT=1"
)

// promptEngine is a server to ask the prompt on: table t holds a short value in a
// varchar(100), and lengthOfC reads back the length the server keeps for it.
type promptEngine struct {
	name      string
	engine    temporalEngine
	lengthOfC string
}

func promptEngines() []promptEngine {
	return []promptEngine{
		{
			name:   "postgres",
			engine: postgresTemporal,
			lengthOfC: "SELECT character_maximum_length FROM information_schema.columns " +
				"WHERE table_schema = current_schema() AND table_name = 't' AND column_name = 'c'",
		},
		{
			name:   "mysql",
			engine: mysqlTemporal,
			lengthOfC: "SELECT character_maximum_length FROM information_schema.columns " +
				"WHERE table_schema = DATABASE() AND table_name = 't' AND column_name = 'c'",
		},
	}
}

const (
	promptTable        = "CREATE TABLE t (id int PRIMARY KEY, c varchar(100))"
	promptNarrowing    = "CREATE TABLE t (id int PRIMARY KEY, c varchar(5))"
	promptSafeAddition = "CREATE TABLE t (id int PRIMARY KEY, c varchar(100), d int)"
)

// newPromptDatabase is a database of its own holding table t and one short value, and
// a desired schema for each kind of plan.
func newPromptDatabase(c *qt.C, engine promptEngine) (conn *dbschema.DatabaseConnection, address, narrowing, safe string) {
	c.Helper()
	conn, address = temporalScratch(c, engine.engine)
	for _, statement := range []string{promptTable, "INSERT INTO t (id, c) VALUES (1, 'ab')"} {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	return conn, address, schemaFile(c, promptNarrowing), schemaFile(c, promptSafeAddition)
}

func lengthOfC(c *qt.C, conn *dbschema.DatabaseConnection, engine promptEngine) int {
	c.Helper()
	var length int
	c.Assert(conn.QueryRowContext(c.Context(), engine.lengthOfC).Scan(&length), qt.IsNil)
	return length
}

// ratingOfPlan is what the prompt owes for a plan `schema plan` rated: a heading and
// each statement above safe. It reads the plan, so the two cannot agree by accident.
func ratingOfPlan(plan planOutcome) string {
	var out strings.Builder
	out.WriteString("Plan rating: destructive\n")
	for i, statement := range plan.Plan.Statements {
		fmt.Fprintf(&out, "Statement %d of %d, %s: %s\n  %s\n", i+1, len(plan.Plan.Statements),
			statement.Severity, statement.Reason, strings.TrimSuffix(strings.Join(strings.Fields(statement.SQL), " "), ";"))
	}
	return out.String()
}

// TestSchemaApplyE2E_ThePromptReadsThePlansRating asks the question the person who
// runs only `schema apply` faces, on a narrowing plan and a safe one. A declined run
// leaves the column as it is, read back from the server.
func TestSchemaApplyE2E_ThePromptReadsThePlansRating(t *testing.T) {
	for _, engine := range promptEngines() {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address, narrowing, safe := newPromptDatabase(c, engine)
			plan := planOf(c, address, narrowing)
			c.Assert(plan.Plan.Statements, qt.HasLen, 1)
			c.Assert(plan.Plan.Statements[0].Severity, qt.Equals, "destructive")
			c.Assert(plan.Plan.Statements[0].Reason, qt.Contains, narrowsReasonPrefix)
			apply := func(schema string) clirun.Result {
				return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Stdin: "no\n"},
					"schema", "apply", "--db-url", address, "--schema-file", schema)
			}

			declined := apply(narrowing)
			c.Assert(declined.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", declined.Stderr))
			c.Assert(declined.Stdout, qt.Contains, "\n"+ratingOfPlan(plan)+promptLine+canceled)
			c.Assert(lengthOfC(c, conn, engine), qt.Equals, 100)

			quiet := apply(safe)
			c.Assert(quiet.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", quiet.Stderr))
			c.Assert(quiet.Stdout, qt.Not(qt.Contains), "Plan rating")
			c.Assert(quiet.Stdout, qt.Contains, promptLine+canceled)
		})
	}
}

// TestSchemaApplyE2E_TheRatingStaysBesideThePrompt holds the runs that read the
// prompt some other way: --json keeps its document, a run with no input fails as it
// does without a rating, and a dry run has no prompt.
func TestSchemaApplyE2E_TheRatingStaysBesideThePrompt(t *testing.T) {
	for _, engine := range promptEngines() {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			_, address, narrowing, _ := newPromptDatabase(c, engine)
			args := []string{"schema", "apply", "--db-url", address, "--schema-file", narrowing}

			asJSON := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Stdin: "no\n"}, append(args, "--json")...)
			c.Assert(asJSON.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", asJSON.Stderr))
			var report struct {
				Outcome string `json:"outcome"`
			}
			c.Assert(json.Unmarshal([]byte(asJSON.Stdout), &report), qt.IsNil, qt.Commentf("stdout:\n%s", asJSON.Stdout))
			c.Assert(report.Outcome, qt.Equals, "canceled")
			c.Assert(asJSON.Stdout, qt.Not(qt.Contains), "Plan rating")
			c.Assert(asJSON.Stderr, qt.Contains, "Plan rating: destructive\n")
			c.Assert(asJSON.Stderr, qt.Contains, promptLine+canceled)

			noInput := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
			c.Assert(noInput.ExitCode, qt.Equals, 2, qt.Commentf("stderr:\n%s", noInput.Stderr))
			c.Assert(noInput.Stdout, qt.Contains, "Plan rating: destructive\n")
			c.Assert(noInput.Stderr, qt.Contains, "read schema apply confirmation: EOF")

			dry := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, append(args, "--dry-run")...)
			c.Assert(dry.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", dry.Stderr))
			c.Assert(dry.Stdout, qt.Not(qt.Contains), "Plan rating")
		})
	}
}

// TestSchemaApplyE2E_YESAndAutoApprovalMeanWhatTheyMean applies the plan the prompt
// rated. The server says whether it ran.
func TestSchemaApplyE2E_YESAndAutoApprovalMeanWhatTheyMean(t *testing.T) {
	tests := []struct {
		name       string
		stdin      string
		flags      []string
		wantRating bool
	}{
		{name: "YES", stdin: "YES\n", wantRating: true},
		{name: "auto-approval", flags: []string{"--auto-approve"}},
	}
	for _, engine := range promptEngines() {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				conn, address, narrowing, _ := newPromptDatabase(c, engine)

				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Stdin: test.stdin},
					append([]string{"schema", "apply", "--db-url", address, "--schema-file", narrowing}, test.flags...)...)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
				c.Assert(strings.Contains(got.Stdout, "Plan rating: destructive\n"), qt.Equals, test.wantRating)
				c.Assert(got.Stdout, qt.Contains, "Schema apply completed successfully.\n")
				c.Assert(lengthOfC(c, conn, engine), qt.Equals, 5)
			})
		}
	}
}

// TestSchemaApplyE2E_APlanFileCarriesItsRating is `schema apply --plan`, where the
// rating comes from the file `schema plan` wrote.
func TestSchemaApplyE2E_APlanFileCarriesItsRating(t *testing.T) {
	for _, engine := range promptEngines() {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address, narrowing, safe := newPromptDatabase(c, engine)
			applyPlanFile := func(schema string) clirun.Result {
				path := filepath.Join(c.TempDir(), "change.plan.json")
				saved := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					"schema", "plan", "--db-url", address, "--schema-file", schema, "--output", path)
				c.Assert(saved.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", saved.Stderr))
				return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Stdin: "no\n"},
					"schema", "apply", "--db-url", address, "--plan", path)
			}

			declined := applyPlanFile(narrowing)
			c.Assert(declined.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", declined.Stderr))
			c.Assert(declined.Stdout, qt.Contains, "\n"+ratingOfPlan(planOf(c, address, narrowing))+promptLine+canceled)
			c.Assert(lengthOfC(c, conn, engine), qt.Equals, 100)

			quiet := applyPlanFile(safe)
			c.Assert(quiet.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", quiet.Stderr))
			c.Assert(quiet.Stdout, qt.Not(qt.Contains), "Plan rating")
		})
	}
}

// atlasAddress is an address in the host:port form an Atlas URL takes.
func atlasAddress(address string) string {
	return strings.NewReplacer("@tcp(", "@", ")/", "/").Replace(address)
}

// TestCompatSchemaApplyE2E_TheRatingStaysOffStandardOutput is the compatibility
// surface, with a second database as the desired state, which needs no dev database.
// The default says the rating on standard error, and strict mode says nothing. Standard
// output holds the same bytes in both, and those are the ones the contract covers.
func TestCompatSchemaApplyE2E_TheRatingStaysOffStandardOutput(t *testing.T) {
	for _, engine := range promptEngines() {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address, _, _ := newPromptDatabase(c, engine)
			narrow, narrowAddress := temporalScratch(c, engine.engine)
			_, err := narrow.ExecContext(c.Context(), promptNarrowing)
			c.Assert(err, qt.IsNil)
			args := []string{"schema", "apply", "-u", atlasAddress(address), "--to", atlasAddress(narrowAddress)}

			full := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Stdin: "no\n"}, args...)
			c.Assert(full.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", full.Stderr))
			c.Assert(full.Stdout, qt.Contains, promptLine+canceled)
			c.Assert(full.Stdout, qt.Not(qt.Contains), "Plan rating")
			c.Assert(full.Stderr, qt.Contains, "Plan rating: destructive\n")
			c.Assert(full.Stderr, qt.Contains, narrowsReasonPrefix)

			strict := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Stdin: "no\n", Env: append(os.Environ(), strictMode)}, args...)
			c.Assert(strict.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", strict.Stderr))
			c.Assert(strict.Stdout, qt.Equals, full.Stdout)
			c.Assert(strict.Stderr, qt.Not(qt.Contains), "Plan rating")
			c.Assert(lengthOfC(c, conn, engine), qt.Equals, 100)
		})
	}
}
