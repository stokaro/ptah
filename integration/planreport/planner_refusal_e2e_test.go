//go:build integration

// Package planreport_test holds what the documents of `schema plan --json` and
// `schema apply --json` report for a plan the planner refuses over what a real
// server holds. The answers come from the shipped binary, and the server is read
// back to show that the refusal came before anything reached it.
package planreport_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // the driver the fixture and the read back open with

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// TestMain runs the tests through clirun.Main, which removes the programs
// clirun.Build compiled for them when they end.
func TestMain(m *testing.M) {
	clirun.Main(m)
}

// viewOption is one entry of a refusal's view_options, read by its wire names.
type viewOption struct {
	View   string `json:"view"`
	Option string `json:"option"`
}

// document is what a caller parses from either command's standard output.
type document struct {
	ContractVersion int    `json:"contract_version"`
	Outcome         string `json:"outcome"`
	Refusal         *struct {
		Code        string       `json:"code"`
		ViewOptions []viewOption `json:"view_options"`
	} `json:"refusal"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// verb is one command that reports a plan, as an operator runs it. Only the plan
// saves a file, at the path args is given.
type verb struct {
	name     string
	args     func(output string) []string
	contract int
}

var verbs = []verb{
	{
		name:     "schema plan",
		args:     func(output string) []string { return []string{"schema", "plan", "--output", output} },
		contract: atlasschema.PlanReportContractVersion,
	},
	{
		name:     "schema apply",
		args:     func(string) []string { return []string{"schema", "apply", "--auto-approve"} },
		contract: atlasschema.ApplyReportContractVersion,
	},
}

// database creates a database of its own holding statements, and returns the
// address ptah connects with and a connection to read it back through.
func database(c *qt.C, statements ...string) (string, *sql.DB) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := sql.Open("pgx", dbtarget.DriverDSN(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	suffix := make([]byte, 6)
	_, err = rand.Read(suffix)
	c.Assert(err, qt.IsNil)
	name := "ptah_planreport_" + hex.EncodeToString(suffix)
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		c.Check(dropErr, qt.IsNil)
	})
	url, err := atlasurl.WithDatabaseName(adminURL, name)
	c.Assert(err, qt.IsNil)
	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	for _, statement := range statements {
		_, err = db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return url, db
}

// run writes the schema, runs one verb over it and reads the document. It also
// returns the path a plan file would be saved at.
func run(c *qt.C, command verb, url, schema string) (clirun.Result, document, string) {
	c.Helper()
	dir := c.TempDir()
	path := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
	output := filepath.Join(dir, "saved.plan.json")
	args := append(command.args(output), "--db-url", url, "--schema-file", path, "--json")
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, args...)
	var got document
	c.Assert(json.Unmarshal([]byte(result.Stdout), &got), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result, got, output
}

// readBack is one answer of the database, as text.
func readBack(c *qt.C, db *sql.DB, query string) string {
	c.Helper()
	var answer string
	c.Assert(db.QueryRowContext(c.Context(), query).Scan(&answer), qt.IsNil)
	return answer
}

const (
	heldViewOptions  = "SELECT reloptions::text FROM pg_class WHERE relname = 'my_docs'"
	optionsBothHeld  = "{security_invoker=true,security_barrier=true}"
	viewsNamedAllow  = "SELECT count(*)::text FROM pg_class WHERE relname = 'allowed_view'"
	routinesNamedFn  = "SELECT count(*)::text FROM pg_proc WHERE proname = 'a_fn'"
	docsTable        = "CREATE TABLE docs (id integer PRIMARY KEY)"
	docsView         = "SELECT id FROM docs"
	docsSecurity     = "ALTER TABLE docs ENABLE ROW LEVEL SECURITY"
	docsPolicyOnView = "CREATE POLICY docs_owner ON docs USING (id IN (SELECT id FROM allowed_view))"
)

// TestPlannerRefusalReportE2E_FailurePath runs each plan the planner refuses over
// what the database holds, through both commands. A refusal is a code a caller can
// key on, at the exit code the refusal has without one. The document's contract
// version stays what a reader of the other codes already reads. Reading the server
// back shows that nothing reached it, and no plan file was saved.
func TestPlannerRefusalReportE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name            string
		built           []string
		declared        string
		wantCode        string
		wantViewOptions []viewOption
		wantMessage     string
		query           string
		wantHeld        string
	}{
		{
			name: "a view holds security options the schema leaves out",
			built: []string{
				docsTable,
				"CREATE VIEW my_docs WITH (security_invoker = true, security_barrier = true) AS " + docsView,
			},
			declared: docsTable + ";\nCREATE VIEW my_docs AS " + docsView + ";\n",
			wantCode: "view-security-option",
			wantViewOptions: []viewOption{
				{View: "my_docs", Option: "security_barrier"},
				{View: "my_docs", Option: "security_invoker"},
			},
			wantMessage: `(?s)generate schema apply operations: unsupported feature: view my_docs holds security_barrier=true and the schema does not state it\..*`,
			query:       heldViewOptions,
			wantHeld:    optionsBothHeld,
		},
		{
			name: "the plan removes a view a policy it keeps reads",
			built: []string{
				docsTable, docsSecurity,
				"CREATE TABLE allowed (id integer PRIMARY KEY)",
				"CREATE VIEW allowed_view AS SELECT id FROM allowed",
				docsPolicyOnView,
			},
			declared:    docsTable + ";\nCREATE TABLE allowed (id integer PRIMARY KEY);\n" + docsSecurity + ";\n" + docsPolicyOnView + ";\n",
			wantCode:    "dependent-object",
			wantMessage: `(?s)generate schema apply operations: unsupported feature: the plan drops view allowed_view, which the declaration removes; .*`,
			query:       viewsNamedAllow,
			wantHeld:    "1",
		},
		{
			name: "the plan drops and creates a view a policy it keeps reads",
			built: []string{
				docsTable, docsSecurity,
				"CREATE TABLE allowed (id integer PRIMARY KEY)",
				"CREATE VIEW allowed_view AS SELECT id FROM allowed",
				docsPolicyOnView,
			},
			declared: docsTable + ";\nCREATE TABLE allowed (id bigint PRIMARY KEY);\nCREATE VIEW allowed_view AS SELECT id FROM allowed;\n" +
				docsSecurity + ";\n" + docsPolicyOnView + ";\n",
			wantCode:    "dependent-object",
			wantMessage: `(?s)generate schema apply operations: unsupported feature: the plan drops view allowed_view and creates it again, .*`,
			query:       viewsNamedAllow,
			wantHeld:    "1",
		},
		{
			name: "the plan has no order that drops a routine and what calls it",
			built: []string{
				docsTable,
				"CREATE TABLE gone (id bigint PRIMARY KEY)",
				"CREATE FUNCTION a_fn() RETURNS bigint LANGUAGE SQL RETURN (SELECT count(*) FROM gone)",
				"ALTER TABLE gone ADD COLUMN n bigint DEFAULT a_fn()",
			},
			declared:    docsTable + ";\n",
			wantCode:    "dependent-object",
			wantMessage: `(?s)generate schema apply operations: unsupported feature: table gone calls routine a_fn, which depends on table gone, .*`,
			query:       routinesNamedFn,
			wantHeld:    "1",
		},
	}
	for _, test := range tests {
		for _, command := range verbs {
			t.Run(test.name+"/"+command.name, func(t *testing.T) {
				c := qt.New(t)
				url, db := database(c, test.built...)

				result, got, output := run(c, command, url, test.declared)

				c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
				c.Assert(got.ContractVersion, qt.Equals, command.contract)
				c.Assert(got.Outcome, qt.Equals, "refused")
				c.Assert(got.Refusal, qt.IsNotNil)
				c.Assert(got.Refusal.Code, qt.Equals, test.wantCode)
				c.Assert(got.Refusal.ViewOptions, qt.DeepEquals, test.wantViewOptions)
				c.Assert(got.Error, qt.IsNotNil)
				c.Assert(got.Error.Message, qt.Matches, test.wantMessage)
				c.Assert(readBack(c, db, test.query), qt.Equals, test.wantHeld)
				_, err := os.Stat(output)
				c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
			})
		}
	}
}

// TestPlannerRefusalReportE2E_HappyPath is the control: the same view, with the
// schema stating what it holds, is no refusal at all.
func TestPlannerRefusalReportE2E_HappyPath(t *testing.T) {
	const statedView = "CREATE VIEW my_docs WITH (security_invoker = true, security_barrier = true) AS " + docsView
	for _, command := range verbs {
		t.Run(command.name, func(t *testing.T) {
			c := qt.New(t)
			url, db := database(c, docsTable, statedView)

			result, got, _ := run(c, command, url, docsTable+";\n"+statedView+";\n")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(got.Outcome, qt.Equals, "no-changes")
			c.Assert(got.Refusal, qt.IsNil)
			c.Assert(readBack(c, db, heldViewOptions), qt.Equals, optionsBothHeld)
		})
	}
}
