//go:build integration

package rowfiles_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/schemaartifact"
)

// These tests hold one boundary on every verb that reads a declared row file. A
// `data` block or a //ptah:schema:data annotation names a path, a schema is not
// always one the reader wrote, and it can come from a pulled artifact. So a file
// outside the project must not be read, and nothing it holds may reach a report, a
// plan, a registry or the database.
//
// They run the shipped binaries because the exit status and the two streams are
// the claim, and because the compatibility tree reads rows through its own
// wiring. SQLite is enough: the refusal is decided before any statement runs.

const (
	// secretValue is what the file outside the project holds.
	secretValue = "TOP-SECRET-VALUE"
	// declaredValue is what the file inside the project holds.
	declaredValue = "DECLARED-VALUE"
)

// project is one working directory, the database a verb reads, and the registry
// `schema push` talks to, with the file a schema must not reach beside it.
type project struct {
	dir      string
	database string
	secret   string
	registry *countingRegistry
}

// countingRegistry answers every request with a failure and counts them, which
// tells a push that was refused from one that started uploading.
type countingRegistry struct {
	address  string
	requests atomic.Int64
}

// newProject writes the schema twice, as HCL and as Go annotations, with file as
// the path it names.
func newProject(c *qt.C, file string) *project {
	c.Helper()
	parent := c.TempDir()
	dir := filepath.Join(parent, "work", "project")
	c.Assert(os.MkdirAll(filepath.Join(dir, "pulled"), 0o750), qt.IsNil)
	c.Assert(os.MkdirAll(filepath.Join(dir, "goent"), 0o750), qt.IsNil)
	c.Assert(os.MkdirAll(filepath.Join(dir, "migrations"), 0o750), qt.IsNil)
	c.Assert(os.MkdirAll(filepath.Join(parent, "work", "secret"), 0o750), qt.IsNil)

	hcl := fmt.Sprintf(`schema "main" {}
table "regions" {
  schema = schema.main
  column "code" {
    null = false
    type = text
  }
  column "name" {
    null = true
    type = text
  }
  primary_key {
    columns = [column.code]
  }
}
data {
  table = table.regions
  keys  = ["code"]
  file  = %s
}
`, strconv.Quote(file))
	writeFile(c, filepath.Join(dir, "pulled", "schema.hcl"), hcl)
	writeFile(c, filepath.Join(dir, "goent", "entities.go"), fmt.Sprintf(`package entities

//ptah:schema:data table="regions" key="code" file=%q
//ptah:schema:table name="regions"
type Region struct {
	//ptah:schema:field name="code" type="TEXT" primary="true"
	Code string
	//ptah:schema:field name="name" type="TEXT"
	Name string
}
`, file))

	secret := filepath.Join(parent, "work", "secret", "rows.yaml")
	writeFile(c, secret, "- code: XX\n  name: "+secretValue+"\n")
	writeFile(c, filepath.Join(dir, "pulled", "rows.yaml"), "- code: AA\n  name: "+declaredValue+"\n")
	writeFile(c, filepath.Join(dir, "goent", "rows.yaml"), "- code: AA\n  name: "+declaredValue+"\n")

	address := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "app.db"))
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), `CREATE TABLE regions (code TEXT NOT NULL PRIMARY KEY, name TEXT)`)
	c.Assert(err, qt.IsNil)

	registry := &countingRegistry{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		registry.requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	c.Cleanup(server.Close)
	registry.address = strings.TrimPrefix(server.URL, "http://")

	return &project{
		dir:      dir,
		database: address,
		secret:   secret,
		registry: registry,
	}
}

func writeFile(c *qt.C, path, contents string) {
	c.Helper()
	c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
}

// run invokes one verb in the project, with {db}, {empty} and {registry} filled in.
//
// {empty} is a database without the table. An Atlas plan file has no field for
// the rows a plan compared, so `ptah-compat schema plan` prints its statements
// only for a table the plan creates, and that print is where a row value shows.
func (p *project) run(c *qt.C, target clirun.Target, args ...string) clirun.Result {
	c.Helper()
	fill := strings.NewReplacer(
		"{db}", p.database,
		"{empty}", atlasurl.SQLiteURLFromPath(filepath.Join(p.dir, "empty.db")),
		"{registry}", p.registry.address,
	)
	filled := make([]string, 0, len(args))
	for _, arg := range args {
		filled = append(filled, fill.Replace(arg))
	}
	return clirun.Run(c, target, clirun.Options{Dir: p.dir}, filled...)
}

// rowCount reads back what the table holds, which is what an apply would change.
func (p *project) rowCount(c *qt.C) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), p.database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT COUNT(*) FROM regions`).Scan(&count), qt.IsNil)
	return count
}

// readingVerbs are the commands that read a row file. Each row names the exit code
// of a refusal, since the two binaries differ, and what the same verb shows for a
// file the project holds, so a row that reads nothing cannot pass as a refusal.
var readingVerbs = []struct {
	name        string
	target      clirun.Target
	args        []string
	refusedExit int
	readExit    int
	readOutput  string
}{
	{
		name: "schema plan", target: clirun.Ptah,
		args:        []string{"schema", "plan", "--schema-file", "pulled/schema.hcl", "--db-url", "{db}", "--dry-run"},
		refusedExit: 2, readOutput: declaredValue,
	},
	{
		name: "schema plan from Go", target: clirun.Ptah,
		args:        []string{"schema", "plan", "--root-dir", "goent", "--db-url", "{db}", "--dry-run"},
		refusedExit: 2, readOutput: declaredValue,
	},
	{
		name: "schema apply", target: clirun.Ptah,
		args:        []string{"schema", "apply", "--schema-file", "pulled/schema.hcl", "--db-url", "{db}", "--dry-run"},
		refusedExit: 2, readOutput: declaredValue,
	},
	{
		name: "schema drift", target: clirun.Ptah,
		args:        []string{"schema", "drift", "--schema-file", "pulled/schema.hcl", "--db-url", "{db}"},
		refusedExit: 2, readExit: 1, readOutput: "1 insert(s)",
	},
	{
		name: "schema drift from Go", target: clirun.Ptah,
		args:        []string{"schema", "drift", "--root-dir", "goent", "--db-url", "{db}"},
		refusedExit: 2, readExit: 1, readOutput: "1 insert(s)",
	},
	{
		name: "schema push", target: clirun.Ptah,
		args:        []string{"schema", "push", "oci://{registry}/rows/schema", "--schema-file", "pulled/schema.hcl", "--plain-http", "--version", "v1"},
		refusedExit: 2, readExit: 2, readOutput: "managed-data.json",
	},
	{
		name: "schema push from Go", target: clirun.Ptah,
		args:        []string{"schema", "push", "oci://{registry}/rows/schema", "--root-dir", "goent", "--plain-http", "--version", "v1"},
		refusedExit: 2, readExit: 2, readOutput: "managed-data.json",
	},
	{
		name: "migrations data", target: clirun.Ptah,
		args:        []string{"migrations", "data", "--root-dir", "goent", "--db-url", "{db}", "--migrations-dir", "migrations", "--dry-run"},
		refusedExit: 2, readOutput: declaredValue,
	},
	{
		name: "compat schema apply", target: clirun.Compat,
		args:        []string{"schema", "apply", "--to", "file://pulled/schema.hcl", "--url", "{db}", "--dry-run"},
		refusedExit: 1, readOutput: declaredValue,
	},
	{
		name: "compat schema plan", target: clirun.Compat,
		args:        []string{"schema", "plan", "--to", "file://pulled/schema.hcl", "--from", "{empty}", "--dry-run"},
		refusedExit: 1, readOutput: declaredValue,
	},
}

// TestReadingVerbs_RefuseARowFileOutsideTheProject is the reproduction, on every
// verb that reads one. The path leaves the project by climbing out of it, and the
// file is really there.
func TestReadingVerbs_RefuseARowFileOutsideTheProject(t *testing.T) {
	for _, verb := range readingVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			p := newProject(c, "../../secret/rows.yaml")

			run := p.run(c, verb.target, verb.args...)

			c.Assert(run.ExitCode, qt.Equals, verb.refusedExit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, "outside allowed root")
			c.Assert(run.Stdout+run.Stderr, qt.Not(qt.Contains), secretValue)
			c.Assert(p.registry.requests.Load(), qt.Equals, int64(0))
			c.Assert(p.rowCount(c), qt.Equals, 0)
		})
	}
}

// TestReadingVerbs_ReadARowFileTheProjectHolds is the control for the refusal
// above, with the same verbs and the same project: a file beside the schema still
// reads, and what it holds reaches the place each verb reports it.
func TestReadingVerbs_ReadARowFileTheProjectHolds(t *testing.T) {
	for _, verb := range readingVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			p := newProject(c, "rows.yaml")

			run := p.run(c, verb.target, verb.args...)

			c.Assert(run.ExitCode, qt.Equals, verb.readExit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout+run.Stderr, qt.Contains, verb.readOutput)
			c.Assert(run.Stdout+run.Stderr, qt.Not(qt.Contains), "outside allowed root")
		})
	}
}

// TestRowFileSpellings_AnswerAlikeOnEveryReader holds the spellings that leave the
// project against one verb per way into the loaders. A Windows spelling refuses on
// every platform, so the same schema gets the same answer on every machine.
func TestRowFileSpellings_AnswerAlikeOnEveryReader(t *testing.T) {
	spellings := []struct {
		name string
		file string
	}{
		{name: "leading slash", file: "/etc/hosts"},
		{name: "leading backslash", file: `\etc\hosts`},
		{name: "drive letter", file: `C:\secret\rows.yaml`},
		{name: "drive letter with slash", file: "C:/secret/rows.yaml"},
		{name: "climbs out in Windows spelling", file: `..\..\secret\rows.yaml`},
	}
	readers := []struct {
		name   string
		target clirun.Target
		args   []string
	}{
		{name: "drift", target: clirun.Ptah, args: []string{"schema", "drift", "--schema-file", "pulled/schema.hcl", "--db-url", "{db}"}},
		{name: "plan", target: clirun.Ptah, args: []string{"schema", "plan", "--schema-file", "pulled/schema.hcl", "--db-url", "{db}", "--dry-run"}},
		{name: "push", target: clirun.Ptah, args: []string{"schema", "push", "oci://{registry}/rows/schema", "--schema-file", "pulled/schema.hcl", "--plain-http", "--version", "v1"}},
		{name: "compat plan", target: clirun.Compat, args: []string{"schema", "plan", "--to", "file://pulled/schema.hcl", "--from", "{empty}", "--dry-run"}},
	}
	for _, spelling := range spellings {
		for _, reader := range readers {
			t.Run(spelling.name+"/"+reader.name, func(t *testing.T) {
				c := qt.New(t)
				p := newProject(c, spelling.file)

				run := p.run(c, reader.target, reader.args...)

				c.Assert(run.ExitCode, qt.Not(qt.Equals), 0)
				c.Assert(run.Stderr, qt.Contains, "outside allowed root")
				c.Assert(run.Stdout+run.Stderr, qt.Not(qt.Contains), secretValue)
				c.Assert(p.registry.requests.Load(), qt.Equals, int64(0))
			})
		}
	}
}

// TestRowFileUnderASecondRoot_Reads holds the composite schema: every --root-dir
// is merged into it, so each of them is the project, and a file under the second
// is no more outside than one under the first.
func TestRowFileUnderASecondRoot_Reads(t *testing.T) {
	verbs := []struct {
		name   string
		args   []string
		exit   int
		output string
	}{
		{
			name:   "plan",
			args:   []string{"schema", "plan", "--root-dir", "pulled", "--root-dir", "goent", "--db-url", "{db}", "--dry-run"},
			output: declaredValue,
		},
		{
			name:   "drift",
			args:   []string{"schema", "drift", "--root-dir", "pulled", "--root-dir", "goent", "--db-url", "{db}"},
			exit:   1,
			output: "1 insert(s)",
		},
		{
			name:   "push",
			args:   []string{"schema", "push", "oci://{registry}/rows/schema", "--root-dir", "pulled", "--root-dir", "goent", "--plain-http", "--version", "v1"},
			exit:   2,
			output: "managed-data.json",
		},
	}
	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			p := newProject(c, "rows.yaml")

			run := p.run(c, clirun.Ptah, verb.args...)

			c.Assert(run.ExitCode, qt.Equals, verb.exit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout+run.Stderr, qt.Contains, verb.output)
		})
	}
}

// TestCompatConfigFromAnotherDirectory holds the other half of
// the compatibility boundary: atlas.hcl is the project its schema sources are
// relative to, so a --config from a subdirectory keeps the rows beside the schema
// it selected, and a path leaving that project is still refused.
func TestCompatConfigFromAnotherDirectory(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		exit     int
		contains string
	}{
		{name: "rows beside the schema", file: "rows.yaml", contains: declaredValue},
		{name: "rows outside the project", file: "../../../secret/rows.yaml", exit: 1, contains: "outside allowed root"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			p := newProject(c, test.file)
			writeFile(c, filepath.Join(p.dir, "pulled", "atlas.hcl"), fmt.Sprintf(`env "local" {
  src = "file://schema.hcl"
  url = %q
}
`, p.database))
			nested := filepath.Join(p.dir, "nested")
			c.Assert(os.Mkdir(nested, 0o750), qt.IsNil)

			run := clirun.Run(c, clirun.Compat, clirun.Options{Dir: nested},
				"schema", "apply", "--config", "file://../pulled/atlas.hcl", "--env", "local", "--dry-run",
			)

			c.Assert(run.ExitCode, qt.Equals, test.exit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout+run.Stderr, qt.Contains, test.contains)
			c.Assert(run.Stdout+run.Stderr, qt.Not(qt.Contains), secretValue)
		})
	}
}

// TestPulledSchemaWithItsRowLayer_NeverReadsTheFile holds what `schema pull` leaves
// behind. The `file` of a published schema names a path in the publisher's working
// copy, which is usually outside this project, and the rows travel in a layer
// beside it. So the verbs must not refuse that path, because they must not read it.
func TestPulledSchemaWithItsRowLayer_NeverReadsTheFile(t *testing.T) {
	verbs := []struct {
		name   string
		args   []string
		exit   int
		output string
	}{
		{
			name:   "plan",
			args:   []string{"schema", "plan", "--schema-file", "pulled/schema.hcl", "--db-url", "{db}", "--dry-run"},
			output: declaredValue,
		},
		{
			name:   "drift",
			args:   []string{"schema", "drift", "--schema-file", "pulled/schema.hcl", "--db-url", "{db}"},
			exit:   1,
			output: "1 insert(s)",
		},
	}
	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			p := newProject(c, "../../secret/rows.yaml")
			writeFile(c, filepath.Join(p.dir, "pulled", schemaartifact.ManagedDataFileName), fmt.Sprintf(
				`{"sets":[{"schema":"main","table":"regions","keys":["code"],"columns":["code","name"],`+
					`"rows":[{"code":{"tag":"str","text":"AA"},"name":{"tag":"str","text":%q}}]}]}`+"\n",
				declaredValue,
			))

			run := p.run(c, clirun.Ptah, verb.args...)

			c.Assert(run.ExitCode, qt.Equals, verb.exit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout+run.Stderr, qt.Contains, verb.output)
			c.Assert(run.Stdout+run.Stderr, qt.Not(qt.Contains), secretValue)
		})
	}
}
