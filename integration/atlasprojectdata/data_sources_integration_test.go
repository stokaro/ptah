//go:build integration

package atlasprojectdata_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	atlasDataHelperFlag       = "--atlas-data-source-helper"
	atlasDataHelperMarkerName = ".ptah-atlas-data-executed"
)

func TestMain(m *testing.M) {
	for i, arg := range os.Args {
		if arg == atlasDataHelperFlag {
			os.Exit(runAtlasDataHelper(os.Args[i+1:]))
		}
	}
	os.Exit(m.Run())
}

func runAtlasDataHelper(args []string) int {
	if len(args) == 0 {
		return 2
	}
	switch args[0] {
	case "echo":
		_, _ = os.Stdout.WriteString(args[1])
	case "fail":
		_, _ = os.Stderr.WriteString("measured failure on stderr\n")
		return 7
	case "pwd":
		workingDir, err := os.Getwd()
		if err != nil {
			return 2
		}
		_, _ = fmt.Fprintln(os.Stdout, workingDir)
	case "mark":
		if err := os.WriteFile(atlasDataHelperMarkerName, []byte("executed"), 0o600); err != nil {
			return 2
		}
	case "sleep":
		_, _ = os.Stderr.WriteString("still running before cancellation\n")
		time.Sleep(5 * time.Second)
	default:
		return 2
	}
	return 0
}

func TestAtlasDataExternalRunsDirectArgvAndPreservesOutput(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "should-not-exist")
	literal := "sqlite://value;touch " + marker + "\n"
	raw := fmt.Appendf(nil, `
data "external" "selected" {
  program = %s
}
env "local" {
  url = data.external.selected
}
`, hclStringList(helperProgram("echo", literal)))

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, literal)
	_, err = os.Stat(marker)
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

func TestAtlasDataExternalOutputCanBeJSONDecoded(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	raw := fmt.Appendf(nil, `
data "external" "selected" {
  program = %s
}
env "local" {
  url = jsondecode(data.external.selected).url
}
`, hclStringList(helperProgram("echo", `{"url":"sqlite://decoded.db"}`)))

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://decoded.db")
}

func TestAtlasDataExternalResolvesRelativeWorkingDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	workingDir := filepath.Join(dir, "working-dir")
	c.Assert(os.Mkdir(workingDir, 0o700), qt.IsNil)
	raw := fmt.Appendf(nil, `
data "external" "selected" {
  program     = %s
  working_dir = "working-dir"
}
env "local" {
  url = data.external.selected
}
`, hclStringList(helperProgram("pwd")))

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, workingDir+"\n")
}

func TestAtlasDataExternalReportsProgramFailure(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	executable, err := os.Executable()
	c.Assert(err, qt.IsNil)
	raw := fmt.Appendf(nil, `
data "external" "failure" {
  program = %s
}
env "local" {
  url = data.external.failure
}
`, hclStringList(helperProgram("fail")))

	_, err = projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.ErrorMatches, `data\.external\.failure: running program `+regexp.QuoteMeta(executable)+`: measured failure on stderr`)
}

func TestAtlasDataExternalHonorsCallerCancellation(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	raw := fmt.Appendf(nil, `
data "external" "selected" {
  program = %s
}
env "local" {
  url = data.external.selected
}
`, hclStringList(helperProgram("sleep")))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: ctx,
		EnvName: "local",
	})

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
}

func TestAtlasDataSQLReturnsPinnedSingleColumnShape(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "query.db")
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE TABLE tenants (name TEXT); INSERT INTO tenants VALUES ('alpha'), ('beta')`)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	dbURL := "sqlite://" + filepath.ToSlash(dbPath)
	raw := fmt.Appendf(nil, `
data "sql" "selected" {
  url   = %s
  query = "SELECT name FROM tenants WHERE name = ? ORDER BY name"
  args  = ["beta"]
}
env "local" {
  url = jsonencode(data.sql.selected)
}
`, strconv.Quote(dbURL))

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, `{"count":1,"value":"beta","values":["beta"]}`)
}

func TestAtlasDataSQLReturnsPinnedEmptyShape(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "query.db")
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE TABLE tenants (name TEXT)`)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	dbURL := "sqlite://" + filepath.ToSlash(dbPath)
	raw := fmt.Appendf(nil, `
data "sql" "selected" {
  url   = %s
  query = "SELECT name FROM tenants"
}
env "local" {
  url = jsonencode(data.sql.selected)
}
`, strconv.Quote(dbURL))

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, `{"count":0,"value":null,"values":[]}`)
}

func TestAtlasDataSQLPreservesPinnedNumericTypes(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "query.db")
	databaseURL := "sqlite://" + filepath.ToSlash(databasePath)
	raw := fmt.Appendf(nil, `
data "sql" "selected" {
  url   = %s
  query = "SELECT 42 UNION ALL SELECT 3.5"
}
env "local" {
  url = jsonencode(data.sql.selected)
}
`, strconv.Quote(databaseURL))

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, `{"count":2,"value":42,"values":[42,3.5]}`)
}

func TestAtlasDataSQLRefusesHeterogeneousRowTypes(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	databaseURL := "sqlite://" + filepath.ToSlash(filepath.Join(dir, "query.db"))
	raw := fmt.Appendf(nil, `
data "sql" "selected" {
  url   = %s
  query = "SELECT 1 UNION ALL SELECT 'x'"
}
env "local" {
  url = jsonencode(data.sql.selected)
}
`, strconv.Quote(databaseURL))

	_, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.ErrorMatches, `data\.sql\.selected: query rows have inconsistent types: number then string`)
}

func TestAtlasDataSQLUsesPinnedNumericArgumentType(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	databaseURL := "sqlite://" + filepath.ToSlash(filepath.Join(dir, "query.db"))
	raw := fmt.Appendf(nil, `
data "sql" "selected" {
  url   = %s
  query = "SELECT typeof(?)"
  args  = [42]
}
env "local" {
  url = jsonencode(data.sql.selected)
}
`, strconv.Quote(databaseURL))

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, `{"count":1,"value":"real","values":["real"]}`)
}

func TestAtlasDataSQLRefusesUnsupportedRowShapes(t *testing.T) {
	dir := t.TempDir()
	databaseURL := "sqlite://" + filepath.ToSlash(filepath.Join(dir, "query.db"))
	tests := []struct {
		name      string
		query     string
		wantError string
	}{
		{
			name:      "null value",
			query:     "SELECT NULL",
			wantError: `data\.sql\.selected: unsupported row type: <nil>`,
		},
		{
			name:      "multiple columns",
			query:     "SELECT 1, 2",
			wantError: `data\.sql\.selected: scanning row: sql: expected 2 destination arguments in Scan, not 1`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			raw := fmt.Appendf(nil, `
data "sql" "selected" {
  url   = %s
  query = %s
}
env "local" {
  url = jsonencode(data.sql.selected)
}
`, strconv.Quote(databaseURL), strconv.Quote(test.query))

			_, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
				Context: context.Background(),
				EnvName: "local",
			})

			c.Assert(err, qt.ErrorMatches, test.wantError)
		})
	}
}

func TestAtlasDataSourcesResolveDependenciesInTopologicalOrder(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	databasePath := filepath.Join(dir, "query.db")
	db, err := sql.Open("sqlite", databasePath)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE TABLE tenants (name TEXT); INSERT INTO tenants VALUES ('selected')`)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	raw := fmt.Appendf(nil, `
data "sql" "query" {
  url   = %s
  query = "SELECT name FROM tenants"
}
data "external" "capture" {
  program = [%s, %s, "echo", data.sql.query.value]
}
env "local" {
  url = data.external.capture
}
`,
		strconv.Quote("sqlite://"+filepath.ToSlash(databasePath)),
		strconv.Quote(helperProgram("echo")[0]),
		strconv.Quote(atlasDataHelperFlag),
	)

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "selected")
}

func TestAtlasDataSourceStructureRefusesBeforeDependencyExecution(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, atlasDataHelperMarkerName)
	executable, err := os.Executable()
	c.Assert(err, qt.IsNil)
	raw := fmt.Appendf(nil, `
data "external" "side_effect" {
  program     = [%s, %s, "mark"]
  working_dir = "."
}
data "external" "invalid" {
  program = [%s, %s, "echo", "unused"]
  unknown = data.external.side_effect
}
env "local" {
  url = data.external.invalid
}
`,
		strconv.Quote(executable),
		strconv.Quote(atlasDataHelperFlag),
		strconv.Quote(executable),
		strconv.Quote(atlasDataHelperFlag),
	)

	_, err = projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.ErrorMatches, `unsupported atlas\.hcl construct "unknown" at .*atlas\.hcl:8`)
	_, statErr := os.Stat(marker)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

func TestAtlasDataRuntimeVariablePreservesFileBytes(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	value := "sqlite://runtime-value\n"
	valuePath := filepath.Join(dir, "runtime-value.txt")
	c.Assert(os.WriteFile(valuePath, []byte(value), 0o600), qt.IsNil)
	raw := fmt.Appendf(nil, `
data "runtimevar" "selected" {
  url = %s
}
env "local" {
  url = data.runtimevar.selected
}
`, strconv.Quote((&url.URL{Scheme: "file", Path: valuePath}).String()))

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, value)
}

func TestAtlasDataRuntimeVariableReadsConstantBytes(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	value := "sqlite://constant-value\n"
	query := url.Values{"val": []string{value}}
	raw := fmt.Appendf(nil, `
data "runtimevar" "selected" {
  url = %s
}
env "local" {
  url = data.runtimevar.selected
}
`, strconv.Quote((&url.URL{Scheme: "constant", RawQuery: query.Encode()}).String()))

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, value)
}

// TestAtlasDataRuntimeVariableReportsAMissingFile pins the half of the message
// that is Ptah's: the source that failed, the operation, and the driver's code.
//
// What follows the code belongs to the driver. Whether it echoes the path
// before "no such file or directory" differs by platform and by driver version,
// so asserting it makes the test report on a dependency's wording rather than
// on this adapter. `(?s)` because [qt.ErrorMatches] anchors the whole message
// and `.` does not match a newline, so a driver that ever writes a second line
// would fail the pattern for a reason unrelated to the behavior.
func TestAtlasDataRuntimeVariableReportsAMissingFile(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	missingURL := (&url.URL{Scheme: "file", Path: filepath.Join(dir, "missing")}).String() + "?timeout=50ms"
	raw := fmt.Appendf(nil, `
data "runtimevar" "missing" {
  url = %s
}
env "local" {
  url = data.runtimevar.missing
}
`, strconv.Quote(missingURL))

	_, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.ErrorMatches, `(?s)data\.runtimevar\.missing: getting latest snapshot: runtimevar \(code=NotFound\): .*`)
}

// runtimeVariableSlowResponse is how long the timeout server withholds its
// answer. It has to dwarf the deadline under test so that the two outcomes are
// far apart in time and cannot be confused for one another.
const runtimeVariableSlowResponse = 2 * time.Second

// TestAtlasDataRuntimeVariableHonorsURLTimeout measures the deadline the URL
// carries, which requires a target that is slow rather than one that is absent.
//
// A missing file cannot serve as that target: it fails with NotFound long
// before any deadline elapses, so a test built on one is green whatever the
// timeout does — including when it is never read.
//
// Two things are asserted and one deliberately is not. Resolution fails, and it
// fails naming the source and the operation, which is Ptah's half of the
// message; and it returns far sooner than the server answers, which is the only
// observation that distinguishes a deadline that was honored from one that was
// dropped — without it the run would succeed with the late value. The driver's
// half of the message is not asserted: Go CDK reports a cut wait as `no value
// yet (code=FailedPrecondition)` rather than as the deadline, and pinning that
// wording would make this test report on a dependency instead of on this
// adapter.
func TestAtlasDataRuntimeVariableHonorsURLTimeout(t *testing.T) {
	c := qt.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		time.Sleep(runtimeVariableSlowResponse)
		_, _ = writer.Write([]byte("sqlite://answered-too-late\n"))
	}))
	c.Cleanup(server.Close)
	dir := t.TempDir()
	raw := fmt.Appendf(nil, `
data "runtimevar" "slow" {
  url = %s
}
env "local" {
  url = data.runtimevar.slow
}
`, strconv.Quote(server.URL+"?timeout=50ms"))

	start := time.Now()
	_, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})
	elapsed := time.Since(start)

	c.Assert(err, qt.ErrorMatches, `(?s)data\.runtimevar\.slow: getting latest snapshot: .*`)
	c.Assert(elapsed < runtimeVariableSlowResponse/2, qt.IsTrue)
}

func TestAtlasDataRuntimeVariableReadsHTTPBytes(t *testing.T) {
	c := qt.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("sqlite://http-value\n"))
	}))
	c.Cleanup(server.Close)
	dir := t.TempDir()
	raw := fmt.Appendf(nil, `
data "runtimevar" "selected" {
  url = %s
}
env "local" {
  url = data.runtimevar.selected
}
`, strconv.Quote(server.URL+"?timeout=2s"))

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://http-value\n")
}

func TestAtlasDataTemplateDirectoryRendersImmutableMigrationFS(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	templateDir := filepath.Join(dir, "templates")
	c.Assert(os.Mkdir(templateDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(templateDir, "1_init.sql"),
		[]byte("CREATE TABLE {{ .table }} (id INTEGER PRIMARY KEY);\n{{ template \"shared/users\" .table }}"),
		0o600,
	), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(templateDir, "nested"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(templateDir, "nested", "ignored.sql"),
		[]byte("{{ define \"shared/users\" }}CREATE TABLE users_{{ . }} (id INTEGER);\n{{ end }}"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(templateDir, "2_ignored.SQL"), []byte("SELECT 2;"), 0o600), qt.IsNil)
	raw := []byte(`
data "template_dir" "selected" {
  path = "templates"
  vars = {
    table = "rendered_widgets"
  }
}
env "local" {
  migration {
    dir = data.template_dir.selected.url
  }
}
`)

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.IsNil)
	expectedURL := (&url.URL{Scheme: "mem", Path: filepath.Join("templates", "selected")}).String()
	c.Assert(cfg.Migration.Dir, qt.Equals, expectedURL)
	migrationFS, ok := cfg.MigrationDirectoryFS(expectedURL)
	c.Assert(ok, qt.IsTrue)
	source, ok := cfg.MigrationDirectorySource(expectedURL)
	c.Assert(ok, qt.IsTrue)
	c.Assert(source.Path, qt.Equals, "templates")
	c.Assert(os.WriteFile(
		filepath.Join(templateDir, "1_init.sql"),
		[]byte("CREATE TABLE mutated_after_parse (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	rendered, err := fs.ReadFile(migrationFS, "1_init.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered), qt.Equals, "CREATE TABLE rendered_widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE users_rendered_widgets (id INTEGER);\n")
	_, err = fs.Stat(migrationFS, "nested/ignored.sql")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	_, err = fs.Stat(migrationFS, "2_ignored.SQL")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	sumBytes, err := fs.ReadFile(migrationFS, migratesum.AtlasFileName)
	c.Assert(err, qt.IsNil)
	sum, err := migratesum.Parse(sumBytes)
	c.Assert(err, qt.IsNil)
	c.Assert(sum.Entries, qt.HasLen, 1)
	c.Assert(sum.Entries[0].Name, qt.Equals, "1_init.sql")
}

func TestAtlasDataTemplateDirectoryMatchesPinnedVariableTypes(t *testing.T) {
	tests := []struct {
		name      string
		template  string
		variables string
		want      string
	}{
		{
			name:      "number is float64",
			template:  `{{ printf "%T:%v" .value .value }}`,
			variables: `{ value = 42 }`,
			want:      "float64:42",
		},
		{
			name:      "string list keeps its element type",
			template:  `{{ printf "%T:%v" .value .value }}`,
			variables: `{ value = tolist(["a", "b"]) }`,
			want:      "[]string:[a b]",
		},
		{
			name:      "number list keeps its element type",
			template:  `{{ printf "%T:%v" .value .value }}`,
			variables: `{ value = tolist([1, 2]) }`,
			want:      "[]float64:[1 2]",
		},
		{
			name:      "boolean list keeps its element type",
			template:  `{{ printf "%T:%v" .value .value }}`,
			variables: `{ value = tolist([true, false]) }`,
			want:      "[]bool:[true false]",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cfg, err := parseTemplateDirectoryVariables(c, test.template, test.variables)
			c.Assert(err, qt.IsNil)
			migrationFS, ok := cfg.MigrationDirectoryFS(cfg.Migration.Dir)
			c.Assert(ok, qt.IsTrue)
			rendered, err := fs.ReadFile(migrationFS, "1.sql")
			c.Assert(err, qt.IsNil)
			c.Assert(string(rendered), qt.Equals, test.want)
		})
	}
}

func TestAtlasDataTemplateDirectoryRefusesUnsupportedVariableTypes(t *testing.T) {
	tests := []struct {
		name      string
		variables string
		wantError string
	}{
		{
			name:      "nested object",
			variables: `{ value = { nested = "x" } }`,
			wantError: `atlas\.hcl "vars" at .*:4: attribute "vars" must be a map of strings, numbers or booleans, got: object`,
		},
		{
			name:      "tuple",
			variables: `{ value = ["a", "b"] }`,
			wantError: `atlas\.hcl "vars" at .*:4: attribute "vars" must be a map of strings, numbers or booleans, got: tuple`,
		},
		{
			name:      "null",
			variables: `{ value = null }`,
			wantError: `atlas\.hcl "vars" at .*:4: attribute "vars" must be a map of strings, numbers or booleans, got: dynamic`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := parseTemplateDirectoryVariables(c, `SELECT 1;`, test.variables)
			c.Assert(err, qt.ErrorMatches, test.wantError)
		})
	}
}

func parseTemplateDirectoryVariables(c *qt.C, source, variables string) (projectconfig.Config, error) {
	c.Helper()
	dir := c.TempDir()
	templateDir := filepath.Join(dir, "templates")
	c.Assert(os.Mkdir(templateDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(templateDir, "1.sql"), []byte(source), 0o600), qt.IsNil)
	raw := fmt.Appendf(nil, `
data "template_dir" "selected" {
  path = "templates"
  vars = %s
}
env "local" {
  migration {
    dir = data.template_dir.selected.url
  }
}
`, variables)
	return projectconfig.ParseAtlasWithOptions(
		raw,
		filepath.Join(dir, "atlas.hcl"),
		projectconfig.AtlasLoadOptions{Context: context.Background(), EnvName: "local"},
	)
}

func TestAtlasDataTemplateDirectoryReportsTemplateFailures(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	templateDir := filepath.Join(dir, "templates")
	c.Assert(os.Mkdir(templateDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(templateDir, "1_init.sql"),
		[]byte("CREATE TABLE {{ .missing }} (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	raw := []byte(`
data "template_dir" "selected" {
  path = "templates"
  vars = {}
}
env "local" {
  migration {
    dir = data.template_dir.selected.url
  }
}
`)

	_, err := projectconfig.ParseAtlasWithOptions(raw, filepath.Join(dir, "atlas.hcl"), projectconfig.AtlasLoadOptions{
		Context: context.Background(),
		EnvName: "local",
	})

	c.Assert(err, qt.ErrorMatches, `data\.template_dir\.selected: rendering template directory: execute 1_init\.sql: .*map has no entry for key "missing"`)
}

func TestAtlasDataTemplateDirectoryAppliesThroughCompatCommand(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)
	cmd := atlas.NewCompatCommand("atlas")
	stderr := executeTemplateDirectoryApply(c, cmd, []string{
		"migrate", "apply",
		"--config", "file://" + filepath.ToSlash(project.configPath),
		"--env", "local",
	}, project.databasePath)
	c.Assert(stderr, qt.Equals, "")
}

func TestAtlasDataTemplateDirectoryAppliesThroughNativeCommand(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)
	t.Chdir(project.root)
	stderr := executeTemplateDirectoryApply(c, root.NewRootCommand(), []string{
		"migrations", "up",
		"--env", "local",
	}, project.databasePath)
	c.Assert(stderr, qt.Contains, "All migrations applied successfully")
}

func TestAtlasDataTemplateDirectoryRoutesCompatStatusThroughRenderedSnapshot(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)
	applyTemplateDirectoryProject(c, project)

	stdout, stderr, err := executeCompatProjectCommand(
		"migrate", "status",
		"--config", "file://"+filepath.ToSlash(project.configPath),
		"--env", "local",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stderr, qt.Equals, "")
}

func TestAtlasDataTemplateDirectoryRoutesCompatSetThroughRenderedSnapshot(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)
	applyTemplateDirectoryProject(c, project)

	stdout, stderr, err := executeCompatProjectCommand(
		"migrate", "set", "1",
		"--config", "file://"+filepath.ToSlash(project.configPath),
		"--env", "local",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stderr, qt.Equals, "")
}

func TestAtlasDataTemplateDirectoryRoutesCompatLintThroughRenderedSnapshot(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)

	stdout, stderr, err := executeCompatProjectCommand(
		"migrate", "lint",
		"--config", "file://"+filepath.ToSlash(project.configPath),
		"--env", "local",
		"--dev-url", "sqlite://"+filepath.ToSlash(filepath.Join(project.root, "dev.db")),
		"--latest", "1",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stderr, qt.Equals, "")
}

func applyTemplateDirectoryProject(c *qt.C, project templateDirectoryProject) {
	c.Helper()
	executeTemplateDirectoryApply(c, atlas.NewCompatCommand("atlas"), []string{
		"migrate", "apply",
		"--config", "file://" + filepath.ToSlash(project.configPath),
		"--env", "local",
	}, project.databasePath)
}

func TestAtlasDataTemplateDirectoryFormattedDownReadsRenderedSnapshot(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)
	executeTemplateDirectoryApply(c, atlas.NewCompatCommand("atlas"), []string{
		"migrate", "apply",
		"--config", "file://" + filepath.ToSlash(project.configPath),
		"--env", "local",
	}, project.databasePath)

	stdout, stderr, err := executeCompatProjectCommand(
		"migrate", "down",
		"--config", "file://"+filepath.ToSlash(project.configPath),
		"--env", "local",
		"--dev-url", "sqlite://"+filepath.ToSlash(filepath.Join(project.root, "dev.db")),
		"--to-version", "0",
		"--format", "{{ json . }}",
	)

	c.Assert(err, qt.ErrorMatches, `rollback verification failed: .*migration 1 has no Atlas down migration.*`)
	c.Assert(err, qt.Not(qt.ErrorMatches), `.*\.ptah-template-dir-.*`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
}

func TestAtlasDataTemplateDirectoryMigrateNewWritesBackToSource(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)

	stdout, stderr, err := executeCompatProjectCommand(
		"migrate", "new", "added",
		"--config", "file://"+filepath.ToSlash(project.configPath),
		"--env", "local",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
	assertTemplateDirectoryMigrationCreated(c, project.templateDir, "added", 2)
	hidden, err := filepath.Glob(filepath.Join(project.root, ".ptah-template-dir-*"))
	c.Assert(err, qt.IsNil)
	c.Assert(hidden, qt.HasLen, 0)
}

func TestAtlasDataTemplateDirectoryMigrateDiffWritesBackToSource(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)
	desiredPath := filepath.Join(project.root, "desired.sql")
	c.Assert(os.WriteFile(
		desiredPath,
		[]byte("CREATE TABLE rendered_widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(project.configPath, fmt.Appendf(nil, `
data "template_dir" "selected" {
  path = "templates"
  vars = {
    table = "rendered_widgets"
  }
}
env "local" {
  dev = %s
  schema {
    src = "file://desired.sql"
  }
  migration {
    dir = data.template_dir.selected.url
  }
}
`, strconv.Quote("sqlite://"+filepath.ToSlash(project.databasePath))), 0o600), qt.IsNil)
	rawTemplate, err := os.ReadFile(filepath.Join(project.templateDir, "1_init.sql"))
	c.Assert(err, qt.IsNil)

	stdout, stderr, err := executeCompatProjectCommand(
		"migrate", "diff", "added",
		"--config", "file://"+filepath.ToSlash(project.configPath),
		"--env", "local",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
	generated := generatedMigrationName(c, project.templateDir, "added")
	generatedSQL, err := os.ReadFile(filepath.Join(project.templateDir, generated))
	c.Assert(err, qt.IsNil)
	c.Assert(string(generatedSQL), qt.Contains, `CREATE TABLE "added"`)
	currentTemplate, err := os.ReadFile(filepath.Join(project.templateDir, "1_init.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(currentTemplate, qt.DeepEquals, rawTemplate)
	assertMigrationDirectorySum(c, project.templateDir, 2)
	hidden, err := filepath.Glob(filepath.Join(project.root, ".ptah-template-dir-*"))
	c.Assert(err, qt.IsNil)
	c.Assert(hidden, qt.HasLen, 0)
}

func TestAtlasDataTemplateDirectoryExplicitDirOverridesProject(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)
	explicitDir := filepath.Join(project.root, "explicit")

	stdout, stderr, err := executeCompatProjectCommand(
		"migrate", "new", "explicit",
		"--config", "file://"+filepath.ToSlash(project.configPath),
		"--env", "local",
		"--dir", "file://"+filepath.ToSlash(explicitDir),
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	assertTemplateDirectoryMigrationCreated(c, explicitDir, "explicit", 1)
	_, err = os.Stat(filepath.Join(project.templateDir, migratesum.AtlasFileName))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	entries, err := os.ReadDir(project.templateDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
}

func TestAtlasDataTemplateDirectoryHashDoesNotWriteRenderedSumToSource(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)

	stdout, stderr, err := executeCompatProjectCommand(
		"migrate", "hash",
		"--config", "file://"+filepath.ToSlash(project.configPath),
		"--env", "local",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
	_, err = os.Stat(filepath.Join(project.templateDir, migratesum.AtlasFileName))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

func TestAtlasDataTemplateDirectoryValidateReadsRenderedSnapshot(t *testing.T) {
	c := qt.New(t)
	project := newTemplateDirectoryProject(c)

	stdout, stderr, err := executeCompatProjectCommand(
		"migrate", "validate",
		"--config", "file://"+filepath.ToSlash(project.configPath),
		"--env", "local",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
}

type templateDirectoryProject struct {
	root         string
	configPath   string
	databasePath string
	templateDir  string
}

func newTemplateDirectoryProject(c *qt.C) templateDirectoryProject {
	c.Helper()
	dir := c.TempDir()
	templateDir := filepath.Join(dir, "templates")
	c.Assert(os.Mkdir(templateDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(templateDir, "1_init.sql"),
		[]byte("CREATE TABLE {{ .table }} (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	databasePath := filepath.Join(dir, "target.db")
	configPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(configPath, fmt.Appendf(nil, `
data "template_dir" "selected" {
  path = "templates"
  vars = {
    table = "rendered_widgets"
  }
}
env "local" {
  url = %s
  migration {
    dir = data.template_dir.selected.url
  }
}
`, strconv.Quote("sqlite://"+filepath.ToSlash(databasePath))), 0o600), qt.IsNil)
	return templateDirectoryProject{
		root:         dir,
		configPath:   configPath,
		databasePath: databasePath,
		templateDir:  templateDir,
	}
}

func executeCompatProjectCommand(args ...string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommand("atlas")
	var stdoutBuffer, stderrBuffer bytes.Buffer
	cmd.SetOut(&stdoutBuffer)
	cmd.SetErr(&stderrBuffer)
	cmd.SetArgs(args)
	err = cmd.ExecuteContext(context.Background())
	return stdoutBuffer.String(), stderrBuffer.String(), err
}

func assertTemplateDirectoryMigrationCreated(c *qt.C, dir, name string, expectedSumEntries int) {
	c.Helper()
	generated := generatedMigrationName(c, dir, name)
	contents, err := os.ReadFile(filepath.Join(dir, generated))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "")
	assertMigrationDirectorySum(c, dir, expectedSumEntries)
}

func generatedMigrationName(c *qt.C, dir, name string) string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	pattern := `^\d{14}_` + regexp.QuoteMeta(name) + `\.sql$`
	var generated []string
	for _, entry := range entries {
		if matched, matchErr := regexp.MatchString(pattern, entry.Name()); matched {
			generated = append(generated, entry.Name())
		} else {
			c.Assert(matchErr, qt.IsNil)
		}
	}
	c.Assert(generated, qt.HasLen, 1)
	return generated[0]
}

func assertMigrationDirectorySum(c *qt.C, dir string, expectedEntries int) {
	c.Helper()
	sumBytes, err := os.ReadFile(filepath.Join(dir, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)
	sum, err := migratesum.Parse(sumBytes)
	c.Assert(err, qt.IsNil)
	c.Assert(sum.Entries, qt.HasLen, expectedEntries)
	verification, err := migratesum.VerifyWithFormat(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(verification.OK(), qt.IsTrue)
}

func executeTemplateDirectoryApply(
	c *qt.C,
	cmd *cobra.Command,
	args []string,
	databasePath string,
) string {
	c.Helper()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)

	err := cmd.ExecuteContext(context.Background())
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String()))
	db, err := sql.Open("sqlite", databasePath)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var tableName string
	err = db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'rendered_widgets'`).Scan(&tableName)
	c.Assert(err, qt.IsNil)
	c.Assert(tableName, qt.Equals, "rendered_widgets")
	return stderr.String()
}

func helperProgram(mode string, args ...string) []string {
	executable, err := os.Executable()
	if err != nil {
		panic(err)
	}
	return append([]string{executable, atlasDataHelperFlag, mode}, args...)
}

func hclStringList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, strconv.Quote(value))
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}
