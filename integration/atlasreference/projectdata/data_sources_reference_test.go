//go:build integration

package projectdata_test

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"go.5x5.cz/ptah/integration/atlasreference"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	referenceEnv     = atlasreference.EnvVar
	referenceVersion = atlasreference.Version
	oracleHelperFlag = "--project-data-reference-helper"
	oracleCapture    = ".ptah-project-data-reference-capture"
)

type commandResult struct {
	code   int
	stdout string
	stderr string
}

type sourceFixture struct {
	configPath string
	capture    string
	want       string
}

func TestMain(m *testing.M) {
	for i, argument := range os.Args {
		if argument == oracleHelperFlag {
			os.Exit(runOracleHelper(os.Args[i+1:]))
		}
	}
	os.Exit(m.Run())
}

func runOracleHelper(arguments []string) int {
	if len(arguments) == 0 {
		return 2
	}
	switch arguments[0] {
	case "capture":
		if len(arguments) != 2 {
			return 2
		}
		// #nosec G304 -- oracleCapture is fixed under the caller-controlled temporary working directory.
		if err := os.WriteFile(oracleCapture, []byte(arguments[1]), 0o600); err != nil {
			return 2
		}
		_, _ = os.Stdout.WriteString("local\n")
	case "fail":
		_, _ = os.Stderr.WriteString("measured failure on stderr\n")
		return 7
	case "output":
		if len(arguments) != 2 {
			return 2
		}
		_, _ = os.Stdout.WriteString(arguments[1])
	default:
		return 2
	}
	return 0
}

func TestProjectDataSourcesMatchPinnedAtlasOutputAndExit(t *testing.T) {
	reference := requireAtlasOracle(t)
	buildCheck := qt.New(t)
	compat := buildCompatBinary(buildCheck)

	t.Run("sql", func(t *testing.T) {
		c := qt.New(t)
		fixture := sqlFixture(c, t.TempDir())
		atlas := runInspect(c, reference, fixture.configPath)
		atlasCapture := readCapture(c, fixture)
		removeCapture(c, fixture)
		ptah := runInspect(c, compat, fixture.configPath)
		ptahCapture := readCapture(c, fixture)

		c.Assert(atlas.code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", atlas.stdout, atlas.stderr))
		c.Assert(ptah.code, qt.Equals, atlas.code)
		c.Assert(ptah.stdout, qt.Equals, atlas.stdout)
		c.Assert(ptah.stderr, qt.Equals, atlas.stderr)
		c.Assert(atlasCapture, qt.Equals, fixture.want)
		c.Assert(ptahCapture, qt.Equals, fixture.want)
	})

	t.Run("sql numeric argument", func(t *testing.T) {
		c := qt.New(t)
		fixture := sqlNumericArgumentFixture(c, t.TempDir())
		atlas := runInspect(c, reference, fixture.configPath)
		atlasCapture := readCapture(c, fixture)
		removeCapture(c, fixture)
		ptah := runInspect(c, compat, fixture.configPath)
		ptahCapture := readCapture(c, fixture)

		c.Assert(atlas.code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", atlas.stdout, atlas.stderr))
		c.Assert(ptah.code, qt.Equals, atlas.code)
		c.Assert(ptah.stdout, qt.Equals, atlas.stdout)
		c.Assert(ptah.stderr, qt.Equals, atlas.stderr)
		c.Assert(atlasCapture, qt.Equals, fixture.want)
		c.Assert(ptahCapture, qt.Equals, fixture.want)
	})

	t.Run("external", func(t *testing.T) {
		c := qt.New(t)
		fixture := externalFixture(c, t.TempDir())
		atlas := runInspect(c, reference, fixture.configPath)
		atlasCapture := readCapture(c, fixture)
		removeCapture(c, fixture)
		ptah := runInspect(c, compat, fixture.configPath)
		ptahCapture := readCapture(c, fixture)

		c.Assert(atlas.code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", atlas.stdout, atlas.stderr))
		c.Assert(ptah.code, qt.Equals, atlas.code)
		c.Assert(ptah.stdout, qt.Equals, atlas.stdout)
		c.Assert(ptah.stderr, qt.Equals, atlas.stderr)
		c.Assert(atlasCapture, qt.Equals, fixture.want)
		c.Assert(ptahCapture, qt.Equals, fixture.want)
	})

	t.Run("runtime variable", func(t *testing.T) {
		c := qt.New(t)
		fixture := runtimeVariableFixture(c, t.TempDir())
		atlas := runInspect(c, reference, fixture.configPath)
		atlasCapture := readCapture(c, fixture)
		removeCapture(c, fixture)
		ptah := runInspect(c, compat, fixture.configPath)
		ptahCapture := readCapture(c, fixture)

		c.Assert(atlas.code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", atlas.stdout, atlas.stderr))
		c.Assert(ptah.code, qt.Equals, atlas.code)
		c.Assert(ptah.stdout, qt.Equals, atlas.stdout)
		c.Assert(ptah.stderr, qt.Equals, atlas.stderr)
		c.Assert(atlasCapture, qt.Equals, fixture.want)
		c.Assert(ptahCapture, qt.Equals, fixture.want)
	})

	t.Run("template directory", func(t *testing.T) {
		c := qt.New(t)
		fixture := templateDirectoryFixture(c, t.TempDir())
		atlas := runInspect(c, reference, fixture.configPath)
		atlasCapture := readCapture(c, fixture)
		removeCapture(c, fixture)
		ptah := runInspect(c, compat, fixture.configPath)
		ptahCapture := readCapture(c, fixture)

		c.Assert(atlas.code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", atlas.stdout, atlas.stderr))
		c.Assert(ptah.code, qt.Equals, atlas.code)
		c.Assert(ptah.stdout, qt.Equals, atlas.stdout)
		c.Assert(ptah.stderr, qt.Equals, atlas.stderr)
		c.Assert(atlasCapture, qt.Equals, fixture.want)
		c.Assert(ptahCapture, qt.Equals, fixture.want)
	})

	t.Run("recognized unreferenced sources", func(t *testing.T) {
		c := qt.New(t)
		fixture := unreferencedSourcesFixture(c, t.TempDir())
		atlas := runInspect(c, reference, fixture.configPath)
		atlasCapture := readCapture(c, fixture)
		removeCapture(c, fixture)
		ptah := runInspect(c, compat, fixture.configPath)
		ptahCapture := readCapture(c, fixture)

		c.Assert(atlas.code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", atlas.stdout, atlas.stderr))
		c.Assert(ptah.code, qt.Equals, atlas.code)
		c.Assert(ptah.stdout, qt.Equals, atlas.stdout)
		c.Assert(ptah.stderr, qt.Equals, atlas.stderr)
		c.Assert(atlasCapture, qt.Equals, fixture.want)
		c.Assert(ptahCapture, qt.Equals, fixture.want)
	})
}

func TestTemplateDirectoryIntegrityAndWritebackMatchPinnedAtlas(t *testing.T) {
	reference := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	tests := []struct {
		name        string
		args        []string
		wantCreated bool
		wantSum     bool
	}{
		{
			name:        "new writes migration and checksum",
			args:        []string{"migrate", "new", "added"},
			wantCreated: true,
			wantSum:     true,
		},
		{
			name: "hash keeps rendered checksum virtual",
			args: []string{"migrate", "hash"},
		},
		{
			name: "validate reads rendered checksum",
			args: []string{"migrate", "validate"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			atlasFixture := templateDirectoryCommandFixture(c, t.TempDir())
			ptahFixture := templateDirectoryCommandFixture(c, t.TempDir())
			states := compareTemplateDirectoryCommand(
				c, reference, compat, atlasFixture, ptahFixture, test.args...,
			)

			c.Assert(states.ptah.files, qt.DeepEquals, states.atlas.files)
			c.Assert(states.ptah.sumEntries, qt.DeepEquals, states.atlas.sumEntries)
			c.Assert(states.ptah.created, qt.Equals, states.atlas.created)
			c.Assert(states.ptah.sumPresent, qt.Equals, states.atlas.sumPresent)
			c.Assert(states.atlas.created, qt.Equals, test.wantCreated)
			c.Assert(states.atlas.sumPresent, qt.Equals, test.wantSum)
		})
	}
}

func TestTemplateDirectoryDiffWritebackMatchesPinnedAtlas(t *testing.T) {
	reference := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	atlasFixture := templateDirectoryDiffCommandFixture(c, t.TempDir())
	ptahFixture := templateDirectoryDiffCommandFixture(c, t.TempDir())
	states := compareTemplateDirectoryCommand(
		c, reference, compat, atlasFixture, ptahFixture, "migrate", "diff", "added",
	)

	c.Assert(
		normalizeGeneratedTemplateContents(states.ptah.files),
		qt.DeepEquals,
		normalizeGeneratedTemplateContents(states.atlas.files),
	)
	c.Assert(states.ptah.sumEntries, qt.DeepEquals, states.atlas.sumEntries)
	c.Assert(states.ptah.created, qt.Equals, states.atlas.created)
	c.Assert(states.ptah.sumPresent, qt.Equals, states.atlas.sumPresent)
	c.Assert(states.atlas.created, qt.IsTrue)
	c.Assert(states.atlas.sumPresent, qt.IsTrue)
}

// compareTemplateDirectoryCommand checks command-level parity and returns each
// source tree for the caller to compare at the fidelity owned by that command.
func compareTemplateDirectoryCommand(
	c *qt.C,
	reference, compat string,
	atlasFixture, ptahFixture templateDirectoryCommandFixtureState,
	args ...string,
) templateDirectoryStates {
	c.Helper()
	atlasResult := runProjectCommand(c, reference, atlasFixture.configPath, args...)
	ptahResult := runProjectCommand(c, compat, ptahFixture.configPath, args...)
	c.Assert(atlasResult.code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", atlasResult.stdout, atlasResult.stderr))
	c.Assert(ptahResult.code, qt.Equals, atlasResult.code)
	c.Assert(ptahResult.stdout, qt.Equals, atlasResult.stdout)
	c.Assert(ptahResult.stderr, qt.Equals, atlasResult.stderr)
	return templateDirectoryStates{
		atlas: readTemplateDirectoryState(c, atlasFixture.templateDir),
		ptah:  readTemplateDirectoryState(c, ptahFixture.templateDir),
	}
}

type templateDirectoryCommandFixtureState struct {
	configPath  string
	templateDir string
}

func templateDirectoryCommandFixture(c *qt.C, directory string) templateDirectoryCommandFixtureState {
	c.Helper()
	templateDir := filepath.Join(directory, "templates")
	c.Assert(os.Mkdir(templateDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(templateDir, "1_init.sql"),
		[]byte("CREATE TABLE {{ .table }} (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	configPath := writeConfig(c, directory, `
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
	return templateDirectoryCommandFixtureState{configPath: configPath, templateDir: templateDir}
}

func templateDirectoryDiffCommandFixture(c *qt.C, directory string) templateDirectoryCommandFixtureState {
	c.Helper()
	templateDir := filepath.Join(directory, "templates")
	c.Assert(os.Mkdir(templateDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(templateDir, "1_init.sql"),
		[]byte("CREATE TABLE {{ .table }} (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(directory, "desired.sql"),
		[]byte("CREATE TABLE rendered_widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	configPath := writeConfig(c, directory, fmt.Sprintf(`
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
`, strconv.Quote("sqlite://"+filepath.ToSlash(filepath.Join(directory, "dev.db")))))
	return templateDirectoryCommandFixtureState{configPath: configPath, templateDir: templateDir}
}

type templateDirectoryState struct {
	files      []string
	sumEntries []string
	created    bool
	sumPresent bool
}

type templateDirectoryStates struct {
	atlas templateDirectoryState
	ptah  templateDirectoryState
}

var generatedTemplateMigration = regexp.MustCompile(`^\d{14}_(.+\.sql)$`)

func readTemplateDirectoryState(c *qt.C, directory string) templateDirectoryState {
	c.Helper()
	entries, err := os.ReadDir(directory)
	c.Assert(err, qt.IsNil)
	state := templateDirectoryState{}
	for _, entry := range entries {
		if entry.Name() == migratesum.AtlasFileName {
			state.sumPresent = true
			continue
		}
		contents, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		c.Assert(err, qt.IsNil)
		name := normalizeTemplateMigrationName(entry.Name())
		state.created = state.created || name != entry.Name()
		state.files = append(state.files, name+"="+string(contents))
	}
	sort.Strings(state.files)
	if !state.sumPresent {
		return state
	}
	sumBytes, err := os.ReadFile(filepath.Join(directory, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)
	sum, err := migratesum.Parse(sumBytes)
	c.Assert(err, qt.IsNil)
	for _, entry := range sum.Entries {
		state.sumEntries = append(state.sumEntries, normalizeTemplateMigrationName(entry.Name))
	}
	verification, err := migratesum.VerifyWithFormat(os.DirFS(directory), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(verification.OK(), qt.IsTrue)
	return state
}

func normalizeTemplateMigrationName(name string) string {
	match := generatedTemplateMigration.FindStringSubmatch(name)
	if len(match) != 2 {
		return name
	}
	return "<timestamp>_" + match[1]
}

func normalizeGeneratedTemplateContents(files []string) []string {
	normalized := make([]string, 0, len(files))
	for _, file := range files {
		name, _, found := strings.Cut(file, "=")
		if found && strings.HasPrefix(name, "<timestamp>_") {
			file = name + "=<generated SQL>"
		}
		normalized = append(normalized, file)
	}
	return normalized
}

func TestExternalFailureKeepsPinnedExitAndProgramStderr(t *testing.T) {
	reference := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	directory := t.TempDir()
	configPath := writeConfig(c, directory, fmt.Sprintf(`
data "external" "failure" {
  program = %s
}
env "local" {
  url = data.external.failure
}
`, hclList(helperProgram("fail"))))

	atlas := runInspect(c, reference, configPath)
	ptah := runInspect(c, compat, configPath)

	c.Assert(atlas.code, qt.Equals, 1)
	c.Assert(ptah.code, qt.Equals, atlas.code)
	c.Assert(atlas.stdout, qt.Equals, "")
	c.Assert(ptah.stdout, qt.Equals, atlas.stdout)
	c.Assert(atlas.stderr, qt.Contains, "measured failure on stderr")
	c.Assert(ptah.stderr, qt.Contains, "measured failure on stderr")
}

func TestUnreferencedUnsupportedNamesKeepPinnedRefusal(t *testing.T) {
	reference := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	tests := []string{"composite_schema", "definitely_unknown"}
	for _, sourceType := range tests {
		t.Run(sourceType, func(t *testing.T) {
			c := qt.New(t)
			directory := t.TempDir()
			configPath := writeConfig(c, directory, fmt.Sprintf(`
data %q "unused" {}
env "local" {
  url = %s
}
`, sourceType, strconv.Quote(inspectedDatabaseURL(directory))))

			atlas := runInspect(c, reference, configPath)
			ptah := runInspect(c, compat, configPath)

			c.Assert(atlas.code, qt.Equals, 1)
			c.Assert(ptah.code, qt.Equals, atlas.code)
			c.Assert(atlas.stdout, qt.Equals, "")
			c.Assert(ptah.stdout, qt.Equals, atlas.stdout)
			c.Assert(atlas.stderr, qt.Contains, sourceType)
			c.Assert(ptah.stderr, qt.Contains, sourceType)
		})
	}
}

func sqlFixture(c *qt.C, directory string) sourceFixture {
	c.Helper()
	databasePath := filepath.Join(directory, "query.db")
	database, err := sql.Open("sqlite", databasePath)
	c.Assert(err, qt.IsNil)
	_, err = database.Exec(`CREATE TABLE tenants (name TEXT); INSERT INTO tenants VALUES ('alpha'), ('beta')`)
	c.Assert(err, qt.IsNil)
	c.Assert(database.Close(), qt.IsNil)
	capture := filepath.Join(directory, oracleCapture)
	configPath := writeConfig(c, directory, fmt.Sprintf(`
data "sql" "selected" {
  url   = %s
  query = "SELECT name FROM tenants ORDER BY name"
}
data "external" "capture" {
	program     = %s
	working_dir = "."
}
locals {
  captured = data.external.capture
}
env "local" {
  url = %s
}
`,
		strconv.Quote("sqlite://"+filepath.ToSlash(databasePath)),
		hclList(helperProgram("capture", "${jsonencode(data.sql.selected)}")),
		strconv.Quote(inspectedDatabaseURL(directory)),
	))
	return sourceFixture{
		configPath: configPath,
		capture:    capture,
		want:       `{"count":2,"value":"alpha","values":["alpha","beta"]}`,
	}
}

func sqlNumericArgumentFixture(c *qt.C, directory string) sourceFixture {
	c.Helper()
	capture := filepath.Join(directory, oracleCapture)
	configPath := writeConfig(c, directory, fmt.Sprintf(`
data "sql" "selected" {
  url   = %s
  query = "SELECT typeof(?)"
  args  = [42]
}
data "external" "capture" {
	program     = %s
	working_dir = "."
}
locals {
  captured = data.external.capture
}
env "local" {
  url = %s
}
`,
		strconv.Quote(inspectedDatabaseURL(directory)),
		hclList(helperProgram("capture", "${jsonencode(data.sql.selected)}")),
		strconv.Quote(inspectedDatabaseURL(directory)),
	))
	return sourceFixture{
		configPath: configPath,
		capture:    capture,
		want:       `{"count":1,"value":"real","values":["real"]}`,
	}
}

func externalFixture(c *qt.C, directory string) sourceFixture {
	c.Helper()
	capture := filepath.Join(directory, oracleCapture)
	value := "external output with trailing newline\n"
	configPath := writeConfig(c, directory, fmt.Sprintf(`
data "external" "selected" {
  program = %s
}
data "external" "capture" {
	program     = %s
	working_dir = "."
}
locals {
  captured = data.external.capture
}
env "local" {
  url = %s
}
`,
		hclList(helperProgram("output", value)),
		hclList(helperProgram("capture", "${data.external.selected}")),
		strconv.Quote(inspectedDatabaseURL(directory)),
	))
	return sourceFixture{configPath: configPath, capture: capture, want: value}
}

func runtimeVariableFixture(c *qt.C, directory string) sourceFixture {
	c.Helper()
	value := "runtime value with trailing newline\n"
	capture := filepath.Join(directory, oracleCapture)
	query := url.Values{"val": []string{value}}
	configPath := writeConfig(c, directory, fmt.Sprintf(`
data "runtimevar" "selected" {
  url = %s
}
data "external" "capture" {
	program     = %s
	working_dir = "."
}
locals {
  captured = data.external.capture
}
env "local" {
  url = %s
}
`,
		strconv.Quote((&url.URL{Scheme: "constant", RawQuery: query.Encode()}).String()),
		hclList(helperProgram("capture", "${data.runtimevar.selected}")),
		strconv.Quote(inspectedDatabaseURL(directory)),
	))
	return sourceFixture{configPath: configPath, capture: capture, want: value}
}

func templateDirectoryFixture(c *qt.C, directory string) sourceFixture {
	c.Helper()
	templateDirectory := filepath.Join(directory, "templates")
	c.Assert(os.Mkdir(templateDirectory, 0o700), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(templateDirectory, "shared"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(templateDirectory, "1_init.sql"),
		[]byte("CREATE TABLE {{ .table }} (id INTEGER PRIMARY KEY);\n{{ template \"shared/users\" .table }}"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(templateDirectory, "shared", "users.sql"),
		[]byte("{{ define \"shared/users\" }}CREATE TABLE users_{{ . }} (id INTEGER);\n{{ end }}"),
		0o600,
	), qt.IsNil)
	capture := filepath.Join(directory, oracleCapture)
	configPath := writeConfig(c, directory, fmt.Sprintf(`
data "template_dir" "selected" {
  path = "templates"
  vars = {
    table  = "rendered_widgets"
    tables = tolist(["rendered_widgets"])
  }
}
data "external" "capture" {
	program     = %s
	working_dir = "."
}
locals {
  captured = data.external.capture
}
env "local" {
  url = %s
}
`,
		hclList(helperProgram("capture", "${data.template_dir.selected.url}")),
		strconv.Quote(inspectedDatabaseURL(directory)),
	))
	want := (&url.URL{Scheme: "mem", Path: filepath.Join("templates", "selected")}).String()
	return sourceFixture{configPath: configPath, capture: capture, want: want}
}

func unreferencedSourcesFixture(c *qt.C, directory string) sourceFixture {
	c.Helper()
	configPath := writeConfig(c, directory, fmt.Sprintf(`
data "hcl_schema" "unused_hcl" {
  path = "missing-schema.hcl"
}
data "external_schema" "unused_external_schema" {
  program = ["missing-external-schema-program"]
}
data "sql" "unused_sql" {
  url   = "sqlite://missing/unused.db"
  query = "SELECT missing"
}
data "external" "unused_external" {
  program = ["missing-project-data-program"]
}
data "runtimevar" "unused_runtimevar" {
  url = "file://missing-runtime-value"
}
data "template_dir" "unused_template" {
  path = "missing-template"
  vars = {}
}
data "remote_dir" "unused_remote" {
  name = "missing-cloud-directory"
}
data "remote_schema" "unused_remote_schema" {
  name = "missing-cloud-schema"
}
data "aws_rds_token" "unused_aws" {
  endpoint = "missing.invalid:5432"
  username = "nobody"
}
data "gcp_cloudsql_token" "unused_gcp" {}
env "local" {
  url = %s
}
`, strconv.Quote(inspectedDatabaseURL(directory))))
	return sourceFixture{configPath: configPath}
}

func writeConfig(c *qt.C, directory, body string) string {
	c.Helper()
	path := filepath.Join(directory, "atlas.hcl")
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

func inspectedDatabaseURL(directory string) string {
	return "sqlite://" + filepath.ToSlash(filepath.Join(directory, "inspected.db"))
}

func helperProgram(arguments ...string) []string {
	executable, err := os.Executable()
	if err != nil {
		panic(err)
	}
	return append([]string{executable, oracleHelperFlag}, arguments...)
}

func hclList(arguments []string) string {
	quoted := make([]string, 0, len(arguments))
	for _, argument := range arguments {
		if expression, ok := strings.CutPrefix(argument, "${"); ok {
			quoted = append(quoted, strings.TrimSuffix(expression, "}"))
			continue
		}
		quoted = append(quoted, strconv.Quote(argument))
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func readCapture(c *qt.C, fixture sourceFixture) string {
	c.Helper()
	if fixture.capture == "" {
		return ""
	}
	contents, err := os.ReadFile(fixture.capture)
	c.Assert(err, qt.IsNil)
	return string(contents)
}

func removeCapture(c *qt.C, fixture sourceFixture) {
	c.Helper()
	if fixture.capture == "" {
		return
	}
	c.Assert(os.Remove(fixture.capture), qt.IsNil)
}

func runInspect(c *qt.C, binary, configPath string) commandResult {
	c.Helper()
	warmUpOracle(c, binary)
	// #nosec G204 -- binary is an explicit pinned test input.
	command := exec.Command(binary,
		"schema", "inspect",
		"--config", "file://"+filepath.ToSlash(configPath),
		"--env", "local",
	)
	command.Dir = filepath.Dir(configPath)
	command.Env = environmentWithoutPtahVariables(os.Environ())
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return commandResult{code: exitError.ExitCode(), stdout: stdout.String(), stderr: stderr.String()}
	}
	c.Assert(err, qt.IsNil, qt.Commentf("%s schema inspect: %s", binary, stderr.String()))
	return commandResult{stdout: stdout.String(), stderr: stderr.String()}
}

func runProjectCommand(c *qt.C, binary, configPath string, arguments ...string) commandResult {
	c.Helper()
	warmUpOracle(c, binary)
	arguments = append(arguments,
		"--config", "file://"+filepath.ToSlash(configPath),
		"--env", "local",
	)
	// #nosec G204 -- binary is an explicit pinned test input.
	command := exec.Command(binary, arguments...)
	command.Dir = filepath.Dir(configPath)
	command.Env = environmentWithoutPtahVariables(os.Environ())
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return commandResult{code: exitError.ExitCode(), stdout: stdout.String(), stderr: stderr.String()}
	}
	c.Assert(err, qt.IsNil, qt.Commentf("%s %s: %s", binary, strings.Join(arguments, " "), stderr.String()))
	return commandResult{stdout: stdout.String(), stderr: stderr.String()}
}

func environmentWithoutPtahVariables(environment []string) []string {
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(key), "PTAH_") {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

var warmedOracles sync.Map

func warmUpOracle(c *qt.C, binary string) {
	c.Helper()
	if _, loaded := warmedOracles.LoadOrStore(binary, struct{}{}); loaded {
		return
	}
	directory := c.TempDir()
	configPath := writeConfig(c, directory, fmt.Sprintf(`
env "local" {
  url = %s
}
`, strconv.Quote(inspectedDatabaseURL(directory))))
	// #nosec G204 -- binary is an explicit pinned test input.
	command := exec.Command(binary,
		"schema", "inspect",
		"--config", "file://"+filepath.ToSlash(configPath),
		"--env", "local",
	)
	command.Dir = directory
	command.Env = environmentWithoutPtahVariables(os.Environ())
	// The run exists only to consume Atlas's first-use edition notice. The real
	// differential command reports any subsequent startup failure.
	_ = command.Run()
}

func buildCompatBinary(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "ptah-compat")
	output, err := exec.Command("go", "build", "-o", path, "go.5x5.cz/ptah/cmd/ptah-compat").CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("build ptah-compat: %s", output))
	return path
}

func requireAtlasOracle(t *testing.T) string {
	t.Helper()
	reference := os.Getenv(referenceEnv)
	if reference == "" {
		t.Skipf("SKIPPED: set %s to the pinned Atlas CE binary (%s) to run project-data conformance",
			referenceEnv, referenceVersion)
	}
	// #nosec G204 -- the operator supplies the pinned reference path.
	output, err := exec.Command(reference, "version").Output()
	if err != nil {
		t.Fatalf("%s=%s is not runnable: %v", referenceEnv, reference, err)
	}
	got, _, _ := strings.Cut(string(output), "\n")
	if strings.TrimSpace(got) != referenceVersion {
		t.Fatalf("%s=%s reports %q, want %q; a different build may have changed the source contract",
			referenceEnv, reference, strings.TrimSpace(got), referenceVersion)
	}
	return reference
}
