package atlas_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// `--export` selects an output template the project declares once, instead of
// one every invocation spells out. An exporter is a Go text/template over the
// same report `--format` renders, so the flag needs no evaluator of its own
// (stokaro/ptah#1620).

const exportProjectHCL = `exporter "markdown" {
  template = "## Rollout\n{{ range .Changes }}- {{ .Cmd }}\n{{ end }}"
}

env "local" {
  url      = "sqlite://%s"
  dev      = "sqlite://%s"
  exporter = "markdown"
}
`

// writeExportProject writes an atlas.hcl declaring one exporter and an env
// that selects it, and returns the config path.
func writeExportProject(c *qt.C, dir, body string) string {
	c.Helper()
	path := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

func runExportCommand(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaDiffExportRendersTheDeclaredTemplate is the flag end to end.
func TestSchemaDiffExportRendersTheDeclaredTemplate(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	dev := filepath.Join(dir, "dev.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	desired := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(desired,
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n"),
		0o600), qt.IsNil)
	config := writeExportProject(c, dir, exportProjectBody(target, dev))

	out, err := runExportCommand(c, "schema", "diff",
		"-c", "file://"+config, "--env", "local",
		"--from", "sqlite://"+target, "--to", "file://"+desired,
		"--dev-url", "sqlite://"+dev, "--export")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "## Rollout")
	c.Assert(out, qt.Contains, "orders")
}

// TestSchemaDiffWithoutExportKeepsTheDefault is the control: the same project,
// the same command, one flag apart. Without it, the test above would pass on a
// build that rendered the template unconditionally.
func TestSchemaDiffWithoutExportKeepsTheDefault(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	dev := filepath.Join(dir, "dev.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	desired := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(desired,
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n"),
		0o600), qt.IsNil)
	config := writeExportProject(c, dir, exportProjectBody(target, dev))

	out, err := runExportCommand(c, "schema", "diff",
		"-c", "file://"+config, "--env", "local",
		"--from", "sqlite://"+target, "--to", "file://"+desired,
		"--dev-url", "sqlite://"+dev)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "## Rollout")
	c.Assert(out, qt.Contains, "CREATE TABLE")
}

// TestSchemaInspectExportRendersTheDeclaredTemplate covers the twin verb.
//
// The flag was registered on neither until now; registering it on one and not
// the other is the inconsistency the earlier batch flagged and deferred.
func TestSchemaInspectExportRendersTheDeclaredTemplate(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	config := writeExportProject(c, dir, `exporter "json_report" {
  template = "EXPORTED {{ json . }}"
}

env "local" {
  url      = "sqlite://`+hclPath(target)+`"
  exporter = "json_report"
}
`)

	out, err := runExportCommand(c, "schema", "inspect",
		"-c", "file://"+config, "--env", "local", "--export")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "EXPORTED ")
	c.Assert(out, qt.Contains, `"users"`)
}

// TestSchemaExportRefusesWhatItCannotResolve covers every way an export can
// fail to name a template.
//
// Each case would otherwise emit the ordinary report and let the operator
// believe their exporter ran, which is the failure the flag was a registered
// refusal to avoid. Implementing it must not reintroduce it.
func TestSchemaExportRefusesWhatItCannotResolve(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		extra  []string
		want   string
	}{
		{
			name: "env selects no exporter",
			config: `exporter "markdown" {
  template = "x"
}

env "local" {
  url = "sqlite://%s"
}
`,
			want: "this env selects no exporter",
		},
		{
			name: "env selects an exporter nothing declares",
			config: `exporter "markdown" {
  template = "x"
}

env "local" {
  url      = "sqlite://%s"
  exporter = "missing"
}
`,
			want: `no exporter "missing": this project declares [markdown]`,
		},
		{
			name: "both --export and --format choose the output",
			config: `exporter "markdown" {
  template = "x"
}

env "local" {
  url      = "sqlite://%s"
  exporter = "markdown"
}
`,
			extra: []string{"--format", "{{ json . }}"},
			want:  "--export and --format both choose the output",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			target := filepath.Join(dir, "target.db")
			createSQLiteSchemaCleanTable(c, target, "users")
			config := writeExportProject(c, dir, sprintfDB(test.config, target))

			args := append([]string{"schema", "inspect",
				"-c", "file://" + config, "--env", "local", "--export"}, test.extra...)
			out, err := runExportCommand(c, args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out+err.Error(), qt.Contains, test.want)
		})
	}
}

// TestSchemaExporterWithoutATemplateIsRefused covers a block that declares a
// name and nothing to render.
//
// An empty template renders to an empty string, so accepting it would print
// nothing at all and exit 0 -- the most complete version of the failure this
// flag exists to avoid, since there is not even a default report to notice.
func TestSchemaExporterWithoutATemplateIsRefused(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	config := writeExportProject(c, dir, `exporter "markdown" {
  format = "not-a-template"
}

env "local" {
  url      = "sqlite://`+hclPath(target)+`"
  exporter = "markdown"
}
`)

	out, err := runExportCommand(c, "schema", "inspect",
		"-c", "file://"+config, "--env", "local", "--export")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, `exporter "markdown" declares no template`)
}

// TestSchemaExportDeclaredTwiceIsRefused keeps two blocks under one name from
// making --export depend on which the parser reached last.
func TestSchemaExportDeclaredTwiceIsRefused(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	config := writeExportProject(c, dir, `exporter "markdown" {
  template = "first"
}

exporter "markdown" {
  template = "second"
}

env "local" {
  url      = "sqlite://`+hclPath(target)+`"
  exporter = "markdown"
}
`)

	out, err := runExportCommand(c, "schema", "inspect",
		"-c", "file://"+config, "--env", "local", "--export")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, `declares exporter "markdown" more than once`)
}

// exportProjectBody fills the shared project template with a target and dev
// database path.
func exportProjectBody(target, dev string) string {
	return fmt.Sprintf(exportProjectHCL, hclPath(target), hclPath(dev))
}

// hclPath makes a filesystem path safe to embed in a quoted HCL string.
//
// A Windows temp path is C:\Users\RUNNER~1\..., and HCL reads a backslash as
// the start of an escape sequence: \U there is "the \U escape must be followed
// by eight hexadecimal digits", so the whole config fails to parse. POSIX paths
// have no backslashes, which is why this only ever failed on Windows -- and the
// Windows run was right (stokaro/ptah#1620).
//
// Forward slashes rather than doubled backslashes: sqlite and file:// URLs take
// them on Windows, and a URL is what every one of these paths becomes.
//
// The replacement is explicit rather than filepath.ToSlash, which keys off
// os.PathSeparator and is therefore a no-op everywhere except Windows. That
// would have fixed the failing job while leaving the fix unverifiable on the
// platforms this repository is developed on -- and a fix no local test can
// exercise is one the next refactor silently undoes.
func hclPath(path string) string {
	return strings.ReplaceAll(path, `\`, "/")
}

// sprintfDB fills a single %s in a config body with the database path, and
// leaves a body carrying none unchanged.
func sprintfDB(body, target string) string {
	if !strings.Contains(body, "%s") {
		return body
	}
	return fmt.Sprintf(body, hclPath(target))
}

// TestExportProjectBodyEscapesAWindowsPath reproduces, on any platform, the
// failure that only Windows saw.
//
// The fixtures embed a filesystem path in a quoted HCL string. A Windows temp
// path is `C:\Users\RUNNER~1\...`, and HCL reads the backslash as an escape:
// `\U` is "the \U escape sequence must be followed by eight hexadecimal
// digits", so the config fails to parse before any exporter is reached. POSIX
// paths carry no backslashes, so every macOS and Linux run passed while the
// Windows job failed on all six export tests.
//
// Asserting on the rendered body rather than on hclPath's return value is what
// makes this a test of the fixture: a future config template that interpolates
// a path without going through the helper reddens here.
func TestExportProjectBodyEscapesAWindowsPath(t *testing.T) {
	c := qt.New(t)
	windowsPath := `C:\Users\RUNNER~1\AppData\Local\Temp\TestExport001\target.db`

	body := exportProjectBody(windowsPath, windowsPath)

	// The assertion is on the url lines, not the whole body: the exporter's
	// own template legitimately contains `\n`, and a blanket no-backslash rule
	// would fail on that rather than on the defect.
	urlLines := urlLinesOf(body)
	c.Assert(urlLines, qt.Not(qt.HasLen), 0, qt.Commentf("%s", body))
	for _, line := range urlLines {
		c.Assert(line, qt.Not(qt.Contains), `\`)
		c.Assert(line, qt.Contains, "C:/Users/RUNNER~1/AppData/Local/Temp/TestExport001/target.db")
	}
}

// urlLinesOf returns the config lines carrying a database URL, which are the
// ones an interpolated path reaches.
func urlLinesOf(body string) []string {
	var lines []string
	for line := range strings.SplitSeq(body, "\n") {
		if strings.Contains(line, "sqlite://") {
			lines = append(lines, line)
		}
	}
	return lines
}

// The three cases below came from review on stokaro/ptah#1620. Each was
// reproduced before being fixed, and each is a way --export could look like it
// worked while doing something else.

// TestSchemaExportFalseKeepsTheDefaultOutput covers the explicit false.
//
// Cobra marks a boolean flag changed for `--export=false` too, so testing
// Changed alone applied the exporter on an invocation that asked for ordinary
// output -- and errored on a project declaring none. Generated command lines
// pass explicit booleans, so this is a spelling real callers use.
func TestSchemaExportFalseKeepsTheDefaultOutput(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	dev := filepath.Join(dir, "dev.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	desired := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(desired,
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n"),
		0o600), qt.IsNil)
	config := writeExportProject(c, dir, exportProjectBody(target, dev))

	out, err := runExportCommand(c, "schema", "diff",
		"-c", "file://"+config, "--env", "local",
		"--from", "sqlite://"+target, "--to", "file://"+desired,
		"--dev-url", "sqlite://"+dev, "--export=false")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "## Rollout")
	c.Assert(out, qt.Contains, "CREATE TABLE")
}

// TestSchemaExporterWithAnEmptyTemplateIsRefused covers a template that is
// present and blank.
//
// It renders nothing, so an export selecting it printed nothing at all and
// exited 0 -- indistinguishable from no exporter being selected, because the
// caller read selection off the returned string. Refusing it at parse time is
// what keeps the two apart.
func TestSchemaExporterWithAnEmptyTemplateIsRefused(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	config := writeExportProject(c, dir, `exporter "blank" {
  template = ""
}

env "local" {
  url      = "sqlite://`+hclPath(target)+`"
  exporter = "blank"
}
`)

	out, err := runExportCommand(c, "schema", "inspect",
		"-c", "file://"+config, "--env", "local", "--export")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, `exporter "blank" declares an empty template`)
}

// TestSchemaExporterNestedBlockIsStillEvaluated keeps this surface from being
// looser than the community binary.
//
// Before the exporter parser existed, the block took the unknown-top-level-name
// path, whose body evaluation refuses a child expression like
// `metadata { value = var.missing }` exactly as Atlas CE refuses it. Walking
// only Attributes made that configuration succeed while silently discarding the
// child. The control is the same bad reference inside an unknown block, which
// was refused throughout.
func TestSchemaExporterNestedBlockIsStillEvaluated(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	config := writeExportProject(c, dir, `exporter "markdown" {
  template = "x"
  metadata {
    value = var.missing
  }
}

env "local" {
  url      = "sqlite://`+hclPath(target)+`"
  exporter = "markdown"
}
`)

	out, err := runExportCommand(c, "schema", "inspect",
		"-c", "file://"+config, "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, `no variable named "var"`)
}

// TestUnknownBlockNestedBadReferenceIsRefused is that control, held here so the
// test above is measuring the exporter path and not a rule that stopped
// applying anywhere.
func TestUnknownBlockNestedBadReferenceIsRefused(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	config := writeExportProject(c, dir, `frobnicate "x" {
  metadata {
    value = var.missing
  }
}

env "local" {
  url = "sqlite://`+hclPath(target)+`"
}
`)

	out, err := runExportCommand(c, "schema", "inspect",
		"-c", "file://"+config, "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, `no variable named "var"`)
}
