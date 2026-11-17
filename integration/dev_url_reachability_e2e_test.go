//go:build integration

package integration_test

import (
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// A dev database URL is opened by every command that is given one, whether or
// not the command needs the database. The pinned community binary v1.3.0
// opens it on every `schema diff` and `schema apply`, and a URL nothing answers
// on fails the run there:
//
//	Error: postgres: scanning system variables: dial tcp 127.0.0.1:1: connect: connection refused
//
// Each row below is a command that planned without opening the URL and passed.

// closedDevURL names a port nothing listens on. The password is long enough
// that redacting it from an error leaves the rest of the error readable.
//
// #nosec G101 -- a fixture password for a port nothing listens on.
const closedDevURL = "postgres://ptah:closed-port-secret@127.0.0.1:1/dev?sslmode=disable&search_path=public"

// closedDevRefusal is the part of the driver's error both binaries print.
const closedDevRefusal = "dial tcp 127.0.0.1:1: connect: connection refused"

// devReachFixture is a target database holding table t, two schema files and a
// hashed migration directory none of the rows applies.
type devReachFixture struct {
	target string
	from   string
	to     string
	dir    string
	output string
}

// devReachRow is one command that takes a dev database URL.
type devReachRow struct {
	name   string
	binary clirun.Target
	// flag is the dev database flag the command takes and names in its
	// refusal.
	flag string
	// refusedCode is the exit status of a run whose dev URL nothing answers
	// on.
	refusedCode int
	args        func(f devReachFixture) []string
}

var compatDevReachRows = []devReachRow{
	{
		name: "schema diff of two files", binary: clirun.Compat, flag: "dev-url", refusedCode: 1,
		args: func(f devReachFixture) []string {
			return []string{"schema", "diff", "--from", "file://" + f.from, "--to", "file://" + f.to}
		},
	},
	{
		name: "schema diff of a database and a file", binary: clirun.Compat, flag: "dev-url", refusedCode: 1,
		args: func(f devReachFixture) []string {
			return []string{"schema", "diff", "--from", f.target, "--to", "file://" + f.to}
		},
	},
	{
		name: "schema diff of two databases", binary: clirun.Compat, flag: "dev-url", refusedCode: 1,
		args: func(f devReachFixture) []string {
			return []string{"schema", "diff", "--from", f.target, "--to", f.target}
		},
	},
	{
		name: "schema apply of a database", binary: clirun.Compat, flag: "dev-url", refusedCode: 1,
		args: func(f devReachFixture) []string {
			return []string{"schema", "apply", "--url", f.target, "--to", f.target, "--dry-run"}
		},
	},
	{
		name: "schema apply of a file", binary: clirun.Compat, flag: "dev-url", refusedCode: 1,
		args: func(f devReachFixture) []string {
			return []string{"schema", "apply", "--url", f.target, "--to", "file://" + f.to, "--dry-run"}
		},
	},
}

var compatOnlyDevReachRows = []devReachRow{
	{
		name: "schema plan", binary: clirun.Compat, flag: "dev-url", refusedCode: 1,
		args: func(f devReachFixture) []string {
			return []string{"schema", "plan", "--from", f.target, "--to", "file://" + f.to}
		},
	},
	{
		name: "migrate down with nothing to roll back", binary: clirun.Compat, flag: "dev-url", refusedCode: 1,
		args: func(f devReachFixture) []string {
			return []string{"migrate", "down", "--url", f.target, "--dir", "file://" + f.dir}
		},
	},
	{
		name: "migrate down with a format and nothing to roll back", binary: clirun.Compat, flag: "dev-url", refusedCode: 1,
		args: func(f devReachFixture) []string {
			return []string{"migrate", "down", "--url", f.target, "--dir", "file://" + f.dir, "--format", "{{ json . }}"}
		},
	},
}

var nativeDevReachRows = []devReachRow{
	{
		name: "schema diff of two files", binary: clirun.Ptah, flag: "dev-url", refusedCode: 2,
		args: func(f devReachFixture) []string {
			return []string{"schema", "diff", "--from", f.from, "--to", f.to}
		},
	},
	{
		name: "schema diff of a database and a file", binary: clirun.Ptah, flag: "dev-url", refusedCode: 2,
		args: func(f devReachFixture) []string {
			return []string{"schema", "diff", "--from", f.target, "--to", f.to}
		},
	},
	{
		name: "schema compare", binary: clirun.Ptah, flag: "dev-url", refusedCode: 2,
		args: func(f devReachFixture) []string {
			return []string{"schema", "compare", "--db-url", f.target, "--schema-file", f.to}
		},
	},
	{
		name: "schema plan", binary: clirun.Ptah, flag: "dev-url", refusedCode: 2,
		args: func(f devReachFixture) []string {
			return []string{"schema", "plan", "--db-url", f.target, "--schema-file", f.to, "--output", f.output}
		},
	},
	{
		name: "migrations down with nothing to roll back", binary: clirun.Ptah, flag: "shadow-db", refusedCode: 2,
		args: func(f devReachFixture) []string {
			return []string{"migrations", "down", "--db-url", f.target, "--migrations-dir", f.dir, "--dir-format", "atlas", "--target", "0"}
		},
	},
	{
		name: "migrations baseline dry run", binary: clirun.Ptah, flag: "shadow-db", refusedCode: 2,
		args: func(f devReachFixture) []string {
			return []string{"migrations", "baseline", "--db-url", f.target, "--migrations-dir", f.dir, "--dir-format", "atlas", "--dry-run"}
		},
	},
}

// nativeProjectDevRows are the native commands that read the project file's
// `dev:` key and need nothing from the dev database on these arguments. Each
// is run with closedDevURL as that key and no flag, and succeeds: a default
// for every command in the project is not opened by one that does not use it.
// `schema compare` reads no `dev:` key at all, and its row holds that.
var nativeProjectDevRows = []devReachRow{
	nativeDevReachRows[0], // schema diff of two files
	nativeDevReachRows[1], // schema diff of a database and a file
	nativeDevReachRows[2], // schema compare
	nativeDevReachRows[3], // schema plan
	nativeDevReachRows[4], // migrations down with nothing to roll back
	nativeSchemaInspectRow,
}

// nativeSchemaInspectRow reads a database, which never needs the dev
// database, so the dev URL is never opened, from a flag or from the project
// file. ptah-compat's `schema inspect` does not open it either, as the pinned
// community binary does not.
var nativeSchemaInspectRow = devReachRow{
	name: "schema inspect of a database", binary: clirun.Ptah, flag: "dev-url",
	args: func(f devReachFixture) []string {
		return []string{"schema", "inspect", "--db-url", f.target}
	},
}

// compatSchemaInspectRow is the ptah-compat counterpart of
// nativeSchemaInspectRow.
var compatSchemaInspectRow = devReachRow{
	name: "schema inspect of a database", binary: clirun.Compat, flag: "dev-url",
	args: func(f devReachFixture) []string {
		return []string{"schema", "inspect", "--url", f.target}
	},
}

// newDevReachFixture creates the target database and the files the rows read.
func newDevReachFixture(c *qt.C) devReachFixture {
	c.Helper()
	target := postgresScratchDevURL(c, "ptah_devreach")
	execPostgresFamilySQL(c, c.Context(), target, "CREATE TABLE t (id integer)")
	root := c.TempDir()
	from := filepath.Join(root, "from.sql")
	to := filepath.Join(root, "to.sql")
	dir := filepath.Join(root, "migrations")
	c.Assert(os.WriteFile(from, []byte("CREATE TABLE t (id integer);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte("CREATE TABLE t (id integer, b text);\n"), 0o600), qt.IsNil)
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_init.sql"), []byte("CREATE TABLE t2 (id integer);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return devReachFixture{
		target: pinnedDevURL(c, target, "public"),
		from:   from,
		to:     to,
		dir:    dir,
		output: filepath.Join(root, "plan.json"),
	}
}

// runDevReachRow runs row with binary and dev as its dev database URL.
func runDevReachRow(c *qt.C, binary string, f devReachFixture, row devReachRow, dev string) clirun.Result {
	c.Helper()
	return runFormatTemplateVerb(c, binary, c.TempDir(), nil, append(row.args(f), "--"+row.flag, dev)...)
}

// assertDevURLRefused runs row with the closed dev URL and checks that the run
// failed on it before writing a plan.
func assertDevURLRefused(c *qt.C, binary string, f devReachFixture, row devReachRow) clirun.Result {
	c.Helper()
	got := runDevReachRow(c, binary, f, row, closedDevURL)
	c.Assert(got.ExitCode, qt.Equals, row.refusedCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, closedDevRefusal)
	c.Assert(got.Stdout, qt.Not(qt.Contains), "ADD COLUMN")
	return got
}

// assertDevURLAnswered runs row with a dev database that answers and checks
// that the run succeeds, so a refusal above is about the URL and nothing else.
func assertDevURLAnswered(c *qt.C, binary string, f devReachFixture, row devReachRow) {
	c.Helper()
	got := runDevReachRow(c, binary, f, row, pinnedDevURL(c, postgresScratchDevURL(c, "ptah_devreach_dev"), "public"))
	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
}

// TestDevURLNothingAnswersOnIsRefusedE2E is every command of both binaries
// refusing a dev database URL nothing answers on, in Ptah's words, which name
// the flag the operator typed.
func TestDevURLNothingAnswersOnIsRefusedE2E(t *testing.T) {
	f := newDevReachFixture(qt.New(t))
	for _, row := range slices.Concat(compatDevReachRows, compatOnlyDevReachRows, nativeDevReachRows) {
		t.Run(path.Base(string(row.binary))+" "+row.name, func(t *testing.T) {
			c := qt.New(t)
			got := assertDevURLRefused(c, clirun.Build(c, row.binary), f, row)
			c.Assert(got.Stderr, qt.Contains, "connect to --"+row.flag+": ")
		})
	}
}

// TestDevURLThatAnswersIsUsedE2E is the control: the same commands succeed
// beside a dev database that answers.
func TestDevURLThatAnswersIsUsedE2E(t *testing.T) {
	f := newDevReachFixture(qt.New(t))
	for _, row := range slices.Concat(compatDevReachRows, compatOnlyDevReachRows, nativeDevReachRows) {
		t.Run(path.Base(string(row.binary))+" "+row.name, func(t *testing.T) {
			c := qt.New(t)
			assertDevURLAnswered(c, clirun.Build(c, row.binary), f, row)
		})
	}
}

// TestReferenceDevURLNothingAnswersOnIsRefusedE2E is the measurement the
// compat rows restate, taken against the pinned community binary.
func TestReferenceDevURLNothingAnswersOnIsRefusedE2E(t *testing.T) {
	reference := requireEngineReference(t)
	f := newDevReachFixture(qt.New(t))
	for _, row := range compatDevReachRows {
		t.Run(row.name, func(t *testing.T) {
			assertDevURLRefused(qt.New(t), reference, f, row)
		})
	}
}

// TestReferenceDevURLThatAnswersIsUsedE2E is the reference control.
func TestReferenceDevURLThatAnswersIsUsedE2E(t *testing.T) {
	reference := requireEngineReference(t)
	f := newDevReachFixture(qt.New(t))
	for _, row := range compatDevReachRows {
		t.Run(row.name, func(t *testing.T) {
			assertDevURLAnswered(qt.New(t), reference, f, row)
		})
	}
}

// envName is the PTAH_* variable a native flag reads.
func envName(flag string) string {
	return "PTAH_" + strings.ToUpper(strings.ReplaceAll(flag, "-", "_"))
}

// TestDevURLInTheEnvironmentNothingAnswersOnIsRefusedE2E is the native rows
// given the closed dev URL through the flag's PTAH_* variable instead of the
// flag. A variable is set for the run, as a flag is, so the run refuses it.
func TestDevURLInTheEnvironmentNothingAnswersOnIsRefusedE2E(t *testing.T) {
	f := newDevReachFixture(qt.New(t))
	for _, row := range nativeDevReachRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			got := runFormatTemplateVerb(c, clirun.Build(c, row.binary), c.TempDir(),
				[]string{envName(row.flag) + "=" + closedDevURL}, row.args(f)...)
			c.Assert(got.ExitCode, qt.Equals, row.refusedCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "connect to --"+row.flag+": ")
			c.Assert(got.Stderr, qt.Contains, closedDevRefusal)
		})
	}
}

// TestDevURLInTheProjectFileIsNotOpenedByANativeCommandThatDoesNotUseItE2E is
// each native command that needs nothing from the dev database run with
// closedDevURL as the project file's `dev:` key. The run succeeds, where the
// same URL given as the flag is refused by
// TestDevURLNothingAnswersOnIsRefusedE2E.
func TestDevURLInTheProjectFileIsNotOpenedByANativeCommandThatDoesNotUseItE2E(t *testing.T) {
	f := newDevReachFixture(qt.New(t))
	for _, row := range nativeProjectDevRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte("dev: "+strconv.Quote(closedDevURL)+"\n"), 0o600), qt.IsNil)
			got := runFormatTemplateVerb(c, clirun.Build(c, row.binary), dir, nil, row.args(f)...)
			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Not(qt.Contains), closedDevRefusal)
		})
	}
}

// TestDevURLOfSchemaInspectOfADatabaseIsNeverOpenedE2E is `schema inspect` of
// a database given the closed dev URL as a flag, on both binaries: it reads
// the database and succeeds.
func TestDevURLOfSchemaInspectOfADatabaseIsNeverOpenedE2E(t *testing.T) {
	f := newDevReachFixture(qt.New(t))
	for _, row := range []devReachRow{nativeSchemaInspectRow, compatSchemaInspectRow} {
		t.Run(path.Base(string(row.binary)), func(t *testing.T) {
			c := qt.New(t)
			got := runDevReachRow(c, clirun.Build(c, row.binary), f, row, closedDevURL)
			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, `table "t"`)
		})
	}
}

// compatProjectDevRows are ptah-compat commands run with closedDevURL as the
// `dev` of an atlas.hcl environment and no flag. The pinned community binary
// opens a dev URL from its project file as it opens one from the flag, and so
// does ptah-compat.
var compatProjectDevRows = []devReachRow{
	{
		name: "schema diff of two files", binary: clirun.Compat, refusedCode: 1,
		args: func(f devReachFixture) []string {
			return []string{"schema", "diff", "--env", "local", "--from", "file://" + f.from, "--to", "file://" + f.to}
		},
	},
	{
		name: "schema apply of a file", binary: clirun.Compat, refusedCode: 1,
		args: func(f devReachFixture) []string {
			return []string{"schema", "apply", "--env", "local", "--to", "file://" + f.to, "--dry-run"}
		},
	},
}

// assertProjectDevURLRefused runs row in a directory whose atlas.hcl names
// the target and the closed dev URL, and checks that the run fails on the dev
// URL before it writes a plan.
func assertProjectDevURLRefused(c *qt.C, binary string, f devReachFixture, row devReachRow) clirun.Result {
	c.Helper()
	dir := c.TempDir()
	project := "env \"local\" {\n  url = " + strconv.Quote(f.target) + "\n  dev = " + strconv.Quote(closedDevURL) + "\n}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	got := runFormatTemplateVerb(c, binary, dir, nil, row.args(f)...)
	c.Assert(got.ExitCode, qt.Equals, row.refusedCode, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, closedDevRefusal)
	c.Assert(got.Stdout, qt.Not(qt.Contains), "ADD COLUMN")
	return got
}

// TestCompatDevURLInTheProjectFileNothingAnswersOnIsRefusedE2E is the control
// for the native rule above: ptah-compat still refuses a dev URL its project
// file names, on a run that needs nothing from it.
func TestCompatDevURLInTheProjectFileNothingAnswersOnIsRefusedE2E(t *testing.T) {
	f := newDevReachFixture(qt.New(t))
	for _, row := range compatProjectDevRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			got := assertProjectDevURLRefused(c, clirun.Build(c, row.binary), f, row)
			c.Assert(got.Stderr, qt.Contains, "connect to --dev-url: ")
		})
	}
}

// TestReferenceDevURLInTheProjectFileNothingAnswersOnIsRefusedE2E is the
// measurement the compat control restates, taken against the pinned community
// binary.
func TestReferenceDevURLInTheProjectFileNothingAnswersOnIsRefusedE2E(t *testing.T) {
	reference := requireEngineReference(t)
	f := newDevReachFixture(qt.New(t))
	for _, row := range compatProjectDevRows {
		t.Run(row.name, func(t *testing.T) {
			assertProjectDevURLRefused(qt.New(t), reference, f, row)
		})
	}
}

// TestCompatSchemaApplyDevURLOfAnotherDialectIsRefusedBeforeConnectingE2E is
// ptah-compat `schema apply` given a MySQL dev URL beside a PostgreSQL target.
// The dialects are compared before the dev URL is opened, so the refusal names
// the mismatch and not the closed port behind the URL.
func TestCompatSchemaApplyDevURLOfAnotherDialectIsRefusedBeforeConnectingE2E(t *testing.T) {
	c := qt.New(t)
	f := newDevReachFixture(c)
	got := runFormatTemplateVerb(c, clirun.Build(c, clirun.Compat), c.TempDir(), nil,
		"schema", "apply", "--url", f.target, "--to", "file://"+f.to, "--dry-run",
		"--dev-url", "mysql://root:closed-port-secret@127.0.0.1:1/dev")
	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `--dev-url dialect "mysql" does not match --url dialect "postgres"`)
	c.Assert(got.Stderr, qt.Not(qt.Contains), "connection refused")
}
