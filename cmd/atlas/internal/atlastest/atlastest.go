// Package atlastest holds the test helpers that more than one of cmd/atlas's
// test packages needs.
//
// The tests of this command are one package of 2466 tests, which takes a third
// of Go's per-package timeout on the slow Windows runner. Splitting them wants
// the external test files -- 138 of the 157 -- moved into sibling packages,
// and 138 files sharing twenty-odd unexported helpers cannot move while the
// helpers are private to one of them (stokaro/ptah#1812).
//
// So the helpers come here first, exported, as their own change. The move that
// follows is then a rename and a package clause, with nothing to reason about.
package atlastest

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/atlascompat"
	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/testutils"
	"go.5x5.cz/ptah/migration/migrationfile"
)

func SqliteTableCount(c *qt.C, dbPath, table string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	row := conn.QueryRowContext(
		context.Background(),
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?`,
		table,
	)
	var count int
	c.Assert(row.Scan(&count), qt.IsNil)
	return count
}

func SqliteIndexCount(c *qt.C, dbPath, index string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	row := conn.QueryRowContext(
		context.Background(),
		`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = ?`,
		index,
	)
	var count int
	c.Assert(row.Scan(&count), qt.IsNil)
	return count
}

func SqliteHasTable(t *testing.T, dbPath, table string) bool {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

// seedSQLiteDB creates a SQLite database with the given DDL and returns its
// path.
func SeedSQLiteDB(c *qt.C, ddl string) string {
	c.Helper()
	dbPath := filepath.Join(c.TempDir(), "seed.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), ddl)
	c.Assert(err, qt.IsNil)
	return dbPath
}

// seedSQLiteDBAt creates a SQLite database with the given DDL at path.
func SeedSQLiteDBAt(t *testing.T, path, ddl string) {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), ddl)
	c.Assert(err, qt.IsNil)
}

func RunCompat(args ...string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// runCompatStreams runs one compat invocation with stdout and stderr captured
// separately, because some assertions below are about which stream a line
// landed on.
func RunCompatStreams(c *qt.C, args ...string) (stdout, stderr string, err error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func RunCompatCommand(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func RunCompatArgs(args []string) (string, error) {
	return RunCompatOutput(args...)
}

// RunCompatOutput is RunCompatArgs for a caller that spells its arguments out
// rather than building a slice.
//
// Two spellings of one call because both were already in the tree, three times
// over: runAtlasArgs and executeAtlasProjectCommand were byte-identical to
// RunCompatArgs apart from the variadic signature. One of them is now the
// implementation and the other is a line.
func RunCompatOutput(args ...string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// RepoRoot returns the repository root, found by walking up from the test's
// working directory until a go.mod is there.
//
// It exists because the alternative was measured and broke: a test reaching a
// documentation page spelled the path as `filepath.Join("..", "..", "docs",
// ...)`, which encodes how deep the package happens to sit. Moving the test
// one directory down turned that into a file-not-found in eight subtests, and
// the failure named the page rather than the move. Asking the filesystem where
// the module is costs one walk and cannot go stale.
func RepoRoot(c *qt.C) string {
	c.Helper()
	dir, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		c.Assert(parent, qt.Not(qt.Equals), dir,
			qt.Commentf("no go.mod above the working directory"))
		dir = parent
	}
}

// SQLiteRowCount counts the rows of one table.
//
// Not to be confused with SqliteTableCount, which counts the TABLES of that
// name: one asks whether a table is there, this one asks how much is in it.
// They were two helpers with names one letter apart, in two files, and the
// pair is the reason both are here rather than either being re-derived.
func SQLiteRowCount(c *qt.C, dbPath, table string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), atlasurl.SQLiteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(context.Background(),
		"SELECT count(*) FROM "+table).Scan(&count), qt.IsNil)
	return count
}

// ErrMessageOrEmpty renders an error for comparison against a table row
// without a branch in the test body.
func ErrMessageOrEmpty(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// FreshDevURL returns a throwaway database URL no other invocation has used.
//
// A shared --dev-url is not reset between invocations, so a negative source run
// against a database an earlier positive run populated reports "1 cases,
// 1 passed, 0 failed" and exit 0 -- the fixture would stop discriminating.
func FreshDevURL(c *qt.C) string {
	c.Helper()
	// Slashed because callers interpolate this into an atlas.hcl double-quoted
	// string, where a Windows separator makes \U an invalid escape and the
	// whole project file is refused.
	return "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), "dev.db"))
}

// ApplyWithoutDevURLEnvVar is the variable that restores planning a
// non-database desired state without a dev database.
//
// The name is spelled here rather than imported because the constant it
// mirrors is unexported in package atlas, and exporting it would widen that
// command's surface for a test -- a second copy this repository had already
// accepted before the split, in the external test package this helper came
// from.
//
// It needs no tripwire, because every caller is one. AllowSchemaApplyWithoutDevURL
// sets this variable and the test then plans without a dev database; a name
// that stopped matching would leave the gate armed and the test would fail at
// `--dev-url cannot be empty`, naming the flag rather than the drift but
// failing all the same.
const ApplyWithoutDevURLEnvVar = "PTAH_ATLAS_APPLY_WITHOUT_DEV_URL"

// AllowSchemaApplyWithoutDevURL keeps a test on the pre-stokaro/ptah#940 apply
// path, where `schema apply --to file://...` planned without a dev database.
//
// It marks a test that predates the `--dev-url cannot be empty` gate and is not
// about the dev database: the subject is a format template, a lock note, a
// scope selector, or an atlas.hcl env. The gate itself is covered by
// TestSchemaApplyRequiresDevURLForFileSource and its neighbors, which run
// without this.
func AllowSchemaApplyWithoutDevURL(t *testing.T) {
	t.Helper()
	t.Setenv(ApplyWithoutDevURLEnvVar, "1")
}

// SchemaApplyLockUnsupportedNote is the line `schema apply` writes when the
// target dialect has no lock to take.
const SchemaApplyLockUnsupportedNote = `note: schema apply locking is not supported for dialect "sqlite"; ` +
	`--lock-timeout is ignored and the apply proceeds without a database lock`

func CompatTableNames(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, table.Name)
	}
	return names
}

func AssertPathPresent(c *qt.C, path string) {
	c.Helper()
	_, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
}

func AssertPathAbsent(c *qt.C, path string) {
	c.Helper()
	_, err := os.Stat(path)
	c.Assert(os.IsNotExist(err), qt.IsTrue, qt.Commentf("expected %s to be absent, stat error was %v", path, err))
}

func AssertDirEmpty(c *qt.C, dir string) {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

func AssertSQLiteTableCount(c *qt.C, dbPath, table string, want int) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	err = conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, want)
}

func AssertOneMigrationNamed(c *qt.C, dir, pattern string) {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
}

func WriteHashedAtlasDir(c *qt.C, dir, name, body string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	sum, err := atlascompat.ComputeSum(os.DirFS(dir), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
}

func WriteAtlasApplyProjectSum(c *qt.C, dir string) {
	c.Helper()
	sum, err := atlascompat.ComputeSum(os.DirFS(dir), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
}

func WriteAtlasApplyProjectMigration(c *qt.C, dir, name, sql string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
}

// installAppendEditor points $EDITOR at a script that appends a marker line to
// every file it receives, so editor-driven paths stay hermetic and never spawn
// an interactive editor.
func InstallAppendEditor(t *testing.T, marker string) {
	testutils.SkipWithoutPOSIXShell(t)
	t.Helper()
	path := filepath.Join(t.TempDir(), "editor.sh")
	script := "#!/bin/sh\nfor f in \"$@\"; do\n  printf '%s\\n' \"" + marker + "\" >> \"$f\"\ndone\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil { // #nosec G306 -- test editor script must be executable
		t.Fatal(err)
	}
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", path)
}

// regexpQuote escapes a literal error message for qt.ErrorMatches, which
// anchors and compiles its argument as a regular expression.
func RegexpQuote(literal string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`, `.`, `\.`, `+`, `\+`, `*`, `\*`, `?`, `\?`,
		`(`, `\(`, `)`, `\)`, `[`, `\[`, `]`, `\]`, `{`, `\{`, `}`, `\}`,
		`^`, `\^`, `$`, `\$`, `|`, `\|`,
	)
	return replacer.Replace(literal)
}

func ExecuteAtlasTestCommand(cmd *cobra.Command) error {
	executed, err := cmd.ExecuteC()
	return cmdutil.NormalizeCommandError(executed, err, 2)
}

func AvailableChildNames(parent *cobra.Command) []string {
	names := make([]string, 0, len(parent.Commands()))
	for _, child := range parent.Commands() {
		if !child.Hidden {
			names = append(names, child.Name())
		}
	}
	slices.Sort(names)
	return names
}

func SqliteAtlasRevisionVersions(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), atlasurl.SQLiteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(), "SELECT version FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	versions := make([]string, 0)
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

// AcceptedNameCase is one migration name the pinned binary accepts, and the
// value the test that asserts it iterates over.
//
// The fields are exported because the test now lives in another package. That
// is the one cost of the split, and it is a small one: a two-field fixture
// type says nothing by being private.
type AcceptedNameCase struct {
	Name  string
	Given string
}
