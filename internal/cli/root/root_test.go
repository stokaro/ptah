package root_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/root"
)

func TestNewRootCommand_UsesPtahBranding(t *testing.T) {
	c := qt.New(t)

	cmd := root.NewRootCommand()

	c.Assert(cmd.Use, qt.Equals, "ptah")
	c.Assert(cmd.Short, qt.Contains, "Ptah")
	c.Assert(cmd.Version, qt.Not(qt.Equals), "")
}

func TestNewRootCommand_HelpAdvertisesPtahEnvVars(t *testing.T) {
	c := qt.New(t)
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrations", "up", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Usage:")
	c.Assert(out.String(), qt.Contains, "[env: PTAH_DB_URL]")
	c.Assert(out.String(), qt.Not(qt.Contains), "PACKAGE_"+"MIGRATOR")
}

func TestNewRootCommand_VersionSubcommandPrintsBuildInfo(t *testing.T) {
	c := qt.New(t)
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"version"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Version: ")
	c.Assert(out.String(), qt.Contains, "Commit: ")
	c.Assert(out.String(), qt.Contains, "Date: ")
	c.Assert(out.String(), qt.Contains, "Go: ")
	c.Assert(out.String(), qt.Contains, "Platform: ")
}

func TestNewRootCommand_SchemaExportSubcommandIsRegistered(t *testing.T) {
	c := qt.New(t)
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "export", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Export a Ptah schema to another format")
	c.Assert(out.String(), qt.Contains, "--cleanup-go-annotations")
}

func TestNewRootCommand_NativeCommandTreeIsRegistered(t *testing.T) {
	c := qt.New(t)

	cmd := root.NewRootCommand()
	for _, path := range [][]string{
		{"schema", "render"},
		{"schema", "compare"},
		{"schema", "drift"},
		{"introspect"},
		{"db", "read"},
		{"db", "drop-all"},
		{"db", "capabilities"},
		{"migrations", "plan"},
		{"migrations", "generate"},
		{"migrations", "create"},
		{"migrations", "up"},
		{"migrations", "down"},
		{"migrations", "status"},
		{"migrations", "baseline"},
		{"migrations", "repair"},
		{"migrations", "hash"},
		{"migrations", "validate"},
		{"migrations", "lint"},
		{"oci", "referrers"},
		{"viz"},
	} {
		found, _, err := cmd.Find(path)
		c.Assert(err, qt.IsNil)
		c.Assert(found.CommandPath(), qt.Equals, "ptah "+strings.Join(path, " "))
	}
}

// TestNewRootCommand_VersionSpellingsPrintTheSameBlock replaces the former
// TestNewRootCommand_VersionFlagWorks, which asserted the substring
// "ptah version" -- cobra's built-in template, and the second of the two
// formats stokaro/ptah#1064 reported. The flag spellings now render the same
// block as the `version` subcommand, so this asserts mutual equality instead of
// a literal: which format the flags produce is the whole point, and pinning the
// old one is what let the two drift apart in the first place.
func TestNewRootCommand_VersionSpellingsPrintTheSameBlock(t *testing.T) {
	c := qt.New(t)

	want := runRootCommandOutput(c, "version")
	c.Assert(want, qt.Contains, "Version: ")
	c.Assert(want, qt.Not(qt.Contains), "ptah version")

	tests := []struct {
		name string
		args []string
	}{
		{name: "long flag", args: []string{"--version"}},
		{name: "short flag", args: []string{"-v"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(runRootCommandOutput(c, tt.args...), qt.Equals, want)
		})
	}
}

func runRootCommandOutput(c *qt.C, args ...string) string {
	c.Helper()
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)

	c.Assert(cmd.Execute(), qt.IsNil)
	return out.String()
}

func TestNewRootCommand_PTAHDBURLFeedsCommandFlag(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DB_URL", "postgres://user:pass@127.0.0.1:1/db?sslmode=disable")
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrations", "status", "--migrations-dir", filepath.ToSlash(t.TempDir())})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "database URL is required")
	c.Assert(err.Error(), qt.Contains, "error connecting to database")
}

func TestNewRootCommand_MalformedPTAHDryRunAppliesNothing(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "notabool")
	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_users.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_users.down.sql"),
		[]byte("DROP TABLE users;\n"), 0o600), qt.IsNil)
	dbPath := filepath.Join(t.TempDir(), "native-env.db")
	dbURL := atlasurl.SQLiteURLFromPath(dbPath)

	_, _, err := executeRootCommand(
		"migrations", "up",
		"--db-url", dbURL,
		"--migrations-dir", migrationsDir,
	)

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "notabool" for PTAH_DRY_RUN`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}

// TestNewRootCommand_PTAHLockTimeoutAppliesOnUnlockedDialectWithNote pins the
// half of the refusal that must not fire. `ptah migrations up` and `ptah
// migrations down` bind PTAH_LOCK_TIMEOUT to a --lock-timeout of their own,
// where it bounds each migration's statement lock, so an operator who exports
// the variable for a versioned workflow has configured nothing about this
// apply. Refusing here would tell them to remove a setting another command
// needs. The note is how the dropped request stays visible.
func TestNewRootCommand_PTAHLockTimeoutAppliesOnUnlockedDialectWithNote(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_LOCK_TIMEOUT", "5s")
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath,
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	dbPath := filepath.Join(dir, "target.db")

	stdout, stderr, err := executeRootCommand(
		"schema", "apply",
		"--db-url", atlasurl.SQLiteURLFromPath(dbPath),
		"--schema-file", schemaPath,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "Schema apply completed successfully.")
	c.Assert(stderr, qt.Contains,
		`note: PTAH_LOCK_TIMEOUT is ignored here: dialect "sqlite" has no schema apply lock, `+
			`so this apply runs unlocked. The variable also sets --lock-timeout on `+
			`"ptah migrations up", so only a typed --lock-timeout refuses`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.IsNil)
}

// TestNewRootCommand_PTAHLockTimeoutSaysNothingOnALockingDialect keeps the note
// off a target that does take the lock. The address has no server, so the run
// reaches the connection and fails there, which is the part worth asserting: a
// note printed for every exported variable would say the lock was dropped on a
// database that would have taken it.
func TestNewRootCommand_PTAHLockTimeoutSaysNothingOnALockingDialect(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_LOCK_TIMEOUT", "5s")
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath,
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	_, stderr, err := executeRootCommand(
		"schema", "apply",
		"--db-url", "postgres://ptah@127.0.0.1:1/db?sslmode=disable",
		"--schema-file", schemaPath,
		"--connect-timeout", "2s",
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches, `(?s)connect to --db-url:.*`)
	c.Assert(stderr, qt.Not(qt.Contains), "PTAH_LOCK_TIMEOUT is ignored")
}

// TestNewRootCommand_PTAHLockTimeoutKeepsMigrationsUpWorking is the second half
// of the same measurement, on the command the variable does belong to: the
// exported value configures a versioned run rather than refusing it.
func TestNewRootCommand_PTAHLockTimeoutKeepsMigrationsUpWorking(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_LOCK_TIMEOUT", "5s")
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)
	dbPath := filepath.Join(dir, "versioned.db")

	stdout, stderr, err := executeRootCommand(
		"migrations", "up",
		"--db-url", atlasurl.SQLiteURLFromPath(dbPath),
		"--migrations-dir", migrationsDir,
		"--dry-run",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "DRY RUN MODE")
	c.Assert(stderr, qt.Not(qt.Contains), "schema apply lock")
}

// TestNewRootCommand_TypedLockTimeoutRefusesUnlockedDialect is the refusal seen
// through the tree that installs the environment binding: a flag the operator
// typed still refuses there, so scoping the rule to the command line did not
// scope it away.
func TestNewRootCommand_TypedLockTimeoutRefusesUnlockedDialect(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_LOCK_TIMEOUT", "")
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath,
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	dbPath := filepath.Join(dir, "target.db")

	_, _, err := executeRootCommand(
		"schema", "apply",
		"--db-url", atlasurl.SQLiteURLFromPath(dbPath),
		"--schema-file", schemaPath,
		"--lock-timeout", "5s",
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches,
		`--lock-timeout requested a schema apply lock, and dialect "sqlite" has none: `+
			`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. `+
			`Remove --lock-timeout to apply without a lock`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}

// TestNewRootCommand_NoLockTimeoutAppliesSilentlyOnUnlockedDialect is the
// control for both: with neither spelling set, the apply runs and says nothing
// about a lock. The variable is cleared because a machine that exports it would
// otherwise measure its own environment.
func TestNewRootCommand_NoLockTimeoutAppliesSilentlyOnUnlockedDialect(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_LOCK_TIMEOUT", "")
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath,
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	dbPath := filepath.Join(dir, "target.db")

	stdout, stderr, err := executeRootCommand(
		"schema", "apply",
		"--db-url", atlasurl.SQLiteURLFromPath(dbPath),
		"--schema-file", schemaPath,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "Schema apply completed successfully.")
	c.Assert(stderr, qt.Not(qt.Contains), "schema apply lock")
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.IsNil)
}

func TestNewRootCommand_PTAHAutoApproveDoesNotBypassDropAllConfirmation(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_AUTO_APPROVE", "true")

	dbURL := atlasurl.SQLiteURLFromPath(filepath.Join(t.TempDir(), "ptah.db"))
	out, _, err := captureRootStdIO(c, "no\n", "db", "drop-all", "--db-url", dbURL)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Type 'DELETE EVERYTHING'")
	c.Assert(out, qt.Contains, "Operation canceled.")
	c.Assert(out, qt.Not(qt.Contains), "Auto-approval enabled")
}

func executeRootCommand(args ...string) (stdout, stderr string, err error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func captureRootStdIO(c *qt.C, input string, args ...string) (stdout, stderr string, err error) {
	c.Helper()

	oldStdin := os.Stdin
	oldStdout := os.Stdout
	defer func() {
		os.Stdin = oldStdin
		os.Stdout = oldStdout
	}()

	inR, inW, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(inR.Close(), qt.IsNil) }()
	_, err = inW.WriteString(input)
	c.Assert(err, qt.IsNil)
	c.Assert(inW.Close(), qt.IsNil)

	outR, outW, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(outR.Close(), qt.IsNil) }()

	os.Stdin = inR
	os.Stdout = outW
	stdout, stderr, err = executeRootCommand(args...)
	c.Assert(outW.Close(), qt.IsNil)

	outBytes, readErr := io.ReadAll(outR)
	c.Assert(readErr, qt.IsNil)
	return stdout + string(outBytes), stderr, err
}
