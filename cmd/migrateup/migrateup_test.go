package migrateup

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/lint"
	"github.com/stokaro/ptah/migration/migrator"
)

// TestMigrateUp_VerifySumAbortsOnDriftBeforeConnecting exercises the
// --verify-sum gate: on a drifted migrations directory the command must fail
// on the integrity check before ever touching the database, so a bogus,
// unreachable --db-url is never dialed.
//
// The command uses package-global flag state, so command-level tests reset the
// relevant flag values before and after execution.
func TestMigrateUp_VerifySumAbortsOnDriftBeforeConnecting(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	write := func(name, content string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	write("0000000001_init.up.sql", "CREATE TABLE t (id INT);\n")
	write("0000000001_init.down.sql", "DROP TABLE t;\n")
	_, err := migratesum.Write(dir)
	c.Assert(err, qt.IsNil)

	// Tamper with an already-hashed migration so the directory drifts.
	write("0000000001_init.up.sql", "CREATE TABLE t (id BIGINT);\n")

	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c, cmd) })
	c.Assert(cmd.Flag(migrationLockTimeoutFlag), qt.IsNotNil)

	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"--db-url", "mysql://u@tcp(127.0.0.1:1)/db", // unreachable; must never be dialed
		"--migrations-dir", dir,
		"--verify-sum",
	})

	err = cmd.Execute()
	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorMatches, "(?s).*migration sum verification failed.*")
	c.Assert(err, qt.ErrorMatches, "(?s).*changed: 0000000001_init.up.sql.*",
		qt.Commentf("the drift diagnostic identifies the tampered file"))
}

func TestLintPendingDestructiveScansOnlyPendingVersions(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"0000000001_old.up.sql":   &fstest.MapFile{Data: []byte("DROP TABLE old_data;\n")},
		"0000000001_old.down.sql": &fstest.MapFile{Data: []byte("CREATE TABLE old_data (id INT);\n")},
		"0000000002_next.up.sql": &fstest.MapFile{Data: []byte(`ALTER TABLE users DROP COLUMN legacy;
DROP TYPE old_status;
DROP POLICY tenant_isolation ON accounts;
TRUNCATE TABLE audit_log;
ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;
`)},
		"0000000002_next.down.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE users ADD COLUMN legacy TEXT;\n"),
		},
	}

	findings, err := lintPendingDestructive(fsys, []int64{2}, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 5)
	c.Assert([]string{findings[0].Rule, findings[1].Rule, findings[2].Rule, findings[3].Rule, findings[4].Rule}, qt.DeepEquals, []string{"DS102", "DS107", "DS107", "DS108", "DS109"})
	c.Assert(findings[0].File, qt.Equals, "0000000002_next.up.sql")
}

func TestLintPendingDestructiveHonorsLintConfigSeverityAndDisable(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		lint.ConfigFileName: &fstest.MapFile{Data: []byte(`disabled-rules:
  - DS102
rules:
  DS103:
    severity: warning
`)},
		"0000000001_change_type.up.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE users ALTER COLUMN email TYPE VARCHAR(512);\n"),
		},
		"0000000001_change_type.down.sql": &fstest.MapFile{Data: []byte("-- restore\n")},
		"0000000002_drop_column.up.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE users DROP COLUMN legacy;\n"),
		},
		"0000000002_drop_column.down.sql": &fstest.MapFile{Data: []byte("-- restore\n")},
		"0000000003_drop_table.up.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE audit_log;\n"),
		},
		"0000000003_drop_table.down.sql": &fstest.MapFile{Data: []byte("-- restore\n")},
	}

	findings, err := lintPendingDestructive(fsys, []int64{1, 2}, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0,
		qt.Commentf("DS103 is warning-grade and DS102 is disabled by config"))

	findings, err = lintPendingDestructive(fsys, []int64{1, 2, 3}, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS101")
}

func TestMigrateUpCommandHonorsLintConfigSeverityWithPostgres(t *testing.T) {
	dbURL := postgresTestURL()
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN, POSTGRES_URL, or TEST_DATABASE_URL is not set")
	}

	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	widenTable := "ptah_migrateup_widen_" + suffix
	dropTable := "ptah_migrateup_drop_" + suffix
	widenRevisions := "ptah_migrateup_widen_revisions_" + suffix
	dropRevisions := "ptah_migrateup_drop_revisions_" + suffix
	defer cleanupPostgresObjects(t, conn, widenTable, dropTable, widenRevisions, dropRevisions)

	widenDir := t.TempDir()
	writeMigrateUpFile(c, widenDir, lint.ConfigFileName, `rules:
  DS103:
    severity: warning
`)
	writeMigrateUpFile(c, widenDir, "0000000001_init.up.sql", fmt.Sprintf("CREATE TABLE %s (id SERIAL PRIMARY KEY, email VARCHAR(255));\n", widenTable))
	writeMigrateUpFile(c, widenDir, "0000000001_init.down.sql", fmt.Sprintf("DROP TABLE %s;\n", widenTable))
	writeMigrateUpFile(c, widenDir, "0000000002_widen_email.up.sql", fmt.Sprintf("ALTER TABLE %s ALTER COLUMN email TYPE VARCHAR(512);\n", widenTable))
	writeMigrateUpFile(c, widenDir, "0000000002_widen_email.down.sql", fmt.Sprintf("ALTER TABLE %s ALTER COLUMN email TYPE VARCHAR(255);\n", widenTable))

	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c, cmd) })
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", widenDir,
		"--migrations-table", widenRevisions,
	})

	err = cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))

	var maxLength int
	err = conn.QueryRowContext(ctx, `
		SELECT character_maximum_length
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'email'
	`, widenTable).Scan(&maxLength)
	c.Assert(err, qt.IsNil)
	c.Assert(maxLength, qt.Equals, 512)

	dropDir := t.TempDir()
	writeMigrateUpFile(c, dropDir, lint.ConfigFileName, `rules:
  DS103:
    severity: warning
`)
	writeMigrateUpFile(c, dropDir, "0000000001_init.up.sql", fmt.Sprintf("CREATE TABLE %s (id SERIAL PRIMARY KEY, email VARCHAR(255));\n", dropTable))
	writeMigrateUpFile(c, dropDir, "0000000001_init.down.sql", fmt.Sprintf("DROP TABLE %s;\n", dropTable))
	writeMigrateUpFile(c, dropDir, "0000000002_widen_email.up.sql", fmt.Sprintf("ALTER TABLE %s ALTER COLUMN email TYPE VARCHAR(512);\n", dropTable))
	writeMigrateUpFile(c, dropDir, "0000000002_widen_email.down.sql", fmt.Sprintf("ALTER TABLE %s ALTER COLUMN email TYPE VARCHAR(255);\n", dropTable))
	writeMigrateUpFile(c, dropDir, "0000000003_drop_table.up.sql", fmt.Sprintf("DROP TABLE %s;\n", dropTable))
	writeMigrateUpFile(c, dropDir, "0000000003_drop_table.down.sql", fmt.Sprintf("CREATE TABLE %s (id SERIAL PRIMARY KEY, email VARCHAR(512));\n", dropTable))

	cmd = NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c, cmd)
	stdout.Reset()
	stderr.Reset()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", dropDir,
		"--migrations-table", dropRevisions,
	})

	err = cmd.Execute()
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "pending migrations contain destructive statements")
	c.Assert(err.Error(), qt.Contains, "DS101")

	var dropExists bool
	err = conn.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", "public."+dropTable).Scan(&dropExists)
	c.Assert(err, qt.IsNil)
	c.Assert(dropExists, qt.IsFalse)
}

func TestMigrateUpCommandPreflightHookAbortPreventsMigration(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	writeMigrateUpFile(c, dir, "0000000001_create_guarded.up.sql", "CREATE TABLE guarded (id INTEGER PRIMARY KEY);\n")
	writeMigrateUpFile(c, dir, "0000000001_create_guarded.down.sql", "DROP TABLE guarded;\n")

	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c, cmd) })
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
		"--pre-up-hook", "echo backup refused; exit 7",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "(?s).*up pre-flight custom command hook failed: exit status 7\nbackup refused")

	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	err = conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'guarded'").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestMigrateUpCommandReadsPreflightHookFromConfig(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	writeMigrateUpFile(c, dir, "0000000001_create_config_guarded.up.sql", "CREATE TABLE config_guarded (id INTEGER PRIMARY KEY);\n")
	writeMigrateUpFile(c, dir, "0000000001_create_config_guarded.down.sql", "DROP TABLE config_guarded;\n")
	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	config := fmt.Appendf(nil, `url: %s
migration:
  dir: %s
  pre_up_hook: "echo config backup refused; exit 9"
`, dbURL, dir)
	c.Assert(os.WriteFile(configPath, config, 0o600), qt.IsNil)

	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c, cmd) })
	cmd.SetArgs([]string{"--config", configPath})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "(?s).*up pre-flight custom command hook failed: exit status 9\nconfig backup refused")
}

func TestMigrateUpCommandPgDumpHookWritesArtifact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake pg_dump shell script requires a POSIX shell")
	}
	c := qt.New(t)
	ctx := context.Background()
	dbURL := postgresTestURL()
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN, POSTGRES_URL, or TEST_DATABASE_URL is not set")
	}
	tableName := fmt.Sprintf("ptah_preflight_pg_dump_%d", time.Now().UnixNano())

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	cleanupPostgresObjects(t, conn, tableName)
	defer cleanupPostgresObjects(t, conn, tableName)

	dir := t.TempDir()
	writeMigrateUpFile(c, dir, "0000000001_create_dump_guarded.up.sql", fmt.Sprintf("CREATE TABLE %s (id BIGINT);\n", tableName))
	writeMigrateUpFile(c, dir, "0000000001_create_dump_guarded.down.sql", fmt.Sprintf("DROP TABLE %s;\n", tableName))

	argsLog := filepath.Join(t.TempDir(), "pg_dump_args.log")
	fakeBin := filepath.Join(t.TempDir(), "pg_dump")
	fakeScript := fmt.Appendf(nil, `#!/bin/sh
out=""
: > %[1]q
while [ "$#" -gt 0 ]; do
  printf '%%s\n' "$1" >> %[1]q
  if [ "$1" = "--file" ]; then
    shift
    out="$1"
    printf '%%s\n' "$1" >> %[1]q
  fi
  shift
done
if [ -z "$out" ]; then
  echo "missing --file" >&2
  exit 64
fi
printf 'fake custom dump\n' > "$out"
`, argsLog)
	c.Assert(os.WriteFile(fakeBin, fakeScript, 0o600), qt.IsNil)
	c.Assert(os.Chmod(fakeBin, 0o700), qt.IsNil)
	t.Setenv("PATH", filepath.Dir(fakeBin)+string(os.PathListSeparator)+os.Getenv("PATH"))

	dumpDir := t.TempDir()
	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c, cmd) })
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
		"--pg-dump-to", dumpDir,
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	matches, err := filepath.Glob(filepath.Join(dumpDir, "ptah_pre_v0_to_v1_*.dump"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	dumpData, err := os.ReadFile(matches[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(dumpData), qt.Equals, "fake custom dump\n")

	argsData, err := os.ReadFile(argsLog)
	c.Assert(err, qt.IsNil)
	c.Assert(string(argsData), qt.Contains, "--format=custom\n")
	c.Assert(string(argsData), qt.Contains, "--file\n"+matches[0]+"\n")
	if password := databaseURLPasswordForTest(dbURL); password != "" {
		c.Assert(string(argsData), qt.Not(qt.Contains), password)
	}
}

func TestMigrateUpCommandDryRunSkipsPreflightSideEffects(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	writeMigrateUpFile(c, dir, "0000000001_create_dry_guarded.up.sql", "CREATE TABLE dry_guarded (id INTEGER PRIMARY KEY);\n")
	writeMigrateUpFile(c, dir, "0000000001_create_dry_guarded.down.sql", "DROP TABLE dry_guarded;\n")
	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()

	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c, cmd) })
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
		"--dry-run",
		"--pre-up-hook", "echo should not run; exit 97",
		"--pg-dump-to", filepath.Join(t.TempDir(), "pg"),
		"--mysqldump-to", filepath.Join(t.TempDir(), "mysql"),
		"--webhook", "https://ops.example/hooks/ptah",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
}

func TestPendingMigrationsForRunSkipsOutOfOrderWhenLinearSkip(t *testing.T) {
	c := qt.New(t)

	status := &migrator.MigrationStatus{
		PendingMigrations:    []int64{3, 6},
		OutOfOrderMigrations: []int64{3},
	}

	c.Assert(
		pendingMigrationsForRun(status, migrator.ExecOrderLinear),
		qt.DeepEquals,
		[]int64{3, 6},
	)
	c.Assert(
		pendingMigrationsForRun(status, migrator.ExecOrderNonLinear),
		qt.DeepEquals,
		[]int64{3, 6},
	)
	c.Assert(
		pendingMigrationsForRun(status, migrator.ExecOrderLinearSkip),
		qt.DeepEquals,
		[]int64{6},
	)
}

func postgresTestURL() string {
	for _, name := range []string{"POSTGRES_TEST_DSN", "POSTGRES_URL", "TEST_DATABASE_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

func writeMigrateUpFile(c *qt.C, dir, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func databaseURLPasswordForTest(dbURL string) string {
	parsed, err := url.Parse(dbURL)
	if err != nil || parsed.User == nil {
		return ""
	}
	password, ok := parsed.User.Password()
	if !ok {
		return ""
	}
	return password
}

func resetMigrateUpCommandForTest(c *qt.C, cmd interface{ Flag(string) *pflag.Flag }) {
	c.Helper()
	setMigrateUpFlagForTest(c, cmd, dbURLFlag, "")
	setMigrateUpFlagForTest(c, cmd, migrationsFlag, "")
	setMigrateUpFlagForTest(c, cmd, dryRunFlag, "false")
	setMigrateUpFlagForTest(c, cmd, verboseFlag, "false")
	setMigrateUpFlagForTest(c, cmd, verifySumFlag, "false")
	setMigrateUpFlagForTest(c, cmd, dirFormatFlag, string(migrator.MigrationDirFormatAuto))
	setMigrateUpFlagForTest(c, cmd, atlasEnvFlag, "")
	setMigrateUpFlagForTest(c, cmd, execOrderFlag, string(migrator.ExecOrderLinear))
	setMigrateUpFlagForTest(c, cmd, migrationLockTimeoutFlag, "")
	setMigrateUpFlagForTest(c, cmd, lockTimeoutFlag, "")
	setMigrateUpFlagForTest(c, cmd, statementTimeoutFlag, "")
	setMigrateUpFlagForTest(c, cmd, allowDestructiveFlag, "false")
	setMigrateUpFlagForTest(c, cmd, preUpHookFlag, "")
	setMigrateUpFlagForTest(c, cmd, pgDumpToFlag, "")
	setMigrateUpFlagForTest(c, cmd, mySQLDumpToFlag, "")
	setMigrateUpFlagForTest(c, cmd, webhookFlag, "")
	setMigrateUpFlagForTest(c, cmd, dbcli.ConfigFlagName, "")
	setMigrateUpFlagForTest(c, cmd, dbcli.EnvFlagName, "")
	setMigrateUpFlagForTest(c, cmd, dbcli.MigrationsSchemaFlagName, "")
	setMigrateUpFlagForTest(c, cmd, dbcli.MigrationsTableFlagName, "")
	setMigrateUpFlagForTest(c, cmd, dbcli.RevisionTableFormatFlagName, string(migrator.RevisionTableFormatPtah))
	setMigrateUpFlagForTest(c, cmd, dbcli.ConnectTimeoutFlagName, dbcli.DefaultConnectTimeout.String())
}

func setMigrateUpFlagForTest(c *qt.C, cmd interface{ Flag(string) *pflag.Flag }, name, value string) {
	c.Helper()
	flag := cmd.Flag(name)
	c.Assert(flag, qt.IsNotNil, qt.Commentf("flag %s", name))
	c.Assert(flag.Value.Set(value), qt.IsNil)
	flag.Changed = false
}

func cleanupPostgresObjects(t *testing.T, conn *dbschema.DatabaseConnection, names ...string) {
	t.Helper()
	ctx := context.Background()
	for _, name := range names {
		if !safePostgresIdentifier(name) {
			t.Fatalf("unsafe test identifier %q", name)
		}
		_, _ = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", name))
	}
}

func safePostgresIdentifier(name string) bool {
	return name != "" && strings.Trim(name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") == ""
}
