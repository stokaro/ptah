package migrateup

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/cliobs"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
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
	resetMigrateUpCommandForTest(c.TB, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c.TB, cmd) })
	c.Assert(cmd.Flag(migrationLockTimeoutFlag), qt.IsNotNil)
	c.Assert(cmd.Flag(txModeFlag), qt.IsNotNil)
	c.Assert(cmd.Flag(cliobs.LogFormatFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(cliobs.LogLevelFlagName), qt.IsNotNil)
	c.Assert(cmd.Flag(cliobs.MetricsAddrFlagName), qt.IsNotNil)

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

	findings, err := lintPendingDestructive(fsys, []int64{2}, "postgres", "")
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

	findings, err := lintPendingDestructive(fsys, []int64{1, 2}, "postgres", "")
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0,
		qt.Commentf("DS103 is warning-grade and DS102 is disabled by config"))

	findings, err = lintPendingDestructive(fsys, []int64{1, 2, 3}, "postgres", "")
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS101")
}

func TestMigrateUpCommandPreflightHookAbortPreventsMigration(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	writeMigrateUpFile(c.TB, dir, "0000000001_create_guarded.up.sql", "CREATE TABLE guarded (id INTEGER PRIMARY KEY);\n")
	writeMigrateUpFile(c.TB, dir, "0000000001_create_guarded.down.sql", "DROP TABLE guarded;\n")

	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c.TB, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c.TB, cmd) })
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
	writeMigrateUpFile(c.TB, dir, "0000000001_create_config_guarded.up.sql", "CREATE TABLE config_guarded (id INTEGER PRIMARY KEY);\n")
	writeMigrateUpFile(c.TB, dir, "0000000001_create_config_guarded.down.sql", "DROP TABLE config_guarded;\n")
	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	config := fmt.Appendf(nil, `url: %s
migration:
  dir: %s
  pre_up_hook: "echo config backup refused; exit 9"
`, dbURL, dir)
	c.Assert(os.WriteFile(configPath, config, 0o600), qt.IsNil)

	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c.TB, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c.TB, cmd) })
	cmd.SetArgs([]string{"--config", configPath})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "(?s).*up pre-flight custom command hook failed: exit status 9\nconfig backup refused")
}

func TestMigrateUpCommandReadsTxModeFromConfig(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	tableName := "tx_mode_config_keeps_partial_body"
	writeMigrateUpFile(c.TB, dir, "0000000001_partial.up.sql", fmt.Sprintf(`CREATE TABLE %s (id INTEGER PRIMARY KEY);
INSERT INTO missing_tx_mode_config_table (id) VALUES (1);
`, tableName))
	writeMigrateUpFile(c.TB, dir, "0000000001_partial.down.sql", fmt.Sprintf("DROP TABLE %s;\n", tableName))
	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	config := fmt.Appendf(nil, `url: %s
migration:
  dir: %s
  tx_mode: none
`, dbURL, dir)
	c.Assert(os.WriteFile(configPath, config, 0o600), qt.IsNil)

	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c.TB, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c.TB, cmd) })
	cmd.SetArgs([]string{"--config", configPath})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(sqliteMigrateUpTableExists(c.TB, dbURL, tableName), qt.IsTrue)
}

func TestMigrateUpCommandTxModeFlagOverridesConfig(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	tableName := "tx_mode_flag_rolls_back_body"
	writeMigrateUpFile(c.TB, dir, "0000000001_partial.up.sql", fmt.Sprintf(`CREATE TABLE %s (id INTEGER PRIMARY KEY);
INSERT INTO missing_tx_mode_flag_table (id) VALUES (1);
`, tableName))
	writeMigrateUpFile(c.TB, dir, "0000000001_partial.down.sql", fmt.Sprintf("DROP TABLE %s;\n", tableName))
	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	configPath := filepath.Join(t.TempDir(), "ptah.yaml")
	config := fmt.Appendf(nil, `url: %s
migration:
  dir: %s
  tx_mode: none
`, dbURL, dir)
	c.Assert(os.WriteFile(configPath, config, 0o600), qt.IsNil)

	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c.TB, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c.TB, cmd) })
	cmd.SetArgs([]string{"--config", configPath, "--tx-mode", "file"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(sqliteMigrateUpTableExists(c.TB, dbURL, tableName), qt.IsFalse)
}

func TestMigrateUpCommandDryRunSkipsPreflightSideEffects(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	writeMigrateUpFile(c.TB, dir, "0000000001_create_dry_guarded.up.sql", "CREATE TABLE dry_guarded (id INTEGER PRIMARY KEY);\n")
	writeMigrateUpFile(c.TB, dir, "0000000001_create_dry_guarded.down.sql", "DROP TABLE dry_guarded;\n")
	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()

	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c.TB, cmd)
	t.Cleanup(func() { resetMigrateUpCommandForTest(c.TB, cmd) })
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

func TestDatabaseURLPasswordsForTest(t *testing.T) {
	c := qt.New(t)

	c.Assert(databaseURLPasswordsForTest("postgres://user:secret@example.test/db"), qt.DeepEquals, []string{"secret"})
	c.Assert(databaseURLPasswordsForTest("postgres://user:@example.test/db"), qt.IsNil)
	c.Assert(databaseURLPasswordsForTest("postgres://user@example.test/db"), qt.IsNil)
	c.Assert(databaseURLPasswordsForTest(":"), qt.IsNil)
}

func writeMigrateUpFile(tb testing.TB, dir, name, content string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func sqliteMigrateUpTableExists(tb testing.TB, dbURL string, tableName string) bool {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	err = conn.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		tableName,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count > 0
}

func databaseURLPasswordsForTest(dbURL string) []string {
	parsed, err := url.Parse(dbURL)
	if err != nil || parsed.User == nil {
		return nil
	}
	password, ok := parsed.User.Password()
	if !ok || password == "" {
		return nil
	}
	return []string{password}
}

func resetMigrateUpCommandForTest(tb testing.TB, cmd interface{ Flag(string) *pflag.Flag }) {
	c := qt.New(tb)
	c.Helper()
	setMigrateUpFlagForTest(c.TB, cmd, dbURLFlag, "")
	setMigrateUpFlagForTest(c.TB, cmd, migrationsFlag, "")
	setMigrateUpFlagForTest(c.TB, cmd, dryRunFlag, "false")
	setMigrateUpFlagForTest(c.TB, cmd, verboseFlag, "false")
	setMigrateUpFlagForTest(c.TB, cmd, verifySumFlag, "false")
	setMigrateUpFlagForTest(c.TB, cmd, dirFormatFlag, string(migrator.MigrationDirFormatAuto))
	setMigrateUpFlagForTest(c.TB, cmd, atlasEnvFlag, "")
	setMigrateUpFlagForTest(c.TB, cmd, execOrderFlag, string(migrator.ExecOrderLinear))
	setMigrateUpFlagForTest(c.TB, cmd, txModeFlag, string(migrator.MigrationTxModeFile))
	setMigrateUpFlagForTest(c.TB, cmd, migrationLockTimeoutFlag, "")
	setMigrateUpFlagForTest(c.TB, cmd, lockTimeoutFlag, "")
	setMigrateUpFlagForTest(c.TB, cmd, statementTimeoutFlag, "")
	setMigrateUpFlagForTest(c.TB, cmd, allowDestructiveFlag, "false")
	setMigrateUpFlagForTest(c.TB, cmd, preUpHookFlag, "")
	setMigrateUpFlagForTest(c.TB, cmd, pgDumpToFlag, "")
	setMigrateUpFlagForTest(c.TB, cmd, mySQLDumpToFlag, "")
	setMigrateUpFlagForTest(c.TB, cmd, webhookFlag, "")
	setMigrateUpFlagForTest(c.TB, cmd, cliobs.LogFormatFlagName, "text")
	setMigrateUpFlagForTest(c.TB, cmd, cliobs.LogLevelFlagName, "info")
	setMigrateUpFlagForTest(c.TB, cmd, cliobs.MetricsAddrFlagName, "")
	setMigrateUpFlagForTest(c.TB, cmd, dbcli.ConfigFlagName, "")
	setMigrateUpFlagForTest(c.TB, cmd, dbcli.EnvFlagName, "")
	setMigrateUpFlagForTest(c.TB, cmd, dbcli.MigrationsSchemaFlagName, "")
	setMigrateUpFlagForTest(c.TB, cmd, dbcli.MigrationsTableFlagName, "")
	setMigrateUpFlagForTest(c.TB, cmd, dbcli.RevisionTableFormatFlagName, string(migrator.RevisionTableFormatPtah))
	setMigrateUpFlagForTest(c.TB, cmd, dbcli.ConnectTimeoutFlagName, dbcli.DefaultConnectTimeout.String())
}

func setMigrateUpFlagForTest(tb testing.TB, cmd interface{ Flag(string) *pflag.Flag }, name, value string) {
	c := qt.New(tb)
	c.Helper()
	flag := cmd.Flag(name)
	c.Assert(flag, qt.IsNotNil, qt.Commentf("flag %s", name))
	c.Assert(flag.Value.Set(value), qt.IsNil)
	flag.Changed = false
}
