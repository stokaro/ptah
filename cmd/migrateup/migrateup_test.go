package migrateup

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

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
// The command uses package-global flag state, so this package keeps a single
// command-level test to avoid re-registering flags.
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

func TestPendingMigrationsForSafetyCheckSkipsOutOfOrderWhenLinearSkip(t *testing.T) {
	c := qt.New(t)

	status := &migrator.MigrationStatus{
		PendingMigrations:    []int64{3, 6},
		OutOfOrderMigrations: []int64{3},
	}

	c.Assert(
		pendingMigrationsForSafetyCheck(status, migrator.ExecOrderLinear),
		qt.DeepEquals,
		[]int64{3, 6},
	)
	c.Assert(
		pendingMigrationsForSafetyCheck(status, migrator.ExecOrderNonLinear),
		qt.DeepEquals,
		[]int64{3, 6},
	)
	c.Assert(
		pendingMigrationsForSafetyCheck(status, migrator.ExecOrderLinearSkip),
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
