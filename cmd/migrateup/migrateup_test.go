package migrateup

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/migratesum"
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
