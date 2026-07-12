package migrateup

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/migratesum"
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
	c.Assert(err, qt.ErrorMatches, "(?s).*ptah.sum verification failed.*")
	c.Assert(err, qt.ErrorMatches, "(?s).*changed: 0000000001_init.up.sql.*",
		qt.Commentf("the drift diagnostic identifies the tampered file"))
}
