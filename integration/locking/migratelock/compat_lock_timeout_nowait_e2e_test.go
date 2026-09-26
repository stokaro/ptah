//go:build integration

package migratelock_test

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// `ptah-compat migrate apply` reads `--lock-timeout 0` and a negative value as
// the pinned community binary v1.3.0 does: the migration lock is tried once,
// and a lock another session holds refuses the run at once. Measured there on
// PostgreSQL 18 on 2026-09-26 against a held lock (stokaro/ptah#3689): 0 and
// -1s fail at once, 2s fails after two seconds. Each row holds Ptah's
// migration lock from a second session, runs the verb, and reads the target
// back: a refused run creates no table.

// compatLockHolderName is the lock `ptah-compat migrate apply` takes when no
// --lock-name is given.
const compatLockHolderName = "ptah_migrate"

func TestCompatMigrateApplyLockTimeoutUnderAHeldLockE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		wantErr    string
		atLeast    time.Duration
		atMostWait time.Duration
	}{
		{
			name:       "zero does not wait",
			value:      "0",
			wantErr:    `(?s).*migration lock "ptah_migrate" for postgres is held by another runner.*`,
			atMostWait: 5 * time.Second,
		},
		{
			name:       "a negative value does not wait",
			value:      "-1s",
			wantErr:    `(?s).*migration lock "ptah_migrate" for postgres is held by another runner.*`,
			atMostWait: 5 * time.Second,
		},
		{
			name:       "a positive value waits that long",
			value:      "2s",
			wantErr:    `(?s).*timed out acquiring migration lock "ptah_migrate" for postgres after 2s.*`,
			atLeast:    2 * time.Second,
			atMostWait: 30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			target := scratchPostgresDatabase(c)
			dir := writeLockProbeDirectory(c)
			holder, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(holder)
			held, err := dblock.Acquire(c.Context(), holder, compatLockHolderName, 5*time.Second)
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(held.Release(c.Context()), qt.IsNil) }()
			started := time.Now()

			out, err := runCompatMigrateApply(target, dir, tt.value)

			elapsed := time.Since(started)
			c.Assert(err, qt.ErrorMatches, tt.wantErr, qt.Commentf("%s", out))
			c.Assert(elapsed >= tt.atLeast, qt.IsTrue, qt.Commentf("elapsed %s", elapsed))
			c.Assert(elapsed < tt.atMostWait, qt.IsTrue, qt.Commentf("elapsed %s", elapsed))
			c.Assert(lockProbeTableCount(c, target), qt.Equals, 0)
		})
	}
}

// TestCompatMigrateApplyProjectLockTimeoutUnderAHeldLockE2E_FailurePath: an
// atlas.hcl `migration { lock_timeout = "0" }` is read as the flag is, so it
// does not wait either. Without it the run would wait out the 10s default and
// report the elapsed wait.
func TestCompatMigrateApplyProjectLockTimeoutUnderAHeldLockE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := scratchPostgresDatabase(c)
	dir := writeLockProbeDirectory(c)
	// The project file sits above the directory, which atlas.hcl confines its
	// paths to.
	config := filepath.Join(filepath.Dir(dir), "atlas.hcl")
	c.Assert(os.WriteFile(config, []byte("env \"local\" {\n  url = \""+target+"\"\n  migration {\n"+
		"    dir = \"file://"+filepath.ToSlash(dir)+"\"\n    lock_timeout = \"0\"\n  }\n}\n"), 0o600), qt.IsNil)
	holder, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(holder)
	held, err := dblock.Acquire(c.Context(), holder, compatLockHolderName, 5*time.Second)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(held.Release(c.Context()), qt.IsNil) }()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply", "--config", "file://" + filepath.ToSlash(config), "--env", "local"})
	started := time.Now()

	err = cmd.Execute()

	elapsed := time.Since(started)
	c.Assert(err, qt.ErrorMatches, `(?s).*migration lock "ptah_migrate" for postgres is held by another runner.*`,
		qt.Commentf("%s", out.String()))
	c.Assert(elapsed < 5*time.Second, qt.IsTrue, qt.Commentf("elapsed %s", elapsed))
	c.Assert(lockProbeTableCount(c, target), qt.Equals, 0)
}

// TestCompatMigrateApplyLockTimeoutWithTheLockFreeE2E_HappyPath is the
// control: with nobody holding the lock, zero takes it on the first try and
// the migration runs.
func TestCompatMigrateApplyLockTimeoutWithTheLockFreeE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	target := scratchPostgresDatabase(c)
	dir := writeLockProbeDirectory(c)

	out, err := runCompatMigrateApply(target, dir, "0")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(lockProbeTableCount(c, target), qt.Equals, 1)
}

// runCompatMigrateApply runs `ptah-compat migrate apply` in process and returns
// what it wrote.
func runCompatMigrateApply(target, dir, lockTimeout string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply", "--url", target, "--dir", "file://" + dir, "--lock-timeout", lockTimeout})
	err := cmd.Execute()
	return out.String(), err
}

// writeLockProbeDirectory writes a hashed directory whose one migration
// creates lock_probe.
func writeLockProbeDirectory(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_probe.sql"),
		[]byte("CREATE TABLE lock_probe (id bigint PRIMARY KEY);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

// lockProbeTableCount reads back whether the migration created its table.
func lockProbeTableCount(c *qt.C, target string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(),
		`SELECT count(*) FROM pg_catalog.pg_tables WHERE tablename = 'lock_probe'`).Scan(&count), qt.IsNil)
	return count
}

// scratchPostgresDatabase creates a database of its own on the PostgreSQL test
// server, drops it when the test ends, and returns its URL. PostgreSQL scopes
// an advisory lock to its database, so the holder connects to this one, and
// the database is the test's own so "nothing was created" can be read back.
func scratchPostgresDatabase(c *qt.C) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	name := fmt.Sprintf("ptah_compat_lock_%d", time.Now().UnixNano())
	execOnServer(c, c.Context(), adminURL, `CREATE DATABASE "`+name+`"`)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		execOnServer(c, ctx, adminURL, `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
	})
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	return parsed.String()
}
