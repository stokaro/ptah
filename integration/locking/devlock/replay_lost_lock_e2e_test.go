//go:build integration

package devlock_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devclean"
)

// `ptah migrations validate --dev-url` replays the directory on the dev
// database under the realm lock, which lives on a session of its own. Here the
// server ends that session while the second migration sleeps for twenty
// seconds, after the first created a table. The replay stops there and the
// command fails with the loss: another replay may take the realm once the lock
// is gone, and two replays on one dev database would destroy each other's
// objects.
//
// So the replay does not clean up either: a cleanup would drop what that other
// replay put there. The table stays, the error says the dev database was left
// as it was and how to empty it, and the next claim refuses the realm as not
// clean, with the same remedy.
func TestMigrationsValidateStopsWhenTheDevLockSessionEndsE2E(t *testing.T) {
	c := qt.New(t)
	devURL := emptyDevDatabase(c, "ptah_dev_lock_replay")
	witness, err := dbschema.ConnectToDatabase(c.Context(), devURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(witness) })
	var database string
	c.Assert(witness.QueryRowContext(c.Context(), "SELECT current_database()").Scan(&database), qt.IsNil)
	lockName := "ptah-dev-replay:postgres:" + database
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o700), qt.IsNil)
	for name, body := range map[string]string{
		"0000000001_left.up.sql":    "CREATE TABLE ptah_dev_lock_left (id INT);\n",
		"0000000001_left.down.sql":  "DROP TABLE ptah_dev_lock_left;\n",
		"0000000002_sleep.up.sql":   "SELECT pg_sleep(20);\nCREATE TABLE ptah_dev_lock_after (id INT);\n",
		"0000000002_sleep.down.sql": "DROP TABLE ptah_dev_lock_after;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	hashed := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "hash", "--dir", dir)
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	binary := clirun.Build(c, clirun.Ptah)
	cmd := exec.CommandContext(ctx, binary, "migrations", "validate", "--dir", dir, "--dev-url", devURL)
	var stderr syncBuffer
	cmd.Stderr = &stderr
	started := time.Now()
	c.Assert(cmd.Start(), qt.IsNil)

	holder := advisoryLockHolder(c, witness, dblock.PostgresKey(lockName))
	waitForQuery(c, witness, "SELECT pg_sleep(20)", &stderr)
	_, err = witness.ExecContext(c.Context(), "SELECT pg_terminate_backend($1)", holder)
	c.Assert(err, qt.IsNil)
	waitErr := cmd.Wait()
	elapsed := time.Since(started)
	again := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "validate", "--dir", dir, "--dev-url", devURL)

	exitErr, exited := errors.AsType[*exec.ExitError](waitErr)
	c.Assert(exited, qt.IsTrue, qt.Commentf("wait: %v\nstderr:\n%s", waitErr, stderr.String()))
	c.Assert(exitErr.ExitCode(), qt.Not(qt.Equals), 0)
	c.Assert(stderr.String(), qt.Contains, `dev database lock: advisory lock "`+lockName+`" on postgres was lost `+
		`while it was held`)
	c.Assert(stderr.String(), qt.Contains, "the postgres dev database "+database+" was left as it was, since another "+
		"run may be using it now: empty it by hand once no run uses it (ptah db drop-all empties a database), or "+
		devclean.NotCleanRemedy)
	c.Assert(elapsed < 15*time.Second, qt.IsTrue, qt.Commentf("the replay went on for %s", elapsed))
	c.Assert(tableCount(c, witness, "ptah_dev_lock_left"), qt.Equals, int64(1))
	c.Assert(again.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(again.Stderr, qt.Contains, `connected database is not clean: found table "ptah_dev_lock_left"`)
	c.Assert(again.Stderr, qt.Contains, devclean.NotCleanRemedy)
}

// tableCount reads how many tables of a name the connection's database holds.
func tableCount(c *qt.C, conn *dbschema.DatabaseConnection, table string) int64 {
	c.Helper()
	var count int64
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1", table).Scan(&count), qt.IsNil)
	return count
}

// advisoryLockHolder waits until a session holds the PostgreSQL advisory lock
// key, and returns its process id.
func advisoryLockHolder(c *qt.C, conn *dbschema.DatabaseConnection, key int64) int64 {
	c.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		var pid int64
		err := conn.QueryRowContext(c.Context(),
			"SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid::bigint = $1", key).Scan(&pid)
		if err == nil {
			return pid
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.Fatalf("no session took advisory lock %d within a minute", key)
	return 0
}

// waitForQuery waits until a session runs a statement that starts with text,
// which the replay reaches once it holds the lock and has checked it.
func waitForQuery(c *qt.C, conn *dbschema.DatabaseConnection, text string, stderr *syncBuffer) {
	c.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		var running int64
		err := conn.QueryRowContext(c.Context(),
			"SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active' AND starts_with(query, $1)", text).Scan(&running)
		if err == nil && running > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.Fatalf("no session ran %q within a minute; the command wrote:\n%s", text, stderr.String())
}

// emptyDevDatabase creates a database of its own on the live PostgreSQL server
// and returns its URL: a replay refuses a dev database that holds anything.
func emptyDevDatabase(c *qt.C, name string) string {
	c.Helper()
	serverURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	server, err := dbschema.ConnectToDatabase(c.Context(), serverURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(server) })
	_, err = server.ExecContext(c.Context(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	c.Assert(err, qt.IsNil)
	_, err = server.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := server.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		c.Check(err, qt.IsNil)
	})
	parsed, err := url.Parse(serverURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	return parsed.String()
}

// syncBuffer is a buffer a running command writes to while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}
