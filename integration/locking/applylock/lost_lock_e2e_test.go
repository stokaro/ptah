//go:build integration

package applylock_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
)

// `ptah-compat schema apply` holds its lock on a session of its own while it
// plans, asks and applies over the pool. Here that session is ended while the
// command waits for its confirmation, which frees the lock for any other
// session. Once confirmed, the command applies nothing: the schema it planned
// against may have changed under another holder, so it reports the lost lock
// and the table is never created.
func TestCompatSchemaApplyStopsWhenItsLockSessionEndsE2E(t *testing.T) {
	c := qt.New(t)
	work := c.TempDir()
	schemaPath := writeProbeSchema(c, work)
	dbURL := emptyPostgresDatabase(c, "ptah_lost_apply_lock")
	devURL := emptyPostgresDatabase(c, "ptah_lost_apply_lock_dev")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	binary := clirun.Build(c, clirun.Compat)
	cmd := exec.CommandContext(ctx, binary,
		"schema", "apply", "--url", dbURL, "--dev-url", devURL, "--to", "file://"+filepath.ToSlash(schemaPath))
	cmd.Dir = work
	stdin, err := cmd.StdinPipe()
	c.Assert(err, qt.IsNil)
	var stdout, stderr syncBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	c.Assert(cmd.Start(), qt.IsNil)

	c.Assert(waitForText(&stdout, "Type 'YES' to confirm", time.Minute), qt.IsTrue,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String()))
	key := strconv.FormatInt(dblock.PostgresKey(atlasschema.ApplyLockName), 10)
	execOnServer(c, ctx, dbURL, "SELECT pg_terminate_backend(pid) FROM pg_locks "+
		"WHERE locktype = 'advisory' AND granted AND objid::bigint = "+key)
	execOnServer(c, ctx, dbURL, "SELECT pg_try_advisory_lock("+key+")")
	// The lock's session is pinged every second; give the command time to
	// hear that it is gone before it is told to go on.
	time.Sleep(3 * time.Second)
	_, err = io.WriteString(stdin, "YES\n")
	c.Assert(err, qt.IsNil)
	c.Assert(stdin.Close(), qt.IsNil)
	waitErr := cmd.Wait()

	exitErr, exited := errors.AsType[*exec.ExitError](waitErr)
	c.Assert(exited, qt.IsTrue, qt.Commentf("wait: %v\nstdout:\n%s\nstderr:\n%s", waitErr, stdout.String(), stderr.String()))
	c.Assert(exitErr.ExitCode(), qt.Not(qt.Equals), 0)
	c.Assert(stderr.String(), qt.Contains, `schema apply lock: advisory lock "ptah_schema_apply" on postgres was lost `+
		`while it was held`)
	c.Assert(stdout.String(), qt.Not(qt.Contains), "Schema apply completed successfully.")
	c.Assert(tableNames(c, c.Context(), dbURL), qt.HasLen, 0)
}

// emptyPostgresDatabase creates a database of its own on the live PostgreSQL
// server and returns its URL. `schema apply` is declarative, so pointed at a
// database somebody else is using it plans a DROP for every table the schema
// does not declare.
func emptyPostgresDatabase(c *qt.C, name string) string {
	c.Helper()
	serverURL := dbtarget.DriverDSN(c, dbtarget.PostgreSQL)
	execOnServer(c, c.Context(), serverURL, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	execOnServer(c, c.Context(), serverURL, "CREATE DATABASE "+name)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		execOnServer(c, ctx, serverURL, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	parsed, err := url.Parse(serverURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	return parsed.String()
}

// syncBuffer is a buffer a running command writes to while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitForText reports whether buffer comes to hold text within limit.
func waitForText(buffer *syncBuffer, text string, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if strings.Contains(buffer.String(), text) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
