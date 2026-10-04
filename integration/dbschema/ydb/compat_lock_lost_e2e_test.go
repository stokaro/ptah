//go:build integration

package ydb_test

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// ptah-compat schema apply holds the schema apply lock, a semaphore on the
// coordination node, while it plans, asks and applies. Here the node is dropped
// while the command waits for its confirmation, which ends the session that
// held the semaphore. Once confirmed, the command applies nothing: another run
// may have taken the lock and changed the schema it planned against, so it
// reports the lost lock and creates no table.
func TestYDBCompatBinary_SchemaApplyStopsWhenTheLockIsLost(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropCompatTables(c, conn)
			c.Cleanup(func() { dropCompatTables(c, conn) })
			desired := writeCompatFile(c, c.TempDir(), "desired.hcl", compatDesired)
			cmd := exec.CommandContext(ctx, binary, "schema", "apply", "--url", url, "--to", "file://"+desired)
			stdin, err := cmd.StdinPipe()
			c.Assert(err, qt.IsNil)
			var stdout, stderr lockedOutput
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			c.Assert(cmd.Start(), qt.IsNil)

			c.Assert(stdout.waitFor("Type 'YES' to confirm", 2*time.Minute), qt.IsTrue,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String()))
			dropLockNode(c, line)
			// The holder trusts the server's last answer that it holds the
			// semaphore for five seconds, half the session timeout; give it
			// longer than that before the command goes on.
			time.Sleep(8 * time.Second)
			_, err = io.WriteString(stdin, "YES\n")
			c.Assert(err, qt.IsNil)
			c.Assert(stdin.Close(), qt.IsNil)
			waitErr := cmd.Wait()

			_, exited := errors.AsType[*exec.ExitError](waitErr)
			c.Assert(exited, qt.IsTrue, qt.Commentf("wait: %v\nstdout:\n%s\nstderr:\n%s", waitErr, stdout.String(), stderr.String()))
			c.Assert(stderr.String(), qt.Contains,
				`schema apply lock: advisory lock "ptah_schema_apply" on ydb was lost while it was held`)
			c.Assert(stdout.String(), qt.Not(qt.Contains), "Schema apply completed successfully.")
			c.Assert(tableNames(readScoped(c, conn, []string{compatDir})), qt.HasLen, 0)
		})
	}
}

// lockedOutput is a stream a running command writes to while the test reads
// it.
type lockedOutput struct {
	mu   sync.Mutex
	text strings.Builder
}

func (o *lockedOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.text.Write(p)
}

func (o *lockedOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.text.String()
}

// waitFor reports whether the stream comes to hold text within limit.
func (o *lockedOutput) waitFor(text string, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if strings.Contains(o.String(), text) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
