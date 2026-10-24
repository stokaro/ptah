//go:build integration && unix

package atlas_test

// Unix only: the interrupt is delivered with Process.Signal(syscall.SIGINT),
// and Windows cannot send that signal to another process.

import (
	"bytes"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestCompatProviderInterrupt_FailurePath interrupts a ptah-compat command while
// the provider is inside a write, and expects the command to end and take the
// provider with it. The fixture records the write before it waits for
// cancellation, so the signal arrives with the provider process busy.
func TestCompatProviderInterrupt_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newCompatProviderFixture(c, buildCompatProviderBinaries(c))
	cmd := exec.Command(f.binaries.compat, "migrate", "apply", "--dir", f.migrationsURL, "--url", f.address+"?fault=delay-after") // #nosec G204 -- the test runs the ptah-compat binary it built
	cmd.Dir = f.dir
	cmd.Env = append(environmentWithoutPtahVariables(), f.withManifest()...)
	// The command writes while the test may read the output for a message.
	var output syncBuffer
	cmd.Stdout, cmd.Stderr = &output, &output
	c.Assert(cmd.Start(), qt.IsNil)
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	c.Cleanup(func() { _ = cmd.Process.Kill() })

	deadline := time.Now().Add(20 * time.Second)
	for !fileExists(f.dir+"/database.db.applied") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	c.Assert(fileExists(f.dir+"/database.db.applied"), qt.IsTrue, qt.Commentf("the provider never reached the write: %s", output.String()))
	c.Assert(cmd.Process.Signal(syscall.SIGINT), qt.IsNil)

	select {
	case runErr := <-finished:
		c.Assert(runErr, qt.IsNotNil)
	case <-time.After(15 * time.Second):
		c.Fatalf("the command did not end after the interrupt: %s", output.String())
	}
	f.assertStopped(c)
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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
