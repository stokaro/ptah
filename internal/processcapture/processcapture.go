// Package processcapture runs an explicit argv without a shell and captures
// bounded output while cancellation terminates the whole process tree.
package processcapture

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	// DefaultMaxStdout is the default stdout capture limit.
	DefaultMaxStdout = 64 << 20 // 64 MiB
	// DefaultMaxStderr is the default retained stderr tail.
	DefaultMaxStderr = 64 << 10 // 64 KiB
	// DefaultWaitDelay bounds process I/O shutdown after termination begins.
	DefaultWaitDelay = time.Second
)

// FailureKind classifies why a command did not return captured stdout.
type FailureKind uint8

const (
	// FailureStartOrExit reports a start, wait, or process-tree failure.
	FailureStartOrExit FailureKind = iota
	// FailureCanceled reports caller cancellation.
	FailureCanceled
	// FailureTimedOut reports a deadline.
	FailureTimedOut
	// FailureOutputLimit reports stdout beyond the configured bound.
	FailureOutputLimit
)

// Failure is a classified command failure. Err retains the underlying process
// or context error and Stderr is the bounded tail captured before failure.
type Failure struct {
	Kind   FailureKind
	Err    error
	Stderr string
}

func (e *Failure) Error() string {
	if e == nil || e.Err == nil {
		return "process capture failed"
	}
	return e.Err.Error()
}

// Unwrap exposes the underlying process or context error.
func (e *Failure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Command describes one directly executed process.
type Command struct {
	Args      []string
	Dir       string
	Env       []string
	Timeout   time.Duration
	MaxStdout int
	MaxStderr int
	WaitDelay time.Duration
}

// Result is one successful bounded capture.
type Result struct {
	Stdout []byte
	Stderr string
}

// Run executes cmd directly without shell interpretation. A positive timeout
// adds a deadline to ctx; zero and negative values use the caller context as-is.
func Run(ctx context.Context, cmd Command) (Result, error) {
	if len(cmd.Args) == 0 || strings.TrimSpace(cmd.Args[0]) == "" {
		return Result{}, &Failure{Kind: FailureStartOrExit, Err: errors.New("process command is empty")}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx := ctx
	if cmd.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, cmd.Timeout)
		defer cancel()
	}

	maxStdout := cmd.MaxStdout
	if maxStdout == 0 {
		maxStdout = DefaultMaxStdout
	}
	maxStderr := cmd.MaxStderr
	if maxStderr == 0 {
		maxStderr = DefaultMaxStderr
	}
	waitDelay := cmd.WaitDelay
	if waitDelay == 0 {
		waitDelay = DefaultWaitDelay
	}

	// #nosec G204 -- the operator provides an explicit argv that is executed directly without a shell.
	c := exec.Command(cmd.Args[0], cmd.Args[1:]...)
	prepareProcess(c)
	if strings.TrimSpace(cmd.Dir) != "" {
		c.Dir = cmd.Dir
	}
	c.WaitDelay = waitDelay
	if len(cmd.Env) > 0 {
		c.Env = append(c.Environ(), cmd.Env...)
	}
	if err := setWorkingDirectoryEnv(c); err != nil {
		return Result{}, &Failure{Kind: FailureStartOrExit, Err: err}
	}

	stdout := &cappedBuffer{limit: maxStdout}
	stderr := &tailBuffer{limit: maxStderr}
	c.Stdout = stdout
	c.Stderr = stderr

	err := executeCommand(runCtx, c, waitDelay)
	result := Result{Stdout: bytes.Clone(stdout.Bytes()), Stderr: stderr.String()}
	if ctxErr := runCtx.Err(); ctxErr != nil {
		kind := FailureCanceled
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			kind = FailureTimedOut
		}
		return Result{}, &Failure{Kind: kind, Err: errors.Join(ctxErr, err), Stderr: result.Stderr}
	}
	if err != nil {
		return Result{}, &Failure{Kind: FailureStartOrExit, Err: err, Stderr: result.Stderr}
	}
	if stdout.truncated {
		return Result{}, &Failure{Kind: FailureOutputLimit, Stderr: result.Stderr}
	}
	return result, nil
}

// setWorkingDirectoryEnv makes PWD name the directory the program actually runs
// in, on every operating system.
//
// [Command.Dir] chooses the working directory, so PWD is not the caller's to
// set: a value in [Command.Env] that disagrees with Dir describes a directory
// the process is not in. os/exec keeps only the POSIX half of that bargain --
// it appends PWD=<abs Dir> there and documents that "Windows and Plan 9 do not
// use the PWD variable, so we don't need to keep it accurate".
//
// For a shell that does use it, that is not true. Ptah started from git-bash,
// MSYS2 or Cygwin inherits their PWD, which names Ptah's own directory rather
// than Dir, and the program then receives a working directory and an
// environment that disagree.
//
// Appending is enough on both platforms: os/exec deduplicates the environment
// keeping the last occurrence of each key, case-insensitively on Windows.
func setWorkingDirectoryEnv(c *exec.Cmd) error {
	if c.Dir == "" {
		return nil
	}
	absolute, err := filepath.Abs(c.Dir)
	if err != nil {
		return fmt.Errorf("resolve process working directory: %w", err)
	}
	c.Env = append(c.Environ(), "PWD="+absolute)
	return nil
}

type processTree interface {
	terminate() error
	close() error
}

func executeCommand(ctx context.Context, cmd *exec.Cmd, waitDelay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	tree, err := attachProcessTree(cmd)
	if err != nil {
		killErr := cmd.Process.Kill()
		waitErr := cmd.Wait()
		return errors.Join(err, ignoreProcessDone(killErr), waitErr)
	}

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	select {
	case waitErr := <-waitDone:
		return errors.Join(waitErr, terminateAndClose(tree))
	case <-ctx.Done():
		terminateErr := tree.terminate()
		select {
		case waitErr := <-waitDone:
			return errors.Join(waitErr, terminateErr, tree.close())
		case <-time.After(waitDelay):
			return errors.Join(terminateErr, tree.close(), exec.ErrWaitDelay)
		}
	}
}

func terminateAndClose(tree processTree) error {
	return errors.Join(tree.terminate(), tree.close())
}

func ignoreProcessDone(err error) error {
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

type cappedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if remaining := b.limit - b.buf.Len(); remaining > 0 {
		if len(p) > remaining {
			b.buf.Write(p[:remaining])
			b.truncated = true
		} else {
			b.buf.Write(p)
		}
	} else if len(p) > 0 {
		b.truncated = true
	}
	return len(p), nil
}

func (b *cappedBuffer) Bytes() []byte  { return b.buf.Bytes() }
func (b *cappedBuffer) String() string { return b.buf.String() }

type tailBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	written := len(p)
	if b.limit <= 0 {
		b.truncated = b.truncated || len(p) > 0
		return written, nil
	}
	if len(p) >= b.limit {
		b.buf.Reset()
		b.buf.Write(p[len(p)-b.limit:])
		b.truncated = true
		return written, nil
	}
	overflow := b.buf.Len() + len(p) - b.limit
	if overflow > 0 {
		current := b.buf.Bytes()
		copy(current, current[overflow:])
		b.buf.Truncate(len(current) - overflow)
		b.truncated = true
	}
	b.buf.Write(p)
	return written, nil
}

func (b *tailBuffer) String() string { return b.buf.String() }
