// Package schemasource runs an external program that emits a desired schema to
// its standard output and parses that output into Ptah's schema IR.
//
// It is the building block behind the external-command desired-schema source:
// Ptah runs an operator-configured loader (for example an ORM's schema
// exporter) and consumes its stdout, decoupling the desired state from how it
// was produced. The program is always executed directly with an explicit
// argument vector — never through a shell — so no shell quoting or expansion is
// applied.
package schemasource

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/atlashcl"
	"github.com/stokaro/ptah/internal/convert/toschema"
	"github.com/stokaro/ptah/internal/parser"
	"github.com/stokaro/ptah/internal/secretdisplay"
	"github.com/stokaro/ptah/internal/yamlschema"
)

// DefaultTimeout bounds how long an external schema command may run when the
// caller does not set an explicit timeout.
const DefaultTimeout = 60 * time.Second

// maxCapturedOutput bounds how much stdout Ptah buffers from a schema command,
// so a runaway program cannot exhaust memory.
const maxCapturedOutput = 64 << 20 // 64 MiB

// maxCapturedStderr bounds the rolling diagnostic tail retained from stderr.
const maxCapturedStderr = 64 << 10 // 64 KiB

// waitDelay bounds how long Run waits for process I/O after the direct process
// exits or process-tree termination begins.
const waitDelay = time.Second

// Command describes an external program that writes a desired schema to stdout.
type Command struct {
	// Args is the program and its arguments. Args[0] is the executable; it is run
	// directly with no shell, so no shell quoting or expansion is applied.
	Args []string
	// Format is the stdout format: "sql", "hcl", or "yaml". Empty defaults to
	// "sql"; "yml" is accepted as an alias for "yaml".
	Format string
	// Dialect is an optional dialect hint used when parsing SQL output.
	Dialect string
	// Dir is the working directory for the program. Empty uses the current
	// working directory.
	Dir string
	// Timeout bounds execution. Zero uses DefaultTimeout; a negative value
	// disables the timeout.
	Timeout time.Duration
	// Env holds extra "KEY=VALUE" entries appended to the current environment.
	// PATH and PWD cannot be overridden; use an explicit executable path and Dir.
	Env []string
}

// Run executes cmd and parses its standard output into a desired schema. It
// bounds execution with a timeout, and on failure surfaces the program's stderr.
func Run(ctx context.Context, cmd Command) (*goschema.Database, error) {
	if len(cmd.Args) == 0 || strings.TrimSpace(cmd.Args[0]) == "" {
		return nil, errors.New("schema command is empty")
	}

	format := strings.ToLower(strings.TrimSpace(cmd.Format))
	if format == "" {
		format = "sql"
	}
	switch format {
	case "sql", "hcl", "yaml", "yml":
	default:
		return nil, fmt.Errorf(
			"unsupported schema command format %q: expected sql, hcl, or yaml",
			cmd.Format,
		)
	}

	timeout := cmd.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	runCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	stdout, err := run(runCtx, cmd)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(stdout)) == 0 {
		return nil, fmt.Errorf("schema command %q produced empty output", cmd.Args[0])
	}

	db, err := parseOutput(stdout, format, cmd.Dialect)
	if err != nil {
		safeErr := secretdisplay.SanitizeError(
			err,
			append(os.Environ(), cmd.Env...),
			cmd.Args,
		)
		return nil, fmt.Errorf("parse schema command %q output: %w", cmd.Args[0], safeErr)
	}
	return db, nil
}

func parseOutput(data []byte, format, dialect string) (*goschema.Database, error) {
	switch format {
	case "sql":
		return parseSQL(data, dialect)
	case "hcl":
		return atlashcl.Parse(data, "schema-command.hcl")
	case "yaml", "yml":
		return yamlschema.Parse(data)
	default:
		return nil, fmt.Errorf(
			"unsupported schema command format %q: expected sql, hcl, or yaml",
			format,
		)
	}
}

func run(ctx context.Context, cmd Command) ([]byte, error) {
	if err := validateEnvironment(cmd.Env); err != nil {
		return nil, err
	}

	// The program and its arguments are supplied by the operator running Ptah
	// (through --schema-cmd or ptah.yaml), analogous to git's core.editor. Ptah
	// runs it directly with an explicit argument vector and never through a
	// shell, so there is no shell-injection surface.
	c := exec.Command(cmd.Args[0], cmd.Args[1:]...) //nolint:gosec // operator-provided command, run directly without a shell
	prepareProcess(c)
	if strings.TrimSpace(cmd.Dir) != "" {
		c.Dir = cmd.Dir
	}
	c.WaitDelay = waitDelay
	if len(cmd.Env) > 0 {
		c.Env = append(c.Environ(), cmd.Env...)
	}
	effectiveEnv := c.Environ()

	stdout := &cappedBuffer{limit: maxCapturedOutput}
	stderr := &tailBuffer{limit: maxCapturedStderr}
	c.Stdout = stdout
	c.Stderr = stderr

	err := executeCommand(ctx, c)
	if ctxErr := ctx.Err(); ctxErr != nil {
		action := "canceled"
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			action = "timed out"
		}
		return nil, fmt.Errorf(
			"schema command %q %s: %w",
			cmd.Args[0],
			action,
			errors.Join(ctxErr, err),
		)
	}
	if err != nil {
		safeStderr := secretdisplay.Sanitize(stderr.String(), effectiveEnv, cmd.Args)
		return nil, fmt.Errorf(
			"schema command %q failed: %w%s",
			cmd.Args[0],
			err,
			stderrSuffix(safeStderr),
		)
	}
	if stdout.truncated {
		return nil, fmt.Errorf("schema command %q produced more than %d bytes of output", cmd.Args[0], maxCapturedOutput)
	}
	return stdout.Bytes(), nil
}

type processTree interface {
	terminate() error
	close() error
}

func executeCommand(ctx context.Context, cmd *exec.Cmd) error {
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
		return errors.Join(
			fmt.Errorf("attach schema command process tree: %w", err),
			ignoreProcessDone(killErr),
			waitErr,
		)
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

func validateEnvironment(env []string) error {
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			return fmt.Errorf("invalid schema command environment entry %q: expected KEY=VALUE", entry)
		}
		switch {
		case strings.EqualFold(key, "PATH"):
			return errors.New(
				"schema command environment must not override PATH; use an explicit executable path",
			)
		case strings.EqualFold(key, "PWD"):
			return errors.New(
				"schema command environment must not override PWD; use the command working directory",
			)
		}
	}
	return nil
}

// cappedBuffer accumulates up to limit bytes and then silently discards the
// rest, recording that truncation happened. Write always reports a full write so
// the child process is not killed with a short-write error; the timeout still
// bounds a program that keeps producing output past the cap.
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

// tailBuffer drains all writes while retaining only the most recent limit
// bytes. It keeps the diagnostic useful when a command emits a large preamble
// before its final error.
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

// stderrSuffix formats a trailing, length-bounded excerpt of the program's
// stderr for inclusion in an error message.
func stderrSuffix(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	const maxLen = 2000
	if len(s) > maxLen {
		s = "..." + s[len(s)-maxLen:]
	}
	return ": " + s
}

func parseSQL(data []byte, dialect string) (*goschema.Database, error) {
	statements, err := parser.NewParser(string(data), parser.WithDialect(dialect)).Parse()
	if err != nil {
		return nil, err
	}
	db := toschema.ToDatabase(statements)
	goschema.Finalize(&db)
	return &db, nil
}
