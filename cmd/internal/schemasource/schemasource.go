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
	"github.com/stokaro/ptah/internal/convert/toschema"
	"github.com/stokaro/ptah/internal/parser"
)

// DefaultTimeout bounds how long an external schema command may run when the
// caller does not set an explicit timeout.
const DefaultTimeout = 60 * time.Second

// maxCapturedOutput bounds how much stdout (and, for error reporting, stderr)
// Ptah buffers from a schema command, so a runaway program cannot exhaust memory.
const maxCapturedOutput = 64 << 20 // 64 MiB

// waitDelay bounds how long Run waits for a timed-out or canceled command's
// inherited I/O to drain before force-closing it (see exec.Cmd.WaitDelay).
// Without it, a program that spawns a surviving grandchild holding the stdout
// pipe open could block Ptah past its timeout. It is a variable so tests can
// shorten it.
var waitDelay = 10 * time.Second

// Command describes an external program that writes a desired schema to stdout.
type Command struct {
	// Args is the program and its arguments. Args[0] is the executable; it is run
	// directly with no shell, so no shell quoting or expansion is applied.
	Args []string
	// Format is the stdout format. Empty defaults to "sql". "hcl" and "yaml" are
	// not yet supported by the external source.
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
	Env []string
}

// ParseCommandLine splits a whitespace-separated command line into arguments. It
// does NOT interpret shell quoting, so an argument containing spaces cannot be
// expressed this way — use the configuration file's explicit argument list for
// those cases.
func ParseCommandLine(line string) []string {
	return strings.Fields(line)
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
	if format != "sql" {
		return nil, fmt.Errorf("unsupported schema command format %q: only \"sql\" is supported", cmd.Format)
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

	db, err := parseSQL(stdout, cmd.Dialect)
	if err != nil {
		return nil, fmt.Errorf("parse schema command %q output: %w", cmd.Args[0], err)
	}
	return db, nil
}

func run(ctx context.Context, cmd Command) ([]byte, error) {
	// The program and its arguments are supplied by the operator running Ptah
	// (through --schema-cmd or ptah.yaml), analogous to git's core.editor. Ptah
	// runs it directly with an explicit argument vector and never through a
	// shell, so there is no shell-injection surface.
	c := exec.CommandContext(ctx, cmd.Args[0], cmd.Args[1:]...) //nolint:gosec // operator-provided command, run directly without a shell
	c.Dir = cmd.Dir
	// Bound the wait for a killed program's inherited I/O to drain, so a loader
	// that leaves a surviving child holding the stdout pipe open cannot block
	// Ptah past its timeout.
	c.WaitDelay = waitDelay
	if len(cmd.Env) > 0 {
		c.Env = append(os.Environ(), cmd.Env...)
	}

	stdout := &cappedBuffer{limit: maxCapturedOutput}
	stderr := &cappedBuffer{limit: maxCapturedOutput}
	c.Stdout = stdout
	c.Stderr = stderr

	if err := c.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("schema command %q timed out", cmd.Args[0])
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, fmt.Errorf("schema command %q canceled", cmd.Args[0])
		}
		return nil, fmt.Errorf("schema command %q failed: %w%s", cmd.Args[0], err, stderrSuffix(stderr.String()))
	}
	if stdout.truncated {
		return nil, fmt.Errorf("schema command %q produced more than %d bytes of output", cmd.Args[0], maxCapturedOutput)
	}
	return stdout.Bytes(), nil
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
