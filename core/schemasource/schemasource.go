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
	"strings"
	"time"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/processcapture"
	"ptah.run/internal/secretdisplay"
	"ptah.run/internal/sqlschema"
)

// DefaultTimeout bounds how long an external schema command may run when the
// caller does not set an explicit timeout.
const DefaultTimeout = 60 * time.Second

// Command describes an external program that writes a desired schema to stdout.
type Command struct {
	// Args is the program and its arguments. Args[0] is the executable; it is run
	// directly with no shell, so no shell quoting or expansion is applied.
	Args []string
	// Format is the stdout format: "sql", "hcl", or "yaml". Empty defaults to
	// "sql"; "yml" is accepted as an alias for "yaml".
	Format string
	// Dialect is an optional dialect hint used when parsing SQL output; the
	// other formats carry their own type information and need none. Empty
	// parses the SQL without a dialect hint.
	Dialect string
	// Dir is the working directory for the program. Empty uses the current
	// working directory. A non-empty Dir is also reflected in the program's
	// PWD environment variable, on every platform.
	Dir string
	// Timeout bounds execution. Zero uses DefaultTimeout; a negative value
	// disables the timeout.
	Timeout time.Duration
	// Env holds extra "KEY=VALUE" entries appended to the current environment.
	// A malformed entry — no equals sign, or an empty key — is refused before
	// the program starts. PATH and PWD cannot be overridden (matched
	// case-insensitively); use an explicit executable path and Dir.
	Env []string
}

// Run executes cmd and parses its standard output into a desired schema.
//
// Execution is bounded by the resolved timeout, and when Run returns — on
// success, failure, cancellation, or timeout — descendant processes the
// program started are terminated with it. Caller cancellation and a timeout
// are distinguishable: both wrap the context error, so errors.Is against
// context.Canceled or context.DeadlineExceeded tells them apart.
//
// Empty or whitespace-only stdout is rejected rather than parsed into an
// empty desired schema, so an accidentally broken provider cannot silently
// erase the desired state. Captured stdout is bounded, and a program that
// exceeds the bound is refused rather than parsed from truncated output.
//
// When the program fails to start or exits nonzero, the error carries a
// bounded excerpt of its stderr. That excerpt and any parse diagnostic (which
// can quote program output) are redacted against the secret values visible in
// the process environment and argv, and terminal control sequences in them
// are escaped, so the error can be shown to an operator or written to a log.
func Run(ctx context.Context, cmd Command) (*schemamodel.Database, error) {
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

func parseOutput(data []byte, format, dialect string) (*schemamodel.Database, error) {
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

	result, err := processcapture.Run(ctx, processcapture.Command{
		Args: cmd.Args,
		Dir:  cmd.Dir,
		Env:  cmd.Env,
	})
	if err == nil {
		return result.Stdout, nil
	}
	var failure *processcapture.Failure
	if !errors.As(err, &failure) {
		return nil, fmt.Errorf("schema command %q failed: %w", cmd.Args[0], err)
	}
	switch failure.Kind {
	case processcapture.FailureCanceled, processcapture.FailureTimedOut:
		action := "canceled"
		if failure.Kind == processcapture.FailureTimedOut {
			action = "timed out"
		}
		return nil, fmt.Errorf(
			"schema command %q %s: %w",
			cmd.Args[0],
			action,
			failure.Err,
		)
	case processcapture.FailureOutputLimit:
		return nil, fmt.Errorf(
			"schema command %q produced more than %d bytes of output",
			cmd.Args[0],
			processcapture.DefaultMaxStdout,
		)
	default:
		safeStderr := secretdisplay.Sanitize(
			failure.Stderr,
			append(os.Environ(), cmd.Env...),
			cmd.Args,
		)
		return nil, fmt.Errorf(
			"schema command %q failed: %w%s",
			cmd.Args[0],
			failure.Err,
			stderrSuffix(safeStderr),
		)
	}
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

func parseSQL(data []byte, dialect string) (*schemamodel.Database, error) {
	db, _, err := sqlschema.Read(data, dialect)
	if err != nil {
		return nil, err
	}
	return &db, nil
}
