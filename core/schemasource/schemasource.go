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
	"ptah.run/internal/processcapture"
	"ptah.run/internal/secretdisplay"
)

// DefaultTimeout bounds command execution and output interpretation when the
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
	// Dialect selects the SQL grammar and the default owner of target-scoped
	// HCL blocks. A block's explicit dialect takes precedence. YAML carries
	// its own target scopes. Empty leaves selection to the format reader.
	Dialect string
	// Dir is the working directory for the program. Empty uses the current
	// working directory. A non-empty Dir is also reflected in the program's
	// PWD environment variable, on every platform.
	Dir string
	// Timeout bounds execution and output interpretation. Zero uses
	// DefaultTimeout; a negative value
	// disables the timeout.
	Timeout time.Duration
	// Env holds extra "KEY=VALUE" entries appended to the current environment.
	// A malformed entry — no equals sign, or an empty key — is refused before
	// the program starts. PATH and PWD cannot be overridden (matched
	// case-insensitively); use an explicit executable path and Dir.
	Env []string
}

// ErrNoFormats is returned for a nil [Formats]: a program's output cannot be
// read without its readers.
var ErrNoFormats = errors.New("no readers of the schema command's output were given")

// Formats read a program's output in each format Run accepts. The readers
// decode the syntax of the feature owners a caller selects, so this package
// takes them from its caller rather than linking any owner;
// ptah.run/sourceformats.New returns the bundled ones.
type Formats interface {
	// ReadYAML reads a YAML document, as ptah.run/core/yamlschema.Parse does
	// with the owners the readers were built for.
	ReadYAML(ctx context.Context, data []byte) (*schemamodel.Database, error)
	// ReadSQL reads SQL DDL statements. dialect is a hint, empty for none.
	ReadSQL(ctx context.Context, data []byte, dialect string) (*schemamodel.Database, error)
	// ReadHCL reads an Atlas HCL document, naming it filename in a refusal.
	// dialect selects a scoped block's owner when the block names none.
	ReadHCL(ctx context.Context, data []byte, filename, dialect string) (*schemamodel.Database, error)
}

// Run executes cmd and parses its standard output into a desired schema with
// formats, the readers of each output format; a nil formats is refused with
// [ErrNoFormats] before the program starts.
//
// Execution and output interpretation share the resolved timeout. When Run returns — on
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
func Run(ctx context.Context, formats Formats, cmd Command) (*schemamodel.Database, error) {
	if len(cmd.Args) == 0 || strings.TrimSpace(cmd.Args[0]) == "" {
		return nil, errors.New("schema command is empty")
	}
	if formats == nil {
		return nil, fmt.Errorf("schema command: %w", ErrNoFormats)
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

	db, err := parseOutput(runCtx, formats, stdout, format, cmd.Dialect)
	if err != nil {
		safeErr := secretdisplay.SanitizeError(
			err,
			append(os.Environ(), cmd.Env...),
			cmd.Args,
		)
		return nil, fmt.Errorf("parse schema command %q output: %w", cmd.Args[0], safeErr)
	}
	if err := runCtx.Err(); err != nil {
		return nil, err
	}
	return db, nil
}

func parseOutput(ctx context.Context, formats Formats, data []byte, format, dialect string) (*schemamodel.Database, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch format {
	case "sql":
		return formats.ReadSQL(ctx, data, dialect)
	case "hcl":
		return formats.ReadHCL(ctx, data, "schema-command.hcl", dialect)
	case "yaml", "yml":
		return formats.ReadYAML(ctx, data)
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
