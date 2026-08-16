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

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/convert/toschema"
	"go.5x5.cz/ptah/internal/parser"
	"go.5x5.cz/ptah/internal/processcapture"
	"go.5x5.cz/ptah/internal/secretdisplay"
	"go.5x5.cz/ptah/internal/yamlschema"
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

func parseSQL(data []byte, dialect string) (*goschema.Database, error) {
	statements, err := parser.NewParser(string(data), parser.WithDialect(dialect)).Parse()
	if err != nil {
		return nil, err
	}
	db := toschema.ToDatabase(statements, dialect)
	goschema.Finalize(&db)
	return &db, nil
}
