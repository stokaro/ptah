// Package sql contains commands for standalone SQL files.
package sql

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/sqllint"
)

const (
	formatText = "text"
	formatJSON = "json"
)

var errSQLLintFindings = errors.New("sql lint findings found")

// NewSQLCommand returns the standalone SQL command namespace.
func NewSQLCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql",
		Short: "Work with standalone SQL files",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	cmd.AddCommand(newSQLLintCommand())
	return cmd
}

func newSQLLintCommand() *cobra.Command {
	var dialect string
	var version string
	var format string
	var stdin bool
	var disabled []string

	cmd := &cobra.Command{
		Use:   "lint [files...]",
		Short: "Lint standalone SQL files",
		Long: `Lint standalone SQL files using Ptah's SQL parser, AST, and
target capability presets.

This command is intentionally separate from ptah migrations lint, which is
migration directory specific.`,
		Args:          cobra.ArbitraryArgs,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSQLLint(cmd, sqlLintOptions{
				dialect:  dialect,
				version:  version,
				format:   format,
				stdin:    stdin,
				disabled: disabled,
				files:    args,
			})
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&dialect, "dialect", "", "Target dialect: postgres, mysql, mariadb, sqlite, sqlserver, clickhouse, cockroachdb, yugabytedb, or spanner")
	flags.StringVar(&version, "version", "", "Server version string used to refine target capabilities")
	flags.StringVar(&format, "format", formatText, "Output format: text or json")
	flags.BoolVar(&stdin, "stdin", false, "Read SQL from stdin")
	flags.StringArrayVar(&disabled, "disable", nil, "Disable a rule code or family, for example DDL001 or CAP (repeatable)")
	cmdutil.ConfigureCommandArgs(cmd, cobra.ArbitraryArgs)
	return cmd
}

type sqlLintOptions struct {
	dialect  string
	version  string
	format   string
	stdin    bool
	disabled []string
	files    []string
}

type sqlLintReport struct {
	Failed   bool              `json:"failed"`
	Dialect  string            `json:"dialect,omitempty"`
	Version  string            `json:"version,omitempty"`
	Sources  []string          `json:"sources,omitempty"`
	Disabled []string          `json:"disabled_rules,omitempty"`
	Findings []sqllint.Finding `json:"findings"`
	Error    string            `json:"error,omitempty"`
}

func runSQLLint(cmd *cobra.Command, opts sqlLintOptions) error {
	if err := validateSQLLintOptions(opts); err != nil {
		return writeSQLLintError(cmd.ErrOrStderr(), opts.format, err.Error())
	}

	sources, err := readSQLLintSources(cmd.InOrStdin(), opts)
	if err != nil {
		return writeSQLLintError(cmd.ErrOrStderr(), opts.format, err.Error())
	}

	normalizedDialect := platform.NormalizeDialect(opts.dialect)
	var findings []sqllint.Finding
	for _, source := range sources {
		sourceFindings, err := sqllint.LintSource(source, sqllint.Options{
			Dialect:       normalizedDialect,
			Version:       opts.version,
			DisabledRules: opts.disabled,
		})
		if err != nil {
			return writeSQLLintError(cmd.ErrOrStderr(), opts.format, err.Error())
		}
		findings = append(findings, sourceFindings...)
	}
	if findings == nil {
		findings = []sqllint.Finding{}
	}

	report := sqlLintReport{
		Failed:   hasErrorFinding(findings),
		Dialect:  normalizedDialect,
		Version:  opts.version,
		Sources:  sourceNames(sources),
		Disabled: opts.disabled,
		Findings: findings,
	}
	writer := cmd.OutOrStdout()
	if report.Failed {
		writer = cmd.ErrOrStderr()
	}
	if err := writeSQLLintReport(writer, opts.format, report); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if report.Failed {
		return exitcode.New(1, errSQLLintFindings)
	}
	return nil
}

func validateSQLLintOptions(opts sqlLintOptions) error {
	if opts.format != formatText && opts.format != formatJSON {
		return fmt.Errorf("invalid --format value %q: expected text or json", opts.format)
	}
	if opts.dialect != "" && platform.NormalizeDialect(opts.dialect) == "" {
		return fmt.Errorf("invalid --dialect value %q: expected postgres, mysql, mariadb, sqlite, sqlserver, clickhouse, cockroachdb, yugabytedb, or spanner", opts.dialect)
	}
	if opts.version != "" && opts.dialect == "" {
		return fmt.Errorf("--version requires --dialect")
	}
	if opts.stdin && len(opts.files) > 0 {
		return fmt.Errorf("--stdin cannot be combined with file arguments")
	}
	if !opts.stdin && len(opts.files) == 0 {
		return fmt.Errorf("at least one SQL file is required unless --stdin is set")
	}
	return nil
}

func readSQLLintSources(stdin io.Reader, opts sqlLintOptions) ([]sqllint.Source, error) {
	if opts.stdin {
		data, err := io.ReadAll(stdin)
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
		return []sqllint.Source{{Name: "<stdin>", SQL: string(data)}}, nil
	}

	sources := make([]sqllint.Source, 0, len(opts.files))
	for _, path := range opts.files {
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("SQL file %s: %w", path, err)
		}
		if info.IsDir() {
			return nil, fmt.Errorf("SQL file %s: is a directory", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read SQL file %s: %w", path, err)
		}
		sources = append(sources, sqllint.Source{
			Name: filepath.ToSlash(path),
			SQL:  string(data),
		})
	}
	return sources, nil
}

func writeSQLLintReport(w io.Writer, format string, report sqlLintReport) error {
	if format == formatJSON {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	return writeSQLLintText(w, report)
}

func writeSQLLintText(w io.Writer, report sqlLintReport) error {
	if report.Error != "" {
		_, err := fmt.Fprintf(w, "error: %s\n", report.Error)
		return err
	}
	if len(report.Findings) == 0 {
		_, err := fmt.Fprintln(w, "No SQL lint findings.")
		return err
	}
	for _, finding := range report.Findings {
		if _, err := fmt.Fprintf(w, "%s:%d:%d: %s %s: %s\n",
			finding.File,
			finding.Line,
			finding.Column,
			finding.Severity,
			finding.Rule,
			finding.Message,
		); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "\n%d finding(s).\n", len(report.Findings))
	return err
}

func writeSQLLintError(w io.Writer, format, msg string) error {
	report := sqlLintReport{
		Failed:   true,
		Findings: []sqllint.Finding{},
		Error:    msg,
	}
	if err := writeSQLLintReport(w, format, report); err != nil {
		return exitcode.New(2, fmt.Errorf("%s; additionally failed to write error report: %w", msg, err))
	}
	return exitcode.New(2, errors.New(msg))
}

func hasErrorFinding(findings []sqllint.Finding) bool {
	for _, finding := range findings {
		if finding.Severity == sqllint.SeverityError {
			return true
		}
	}
	return false
}

func sourceNames(sources []sqllint.Source) []string {
	names := make([]string, 0, len(sources))
	for _, source := range sources {
		names = append(names, source.Name)
	}
	return names
}
