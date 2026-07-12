package drift

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/internal/schemaops"
	"github.com/stokaro/ptah/migration/safety"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

const (
	formatText          = "text"
	formatJSON          = "json"
	formatGitHubActions = "github-actions"

	severityAll         = "all"
	severityDestructive = "destructive"
)

var errDriftDetected = errors.New("schema drift detected")

// NewDriftCommand returns the drift-check command.
func NewDriftCommand() *cobra.Command {
	var rootDir string
	var dbURL string
	var format string
	var severity string
	var ignored []string
	var useExitCode bool
	var connectTimeoutRaw string

	cmd := &cobra.Command{
		Use:           "drift",
		Short:         "Check live database drift against Go entities",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDrift(cmd, runOptions{
				rootDir:           rootDir,
				dbURL:             dbURL,
				format:            format,
				severity:          severity,
				ignored:           ignored,
				useExitCode:       useExitCode,
				connectTimeoutRaw: connectTimeoutRaw,
			})
		},
	}

	cmd.Flags().StringVar(&rootDir, "root-dir", "./", "Root directory to scan for Go entities")
	cmd.Flags().StringVar(&dbURL, "db-url", "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	cmd.Flags().StringVar(&format, "format", formatText, "Output format: text, json, github-actions")
	cmd.Flags().StringVar(&severity, "severity", severityAll, "Failing drift threshold: all or destructive")
	cmd.Flags().StringArrayVar(&ignored, "ignore", nil, "Ignore drift for a scope, for example tables=audit_log,sessions")
	cmd.Flags().BoolVar(&useExitCode, "exit-code", true, "Return 1 when drift exceeds --severity; errors still return 2")
	cmd.Flags().StringVar(
		&connectTimeoutRaw,
		dbcli.ConnectTimeoutFlagName,
		dbcli.DefaultConnectTimeout.String(),
		"Maximum time to wait when establishing the initial database connection (for example 5s or 1m). Use 0 to disable the timeout.",
	)

	return cmd
}

type runOptions struct {
	rootDir           string
	dbURL             string
	format            string
	severity          string
	ignored           []string
	useExitCode       bool
	connectTimeoutRaw string
}

type driftReport struct {
	Drift            bool                  `json:"drift"`
	Failed           bool                  `json:"failed"`
	FailureThreshold string                `json:"failure_threshold"`
	HighestSeverity  safety.Severity       `json:"highest_severity"`
	Dialect          string                `json:"dialect,omitempty"`
	RootDir          string                `json:"root_dir,omitempty"`
	DatabaseURL      string                `json:"database_url,omitempty"`
	IgnoredTables    []string              `json:"ignored_tables,omitempty"`
	Findings         []safety.Finding      `json:"findings,omitempty"`
	Diff             *difftypes.SchemaDiff `json:"diff,omitempty"`
	Error            string                `json:"error,omitempty"`
}

func runDrift(cmd *cobra.Command, opts runOptions) error {
	if opts.dbURL == "" {
		return writeError(cmd.ErrOrStderr(), opts.format, "database URL is required")
	}
	if err := validateFormat(opts.format); err != nil {
		return writeError(cmd.ErrOrStderr(), formatText, err.Error())
	}
	if err := validateSeverity(opts.severity); err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, err.Error())
	}

	ignoredTables, err := parseIgnoredTables(opts.ignored)
	if err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, err.Error())
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeoutRaw)
	if err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, err.Error())
	}

	result, err := schemaops.Compare(cmd.Context(), schemaops.CompareOptions{
		RootDir:        opts.rootDir,
		DatabaseURL:    opts.dbURL,
		ConnectTimeout: connectTimeout,
		IgnoredTables:  ignoredTables,
	})
	if err != nil {
		return writeError(cmd.ErrOrStderr(), opts.format, err.Error())
	}

	findings := safety.ClassifySchemaDiff(result.Diff)
	highest := safety.Highest(findings)
	hasDrift := result.Diff.HasChanges()
	failed := opts.useExitCode && hasDrift && shouldFailDrift(highest, opts.severity)
	report := driftReport{
		Drift:            hasDrift,
		Failed:           failed,
		FailureThreshold: opts.severity,
		HighestSeverity:  highest,
		Dialect:          result.Dialect,
		RootDir:          result.RootDir,
		DatabaseURL:      result.DatabaseURL,
		IgnoredTables:    ignoredTables,
		Findings:         findings,
		Diff:             result.Diff,
	}

	writer := cmd.OutOrStdout()
	if hasDrift {
		writer = cmd.ErrOrStderr()
	}
	if err := writeReport(writer, opts.format, report); err != nil {
		return writeError(cmd.ErrOrStderr(), formatText, err.Error())
	}
	if failed {
		return exitcode.New(1, errDriftDetected)
	}
	return nil
}

func writeError(w io.Writer, format string, msg string) error {
	report := driftReport{
		Failed:           true,
		FailureThreshold: severityAll,
		HighestSeverity:  safety.Safe,
		Error:            msg,
	}
	_ = writeReport(w, format, report)
	return exitcode.New(2, errors.New(msg))
}

func validateFormat(format string) error {
	switch format {
	case formatText, formatJSON, formatGitHubActions:
		return nil
	default:
		return fmt.Errorf("invalid --format value %q: expected text, json, or github-actions", format)
	}
}

func validateSeverity(severity string) error {
	switch severity {
	case severityAll, severityDestructive:
		return nil
	default:
		return fmt.Errorf("invalid --severity value %q: expected all or destructive", severity)
	}
}

func parseIgnoredTables(values []string) ([]string, error) {
	seen := make(map[string]struct{})
	for _, value := range values {
		key, raw, ok := strings.Cut(value, "=")
		if !ok || strings.TrimSpace(key) != "tables" {
			return nil, fmt.Errorf("invalid --ignore value %q: expected tables=name[,name...]", value)
		}
		for name := range strings.SplitSeq(raw, ",") {
			name = strings.TrimSpace(name)
			if name != "" {
				seen[name] = struct{}{}
			}
		}
	}

	tables := make([]string, 0, len(seen))
	for table := range seen {
		tables = append(tables, table)
	}
	slices.Sort(tables)
	return tables, nil
}

func shouldFailDrift(highest safety.Severity, severity string) bool {
	switch severity {
	case severityDestructive:
		return highest == safety.Destructive
	default:
		return true
	}
}

func writeReport(w io.Writer, format string, report driftReport) error {
	switch format {
	case formatJSON:
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	case formatGitHubActions:
		return writeGitHubActionsReport(w, report)
	default:
		return writeTextReport(w, report)
	}
}

func writeTextReport(w io.Writer, report driftReport) error {
	if report.Error != "" {
		_, err := fmt.Fprintf(w, "Error: %s\n", report.Error)
		return err
	}
	if !report.Drift {
		_, err := fmt.Fprintln(w, "No schema drift detected.")
		return err
	}

	if _, err := fmt.Fprintf(w, "Schema drift detected (highest severity: %s).\n", report.HighestSeverity); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Failure threshold: %s. Failing: %t.\n", report.FailureThreshold, report.Failed); err != nil {
		return err
	}
	if report.DatabaseURL != "" {
		if _, err := fmt.Fprintf(w, "Database: %s\n", report.DatabaseURL); err != nil {
			return err
		}
	}
	if len(report.IgnoredTables) > 0 {
		if _, err := fmt.Fprintf(w, "Ignored tables: %s\n", strings.Join(report.IgnoredTables, ", ")); err != nil {
			return err
		}
	}
	if len(report.Findings) == 0 {
		return nil
	}
	_, err := fmt.Fprintln(w, "\nFindings:")
	if err != nil {
		return err
	}
	for _, finding := range report.Findings {
		if _, err := fmt.Fprintf(w, "- %s: %d (%s)\n", finding.Category, finding.Count, finding.Severity); err != nil {
			return err
		}
	}
	return nil
}

func writeGitHubActionsReport(w io.Writer, report driftReport) error {
	if report.Error != "" {
		_, err := fmt.Fprintf(w, "::error title=Ptah drift check failed::%s\n", escapeWorkflowCommand(report.Error))
		return err
	}
	if !report.Drift {
		_, err := fmt.Fprintln(w, "::notice title=Ptah drift check::No schema drift detected")
		return err
	}

	level := "warning"
	if report.Failed || report.HighestSeverity == safety.Destructive {
		level = "error"
	}
	message := fmt.Sprintf(
		"Schema drift detected; highest severity: %s; failure threshold: %s",
		report.HighestSeverity,
		report.FailureThreshold,
	)
	if _, err := fmt.Fprintf(w, "::%s title=Ptah schema drift::%s\n", level, escapeWorkflowCommand(message)); err != nil {
		return err
	}
	for _, finding := range report.Findings {
		message := fmt.Sprintf("%s: %d (%s)", finding.Category, finding.Count, finding.Severity)
		if _, err := fmt.Fprintf(w, "::%s title=Ptah drift finding::%s\n", level, escapeWorkflowCommand(message)); err != nil {
			return err
		}
	}
	return nil
}

func escapeWorkflowCommand(s string) string {
	replacer := strings.NewReplacer(
		"%", "%25",
		"\r", "%0D",
		"\n", "%0A",
	)
	return replacer.Replace(s)
}
