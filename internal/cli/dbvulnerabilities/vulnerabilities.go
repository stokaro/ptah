// Package dbvulnerabilities contains the native local advisory scan command.
package dbvulnerabilities

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/vulnerability"
)

type options struct {
	dbURLs       []string
	advisories   string
	suppressions string
	format       string
	failOn       string
	deployment   string
	minSeverity  string
	failSeverity string
	ignore       []string
}

// NewCommand returns the native `db vulnerabilities` command.
func NewCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "vulnerabilities",
		Short: "Match live databases against local vulnerability advisories",
		Long: `Read each database's product and version, then match a pinned local advisory
snapshot. PostgreSQL core and selected bundled contrib modules, MySQL, MariaDB
and ClickHouse servers are covered. Unsupported products, unrecognized builds,
stale advisories, partial snapshots, and installed extensions without a
reviewed matcher are reported as incomplete. A target that cannot be read is
reported with a scan error, never as clean. A managed deployment is reported
as not applicable to operator-managed server patching. The command does not
contact an advisory service or upload database inventory.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, opts)
		},
	}
	severities := strings.Join(vulnerability.SeverityNames(), ", ")
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.dbURLs, "db-url", nil, "Database URL to scan; repeat to scan several databases (required)")
	flags.StringVar(&opts.advisories, "advisories", "", "Local advisory snapshot JSON; empty uses each engine's pinned snapshot")
	flags.StringVar(&opts.suppressions, "suppressions", "", "Local JSON file of scoped advisory suppressions")
	flags.StringVar(&opts.format, "format", "table", "Output format: table or json")
	flags.StringVar(&opts.failOn, "fail-on", "both", "CI policy: findings, incomplete, both, or none")
	flags.StringVar(&opts.deployment, "deployment", "self-managed", "Server patch responsibility: self-managed or managed")
	flags.StringVar(&opts.minSeverity, "min-severity", "none", "Leave out advisories rated below this severity: "+severities)
	flags.StringVar(&opts.failSeverity, "fail-severity", "none", "Lowest severity a finding needs to meet --fail-on: "+severities)
	flags.StringArrayVar(&opts.ignore, "ignore", nil, "Advisory ID whose affected finding is recorded as suppressed; repeatable")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// targetReport is one database's report. ScanError is set when the target
// could not be assessed; its report then carries no assessments.
type targetReport struct {
	Target string `json:"target"`
	vulnerability.Report
	ScanError string `json:"scan_error,omitempty"`
}

type document struct {
	Targets []targetReport `json:"targets"`
}

func run(cmd *cobra.Command, opts options) error {
	policy, err := validate(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := vulnerability.ValidateAdvisoryInput(opts.advisories); err != nil {
		targets := make([]targetReport, 0, len(opts.dbURLs))
		for _, dbURL := range opts.dbURLs {
			targets = append(targets, failedTarget(dbURL, vulnerability.Inventory{}, "invalid", "advisory snapshot is unavailable or invalid"))
		}
		if writeErr := write(cmd.OutOrStdout(), opts.format, targets); writeErr != nil {
			return cmdutil.Fail(cmd, errors.Join(err, writeErr))
		}
		return cmdutil.Fail(cmd, err)
	}
	now := time.Now().UTC()
	targets := make([]targetReport, 0, len(opts.dbURLs))
	var failures []error
	for _, dbURL := range opts.dbURLs {
		target, err := scan(cmd, dbURL, opts, policy, now)
		targets = append(targets, target)
		if err != nil {
			failures = append(failures, err)
		}
	}
	if err := write(cmd.OutOrStdout(), opts.format, targets); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write vulnerability report: %w", err))
	}
	if len(failures) != 0 {
		return cmdutil.Fail(cmd, errors.Join(failures...))
	}
	failFindings := opts.failOn == "findings" || opts.failOn == "both"
	failIncomplete := opts.failOn == "incomplete" || opts.failOn == "both"
	for _, target := range targets {
		if (failFindings && target.HasFindingsAtOrAbove(policy.FailSeverity)) || (failIncomplete && target.IsIncomplete()) {
			return exitcode.New(1, fmt.Errorf("vulnerability scan meets --fail-on=%s", opts.failOn))
		}
	}
	return nil
}

func validate(opts options) (vulnerability.Policy, error) {
	if len(opts.dbURLs) == 0 || slices.Contains(opts.dbURLs, "") {
		return vulnerability.Policy{}, errors.New("--db-url is required and must not be empty")
	}
	if opts.format != "table" && opts.format != "json" {
		return vulnerability.Policy{}, errors.New("--format must be table or json")
	}
	if opts.failOn != "findings" && opts.failOn != "incomplete" && opts.failOn != "both" && opts.failOn != "none" {
		return vulnerability.Policy{}, errors.New("--fail-on must be findings, incomplete, both, or none")
	}
	if opts.deployment != "self-managed" && opts.deployment != "managed" {
		return vulnerability.Policy{}, errors.New("--deployment must be self-managed or managed")
	}
	minSeverity, err := vulnerability.ParseSeverity(opts.minSeverity)
	if err != nil {
		return vulnerability.Policy{}, fmt.Errorf("--min-severity: %w", err)
	}
	failSeverity, err := vulnerability.ParseSeverity(opts.failSeverity)
	if err != nil {
		return vulnerability.Policy{}, fmt.Errorf("--fail-severity: %w", err)
	}
	policy := vulnerability.Policy{MinSeverity: minSeverity, FailSeverity: failSeverity, Ignore: opts.ignore}
	if err := policy.Validate(); err != nil {
		return vulnerability.Policy{}, fmt.Errorf("--ignore: %w", err)
	}
	return policy, nil
}

// scan assesses one target. A failure is returned with the target's report so
// the other targets are still scanned and every one is written.
func scan(cmd *cobra.Command, dbURL string, opts options, policy vulnerability.Policy, now time.Time) (targetReport, error) {
	conn, err := dbschema.ConnectToDatabase(cmd.Context(), dbURL)
	if err != nil {
		return failedTarget(dbURL, vulnerability.Inventory{}, "not_assessed", "database connection failed"),
			dburldisplay.RedactError(dbURL, fmt.Errorf("connect to --db-url %s: %w", dburldisplay.Format(dbURL), err))
	}
	defer func() { _ = conn.Close() }()
	report, err := vulnerability.Scan(cmd.Context(), conn, opts.advisories, opts.suppressions, now, opts.deployment)
	if err != nil {
		inventory := vulnerability.CollectIdentity(conn)
		var scanErr *vulnerability.ScanError
		if errors.As(err, &scanErr) && scanErr.Stage == "advisory snapshot" {
			return failedTarget(dbURL, inventory, "invalid", "advisory snapshot is unavailable or invalid"), err
		}
		return failedTarget(dbURL, inventory, "not_assessed", "database inventory or suppression file is unreadable"), err
	}
	return targetReport{Target: dburldisplay.Format(dbURL), Report: policy.Apply(report)}, nil
}

func failedTarget(dbURL string, inventory vulnerability.Inventory, status, message string) targetReport {
	report := vulnerability.Report{Inventory: inventory, Applicability: "unassessed",
		Source:      vulnerability.SourceEvidence{Status: status},
		Assessments: make([]vulnerability.Assessment, 0),
		Incomplete:  []vulnerability.Incomplete{{Product: inventory.Product, Reason: message}}}
	return targetReport{Target: dburldisplay.Format(dbURL), Report: report, ScanError: message}
}

func write(w io.Writer, format string, targets []targetReport) error {
	if format == "json" {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(document{Targets: targets})
	}
	for index, target := range targets {
		if index > 0 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if err := writeTable(w, target); err != nil {
			return err
		}
	}
	return nil
}

func writeTable(w io.Writer, target targetReport) error {
	if _, err := fmt.Fprintf(w, "Target: %s\n", target.Target); err != nil {
		return err
	}
	if target.ScanError != "" {
		_, err := fmt.Fprintf(w, "SCAN ERROR: %s\n", target.ScanError)
		return err
	}
	report := target.Report
	if _, err := fmt.Fprintf(w, "Product: %s\nServer: %s\nDeployment: %s (%s)\nAdvisories: %s (source SHA-256 %s; %d days old; %s)\nSnapshot SHA-256: %s\n",
		report.Inventory.Product, report.Inventory.ServerBanner, report.Deployment, report.Applicability, report.Source.URL,
		report.Source.SHA256, report.Source.AgeDays, report.Source.Status, report.Source.SnapshotSHA256); err != nil {
		return err
	}
	if report.Filter != nil {
		if _, err := fmt.Fprintf(w, "Severity floor: %s (%d advisories below it left out)\n",
			report.Filter.MinSeverity, len(report.Filter.BelowFloor)); err != nil {
			return err
		}
	}
	findings, fixed, unaffected, uncertain, suppressed := 0, 0, 0, 0, 0
	for _, assessment := range report.Assessments {
		switch assessment.Status {
		case "affected":
			if assessment.Suppression != nil {
				suppressed++
			} else {
				findings++
			}
		case "fixed":
			fixed++
		case "unaffected":
			unaffected++
		case "uncertain":
			uncertain++
		}
		if assessment.Status != "affected" && assessment.Status != "uncertain" {
			continue
		}
		if err := writeAssessment(w, assessment); err != nil {
			return err
		}
	}
	for _, gap := range report.Incomplete {
		if _, err := fmt.Fprintf(w, "INCOMPLETE %s %s: %s\n", gap.Product, gap.Component, gap.Reason); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "Summary: %d findings, %d suppressed, %d fixed, %d unaffected, %d uncertain, %d incomplete\n",
		findings, suppressed, fixed, unaffected, uncertain, len(report.Incomplete))
	return err
}

func writeAssessment(w io.Writer, assessment vulnerability.Assessment) error {
	severity := "not supplied"
	if assessment.CVSS != nil {
		severity = fmt.Sprintf("CVSS %.1f %s", *assessment.CVSS, assessment.Severity)
	}
	line := fmt.Sprintf("%s %s %s: %s (server %s", strings.ToUpper(assessment.Status),
		assessment.ID, assessment.Component, assessment.Description, assessment.InstalledVersion)
	if assessment.ExtensionVersion != "" {
		line += ", extension " + assessment.ExtensionVersion
	}
	line += "; " + severity + ")"
	if assessment.Suppression != nil {
		line += " [suppressed: " + assessment.Suppression.Reason + "]"
	}
	evidence := assessment.Reason
	if assessment.AffectedRange != "" {
		evidence += "; affected " + assessment.AffectedRange
	}
	if assessment.FixedVersion != "" {
		evidence += "; fixed " + assessment.FixedVersion
	}
	_, err := fmt.Fprintf(w, "%s\n  %s\n  %s\n", line, evidence, assessment.SourceURL)
	return err
}
