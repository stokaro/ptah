// Package dbvulnerabilities contains the native local advisory scan command.
package dbvulnerabilities

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/vulnerability"
)

type options struct {
	dbURL        string
	advisories   string
	suppressions string
	format       string
	failOn       string
	deployment   string
}

// NewCommand returns the native `db vulnerabilities` command.
func NewCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "vulnerabilities",
		Short: "Match a live database against local vulnerability advisories",
		Long: `Read a database's product and version, then match a pinned local advisory
snapshot. PostgreSQL core and selected bundled contrib modules are covered.
Unsupported products, unrecognized builds, stale advisories, and installed
extensions without a reviewed matcher are reported as incomplete. A managed
deployment is reported as not applicable to operator-managed server patching.
The command does not contact an advisory service or upload database inventory.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, "db-url", "", "Database URL to scan (required)")
	flags.StringVar(&opts.advisories, "advisories", "", "Local advisory snapshot JSON; empty uses the engine's pinned snapshot")
	flags.StringVar(&opts.suppressions, "suppressions", "", "Local JSON file of scoped advisory suppressions")
	flags.StringVar(&opts.format, "format", "table", "Output format: table or json")
	flags.StringVar(&opts.failOn, "fail-on", "both", "CI policy: findings, incomplete, both, or none")
	flags.StringVar(&opts.deployment, "deployment", "self-managed", "Server patch responsibility: self-managed or managed")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func run(cmd *cobra.Command, opts options) error {
	if opts.dbURL == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--db-url is required"))
	}
	if opts.format != "table" && opts.format != "json" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must be table or json"))
	}
	if opts.failOn != "findings" && opts.failOn != "incomplete" && opts.failOn != "both" && opts.failOn != "none" {
		return cmdutil.Fail(cmd, fmt.Errorf("--fail-on must be findings, incomplete, both, or none"))
	}
	if opts.deployment != "self-managed" && opts.deployment != "managed" {
		return cmdutil.Fail(cmd, fmt.Errorf("--deployment must be self-managed or managed"))
	}
	if err := vulnerability.ValidateAdvisoryInput(opts.advisories); err != nil {
		printFailure(cmd, opts.format, "invalid", "advisory snapshot is unavailable or invalid")
		return cmdutil.Fail(cmd, err)
	}
	now := time.Now().UTC()
	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.dbURL)
	if err != nil {
		printFailure(cmd, opts.format, "not_assessed", "database connection failed")
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --db-url: %w", err))
	}
	defer func() { _ = conn.Close() }()
	report, err := vulnerability.Scan(cmd.Context(), conn, opts.advisories, opts.suppressions, now, opts.deployment)
	if err != nil {
		var scanErr *vulnerability.ScanError
		if errors.As(err, &scanErr) && scanErr.Stage == "advisory snapshot" {
			printFailure(cmd, opts.format, "invalid", "advisory snapshot is unavailable or invalid")
		} else {
			printFailure(cmd, opts.format, "not_assessed", "database inventory or suppression file is unreadable")
		}
		return cmdutil.Fail(cmd, err)
	}
	if opts.format == "json" {
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("write vulnerability report: %w", err))
		}
	} else if err := writeTable(cmd.OutOrStdout(), report); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write vulnerability report: %w", err))
	}
	failFindings := opts.failOn == "findings" || opts.failOn == "both"
	failIncomplete := opts.failOn == "incomplete" || opts.failOn == "both"
	if (failFindings && report.HasFindings()) || (failIncomplete && report.IsIncomplete()) {
		return exitcode.New(1, fmt.Errorf("vulnerability scan meets --fail-on=%s", opts.failOn))
	}
	return nil
}

func printFailure(cmd *cobra.Command, format, status, message string) {
	if format != "json" {
		return
	}
	_ = json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
		Source struct {
			Status string `json:"status"`
		} `json:"source"`
		ScanError string `json:"scan_error"`
	}{Source: struct {
		Status string `json:"status"`
	}{Status: status}, ScanError: message})
}

func writeTable(w io.Writer, report vulnerability.Report) error {
	if _, err := fmt.Fprintf(w, "Product: %s\nServer: %s\nDeployment: %s (%s)\nAdvisories: %s (source SHA-256 %s; %d days old; %s)\nSnapshot SHA-256: %s\n",
		report.Inventory.Product, report.Inventory.ServerBanner, report.Deployment, report.Applicability, report.Source.URL,
		report.Source.SHA256, report.Source.AgeDays, report.Source.Status, report.Source.SnapshotSHA256); err != nil {
		return err
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
		severity := "not supplied"
		if assessment.CVSS != nil {
			severity = fmt.Sprintf("CVSS %.1f", *assessment.CVSS)
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
		if _, err := fmt.Fprintf(w, "%s\n  %s\n  %s\n", line, evidence, assessment.SourceURL); err != nil {
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
