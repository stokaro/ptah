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
	"ptah.run/internal/advisorysource"
	"ptah.run/internal/cli/internal/cmdadapter"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/vulnerability"
)

type options struct {
	dbURLs         []string
	advisories     string
	advisorySource string
	suppressions   string
	format         string
	failOn         string
	deployment     string
	minSeverity    string
	failSeverity   string
	ignore         []string
}

// NewCommand returns the native `db vulnerabilities` command.
func NewCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "vulnerabilities",
		Short: "Match live databases against reviewed vulnerability advisories",
		Long: `Read each database's product and version, then match a reviewed advisory
snapshot. PostgreSQL core and selected bundled contrib modules, MySQL, MariaDB
and ClickHouse servers are covered. Unsupported products, unrecognized builds,
stale advisories, partial snapshots, and installed extensions without a
reviewed matcher are reported as incomplete. A target that cannot be read is
reported with a scan error, never as clean. A managed deployment is reported
as not applicable to operator-managed server patching.

The snapshot is read from --advisories when it is set. Otherwise the command
makes one outbound HTTPS GET per engine it scans, to --advisory-source, which
by default is the reviewed snapshot files on the master branch of Ptah's
repository, and keeps the copy in the user cache directory for an hour. The
request names only the snapshot file, so the source learns which engines are
scanned and nothing else about the databases. A copy is used when it passes
the checks the snapshot built into this binary passes, is no older than the
copy it replaces, and is dated no more than a day after this machine's clock.
Each advisory is matched against both the copy and the built-in snapshot, and
the stricter answer is reported: a copy can add a finding, but a removal, a
narrowed range or wider coverage waits for a release. A request has 15 seconds.
When the source gives no usable copy, the cached copy or the built-in snapshot
is read instead, the failure is remembered for an hour, and the report says
why. A run that names its source,
with --advisory-source or PTAH_ADVISORY_SOURCE, also counts that as
incomplete; a default run does not. --advisory-source bundled reads only the
built-in snapshots and makes no request, so a network with no route to the
source does not wait 5 seconds to find that out.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, opts)
		},
	}
	severities := strings.Join(vulnerability.SeverityNames(), ", ")
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.dbURLs, dbURLFlag, nil, "Database URL to scan; repeat to scan several databases (required)")
	flags.StringVar(&opts.advisories, "advisories", "", "Local advisory snapshot JSON for every target; empty reads --advisory-source")
	flags.StringVar(&opts.advisorySource, "advisory-source", advisorysource.Repository,
		"Where reviewed advisory snapshots come from: repository, the files at "+advisorysource.DefaultLocation+
			", fetched over HTTPS by engine file name only; the https URL of a mirror serving the same files; or bundled, for the snapshots built into this binary")
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

// dbURLFlag is the flag that names a database to scan.
const dbURLFlag = "db-url"

// dbURLFlagAsTyped names --db-url the way the operator spelled it:
// ptah-compat's `security scan` forwards its -u/--url here, and a message
// naming --db-url there names a flag that surface refuses. Only the connect
// error needs it: that surface refuses an absent or empty URL itself, before
// it forwards.
func dbURLFlagAsTyped(cmd *cobra.Command) string {
	if cmdadapter.Forwarded(cmd) {
		return "url"
	}
	return dbURLFlag
}

func run(cmd *cobra.Command, opts options) error {
	policy, err := validate(opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	location, err := advisorysource.ParseLocation(opts.advisorySource)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("--advisory-source: %w", err))
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
	snapshots := advisorysource.New(advisorysource.Config{Location: location, CacheDir: advisorysource.DefaultCacheDir()})
	targets := make([]targetReport, 0, len(opts.dbURLs))
	var failures []error
	for _, dbURL := range opts.dbURLs {
		target, err := scan(cmd, dbURL, opts, scanInput{policy: policy, snapshots: snapshots, now: now,
			// The flag or PTAH_ADVISORY_SOURCE named the source, which a default
			// run does not; Changed answers for both.
			requireSource: cmd.Flags().Changed("advisory-source")})
		targets = append(targets, target)
		if err != nil {
			failures = append(failures, err)
		}
	}
	if err := warn(cmd.ErrOrStderr(), targets); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write vulnerability warnings: %w", err))
	}
	if err := write(cmd.OutOrStdout(), opts.format, targets); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write vulnerability report: %w", err))
	}
	if len(failures) != 0 {
		return cmdutil.Fail(cmd, errors.Join(failures...))
	}
	for _, target := range targets {
		if reason := policy.Failure(target.Report); reason != "" {
			return exitcode.New(1, fmt.Errorf("vulnerability scan meets --fail-on=%s: %s has %s", opts.failOn, target.Target, reason))
		}
	}
	return nil
}

func validate(opts options) (vulnerability.Policy, error) {
	if len(opts.dbURLs) == 0 || slices.Contains(opts.dbURLs, "") {
		return vulnerability.Policy{}, errors.New("--" + dbURLFlag + " is required and must not be empty")
	}
	if opts.format != "table" && opts.format != "json" {
		return vulnerability.Policy{}, errors.New("--format must be table or json")
	}
	failOn, err := vulnerability.ParseFailOn(opts.failOn)
	if err != nil {
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
	policy := vulnerability.Policy{MinSeverity: minSeverity, FailSeverity: failSeverity, Ignore: opts.ignore, FailOn: failOn}
	if err := policy.Validate(); err != nil {
		return vulnerability.Policy{}, fmt.Errorf("--ignore: %w", err)
	}
	return policy, nil
}

// scanInput is what every target of one run shares.
type scanInput struct {
	policy        vulnerability.Policy
	snapshots     vulnerability.SnapshotSource
	now           time.Time
	requireSource bool
}

// scan assesses one target. A failure is returned with the target's report so
// the other targets are still scanned and every one is written.
func scan(cmd *cobra.Command, dbURL string, opts options, input scanInput) (targetReport, error) {
	conn, err := dbschema.ConnectToDatabase(cmd.Context(), dbURL)
	if err != nil {
		return failedTarget(dbURL, vulnerability.Inventory{}, "not_assessed", "database connection failed"),
			dburldisplay.RedactError(dbURL, fmt.Errorf("connect to --%s %s: %w", dbURLFlagAsTyped(cmd), dburldisplay.Format(dbURL), err))
	}
	defer func() { _ = conn.Close() }()
	report, err := vulnerability.Scan(cmd.Context(), conn, vulnerability.ScanInput{AdvisoryPath: opts.advisories,
		SuppressionPath: opts.suppressions, Snapshots: input.snapshots, RequireSource: input.requireSource,
		Now: input.now, Deployment: opts.deployment})
	if err != nil {
		inventory := vulnerability.CollectIdentity(conn)
		var scanErr *vulnerability.ScanError
		if !errors.As(err, &scanErr) {
			scanErr = new(vulnerability.ScanError)
		}
		target := failedTarget(dbURL, inventory, "not_assessed", "database inventory or suppression file is unreadable")
		if scanErr.Stage == "advisory snapshot" {
			target = failedTarget(dbURL, inventory, "invalid", "advisory snapshot is unavailable or invalid")
		}
		// A failure after the snapshot was selected keeps where it came from,
		// so warn still says when the advisory source was not used.
		target.Source.Origin = scanErr.Origin
		return target, err
	}
	return targetReport{Target: dburldisplay.Format(dbURL), Report: input.policy.Apply(report)}, nil
}

// warn drops each target's warnings about a product another target of the
// run reports, since the entry they name is that target's, and writes the
// rest to w once each. The reports keep the warnings that remain. It also
// writes, once per file, why the advisory source was not used and which
// snapshot was read instead.
func warn(w io.Writer, targets []targetReport) error {
	reported := make([]string, 0, len(targets))
	for _, target := range targets {
		reported = append(reported, target.Inventory.Product)
	}
	written := make([]string, 0)
	for _, target := range targets {
		origin := target.Source.Origin
		if origin.Fallback == "" || slices.Contains(written, origin.URL) {
			continue
		}
		written = append(written, origin.URL)
		if _, err := io.WriteString(w, vulnerability.FallbackWarning(origin, target.Inventory.Product)); err != nil {
			return err
		}
	}
	for index := range targets {
		targets[index].Warnings = vulnerability.UnclaimedWarnings(targets[index].Warnings, reported)
		for _, warning := range targets[index].Warnings {
			if slices.Contains(written, warning.Message) {
				continue
			}
			written = append(written, warning.Message)
			if _, err := fmt.Fprintf(w, "warning: %s\n", warning.Message); err != nil {
				return err
			}
		}
	}
	return nil
}

// originLine says where a report's snapshot came from, or nothing when the
// report read none.
func originLine(origin vulnerability.Origin) string {
	switch {
	case origin.Kind == "remote" && origin.Fallback != "":
		return fmt.Sprintf("remote %s (cached copy confirmed by the source at %s; unconfirmed, because the source was not used: %s)",
			origin.URL, origin.FetchedAt.UTC().Format(vulnerability.OriginTimeLayout), origin.Fallback)
	case origin.Kind == "remote":
		return fmt.Sprintf("remote %s (confirmed by the source at %s; %s)", origin.URL, origin.FetchedAt.UTC().Format(vulnerability.OriginTimeLayout), origin.Cache)
	case origin.Kind == "bundled" && origin.Fallback != "":
		return fmt.Sprintf("bundled, because %s was not used: %s", origin.URL, origin.Fallback)
	case origin.Kind == "file":
		return "the --advisories file"
	default:
		return origin.Kind
	}
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
	for _, page := range report.Source.AdditionalPages {
		if _, err := fmt.Fprintf(w, "Advisories: %s (source SHA-256 %s)\n", page.URL, page.SHA256); err != nil {
			return err
		}
	}
	if line := originLine(report.Source.Origin); line != "" {
		if _, err := fmt.Fprintf(w, "Advisory data: %s\n", line); err != nil {
			return err
		}
	}
	for _, warning := range report.Warnings {
		if _, err := fmt.Fprintf(w, "WARNING %s\n", warning.Message); err != nil {
			return err
		}
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
