package dbverify

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/migration/migrator"
)

const (
	dbURLFlag  = "db-url"
	checksFlag = "checks"

	formatText = "text"
	formatJSON = "json"
)

type options struct {
	dbURL          string
	checks         string
	connectTimeout string
	format         string
}

// NewVerifyCommand returns the verb that evaluates release-state assertions
// against an existing database.
func NewVerifyCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Evaluate release-state assertions against a database, changing nothing",
		Long: `Evaluate release-state assertions against an existing database.

Each assertion is a ` + "`-- +ptah check`" + ` directive, the same one a migration
carries, read from a .sql file or a directory of them. Every assertion is a
single read-only SELECT returning one truthy scalar; that is proved from the
text before any query runs, and the session is opened read-only wherever the
engine has such a mode.

This verb changes nothing. It applies no schema, runs no migration, loads no
seed data and drops nothing, which is what separates it from the test runners:
those want a throwaway database, and this one is pointed at the database a
release actually runs on.

Four outcomes are reported and kept apart. An assertion that ran and held is
verified; one that ran and did not hold is failed; one that could not run --
malformed, write-shaped, or a query the server refused -- is errored, and
establishes nothing either way. A run that found no assertions at all is not
verified, and exits non-zero: an empty checks path must not read as a clean
release.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runVerify(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.StringVar(&opts.checks, checksFlag, "",
		"Path to a .sql file of `-- +ptah check` directives, or a directory of them (required)")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	flags.StringVar(&opts.format, "format", formatText, "Output format: text or json")
}

func runVerify(cmd *cobra.Command, opts *options) error {
	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if opts.checks == "" {
		return fmt.Errorf("--%s is required", checksFlag)
	}
	// Asked before the dial, like `db capabilities` does: a --format typo
	// resolved afterwards reports a connection failure for a target the
	// operator never meant to reach.
	if err := validateFormat(opts.format); err != nil {
		return err
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return err
	}
	defer dbschema.CloseAndWarn(conn)

	// The checks are parsed after the dial because the directive scanner needs
	// the target's lexer rules, and those come from the connection. A checks
	// file is therefore read by the rules of the engine it will be evaluated
	// against, never by a default that happens to agree.
	sourced, err := loadChecks(opts.checks, conn.Info().Dialect)
	if err != nil {
		return err
	}

	report, err := migrator.VerifyChecks(cmd.Context(), conn, checksOf(sourced))
	if err != nil {
		return err
	}

	if err := writeReport(cmd.OutOrStdout(), opts.format, sourced, report); err != nil {
		return err
	}
	return verdictError(report)
}

func checksOf(sourced []sourcedCheck) []migrator.Check {
	checks := make([]migrator.Check, 0, len(sourced))
	for _, entry := range sourced {
		checks = append(checks, entry.Check)
	}
	return checks
}

// verdictError turns a report into the command's exit status, and the code it
// picks is the difference between a finding and a fault.
//
// An assertion that ran and did not hold is the expected negative result this
// verb exists to produce, so it exits 1, beside drift and pending migrations.
// So does a run that found no assertions: nothing was established, and a
// release gate must not read that as a pass. An assertion that could not run is
// a fault in the input or the server -- a predicate that is not a well-formed
// read-only SELECT, or a query the database refused -- and exits 2 with the
// other command errors.
func verdictError(report migrator.VerifyReport) error {
	switch report.Verdict() {
	case migrator.VerifyVerdictVerified:
		return nil
	case migrator.VerifyVerdictErrored:
		return exitcode.New(2, fmt.Errorf(
			"verification could not complete: %d of %d assertions could not run",
			report.Count(migrator.VerifyStatusErrored), len(report.Results)))
	case migrator.VerifyVerdictFailed:
		return exitcode.New(1, fmt.Errorf(
			"verification failed: %d of %d assertions did not hold",
			report.Count(migrator.VerifyStatusFailed), len(report.Results)))
	default:
		return exitcode.New(1, errors.New(
			"nothing was verified: the checks path holds no `-- +ptah check` directives"))
	}
}

func validateFormat(format string) error {
	switch format {
	case formatText, formatJSON:
		return nil
	default:
		return fmt.Errorf("unsupported --format %q (want %s or %s)", format, formatText, formatJSON)
	}
}

// jsonResult is one assertion in the machine-readable report. Error carries the
// reason an errored assertion could not run, and is absent otherwise, so a
// reader never has to decide whether an empty string means "no error" or "an
// error nobody wrote down".
type jsonResult struct {
	Name   string `json:"name,omitempty"`
	Source string `json:"source"`
	Assert string `json:"assert"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type jsonReport struct {
	Verdict    string       `json:"verdict"`
	Verified   int          `json:"verified"`
	Failed     int          `json:"failed"`
	Errored    int          `json:"errored"`
	Assertions int          `json:"assertions"`
	Results    []jsonResult `json:"results"`
}

func writeReport(out io.Writer, format string, sourced []sourcedCheck, report migrator.VerifyReport) error {
	if format == formatJSON {
		return writeJSONReport(out, sourced, report)
	}
	return writeTextReport(out, sourced, report)
}

func writeJSONReport(out io.Writer, sourced []sourcedCheck, report migrator.VerifyReport) error {
	payload := jsonReport{
		Verdict:    string(report.Verdict()),
		Verified:   report.Count(migrator.VerifyStatusVerified),
		Failed:     report.Count(migrator.VerifyStatusFailed),
		Errored:    report.Count(migrator.VerifyStatusErrored),
		Assertions: len(report.Results),
		Results:    make([]jsonResult, 0, len(report.Results)),
	}
	for index, result := range report.Results {
		entry := jsonResult{
			Name:   result.Name,
			Source: sourceOf(sourced, index),
			Assert: result.Assert,
			Status: string(result.Status),
		}
		if result.Err != nil {
			entry.Error = result.Err.Error()
		}
		payload.Results = append(payload.Results, entry)
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(payload)
}

func writeTextReport(out io.Writer, sourced []sourcedCheck, report migrator.VerifyReport) error {
	if len(report.Results) == 0 {
		_, err := fmt.Fprintf(out, "Verdict: %s (no assertions found)\n", report.Verdict())
		return err
	}

	writer := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "STATUS\tNAME\tSOURCE"); err != nil {
		return err
	}
	for index, result := range report.Results {
		if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\n",
			result.Status, displayName(result.Name), sourceOf(sourced, index)); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	// The assert text and the reason live below the table rather than in it:
	// either can be long, and a column wide enough for them makes the status
	// of the other rows unreadable.
	for _, result := range report.Results {
		if result.Status == migrator.VerifyStatusVerified {
			continue
		}
		if _, err := fmt.Fprintf(out, "\n%s: %s\n  assert: %s\n",
			result.Status, displayName(result.Name), result.Assert); err != nil {
			return err
		}
		if result.Err == nil {
			continue
		}
		if _, err := fmt.Fprintf(out, "  reason: %v\n", result.Err); err != nil {
			return err
		}
	}

	_, err := fmt.Fprintf(out, "\nVerdict: %s (%d verified, %d failed, %d errored of %d)\n",
		report.Verdict(),
		report.Count(migrator.VerifyStatusVerified),
		report.Count(migrator.VerifyStatusFailed),
		report.Count(migrator.VerifyStatusErrored),
		len(report.Results))
	return err
}

func displayName(name string) string {
	if name == "" {
		return "(unnamed)"
	}
	return name
}

// sourceOf names the file a result came from. The report and the loaded checks
// are the same list in the same order -- VerifyChecks reports one result per
// check it was given, in order -- so the index is the join.
func sourceOf(sourced []sourcedCheck, index int) string {
	if index < 0 || index >= len(sourced) {
		return ""
	}
	return sourced[index].Source
}
