package schema

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/schemasecurity"
)

const (
	securityDBURLFlag  = "db-url"
	securitySchemaFlag = "schemas"
	securityFormatFlag = "format"
	securityFailOnFlag = "fail-on"
)

// The three thresholds `ptah lint` already spells, so one operator's CI script
// does not need two vocabularies for the same idea.
const (
	securityFailOnError = "error"
	securityFailOnAny   = "any"
	securityFailOnNone  = "none"
)

type schemaSecurityOptions struct {
	dbURL   string
	schemas string
	format  string
	failOn  string
}

// NewSchemaSecurityCommand returns the native `schema security` command.
func NewSchemaSecurityCommand() *cobra.Command {
	return newSchemaSecurityCommand()
}

// newSchemaSecurityCommand implements `schema security`.
//
// # Why this is a verb rather than a lint rule
//
// Every analyzer in migration/lint runs over the statements a migration
// produces and answers "is this change safe". These rules run over the schema
// itself and answer "is this state safe": who holds which privilege, which
// tables are reachable with no row-level policy, which routines run as their
// owner. Different input, different question (stokaro/ptah#1035).
//
// # Local, and no account
//
// It connects to the database the operator names and to nothing else. There is
// no service, no upload and no account, so the findings can be read on a
// database that may never be reachable from the internet.
func newSchemaSecurityCommand() *cobra.Command {
	opts := schemaSecurityOptions{}
	cmd := &cobra.Command{
		Use:   "security",
		Short: "Report security findings over a live schema's roles, grants and policies",
		Long: `Read a live database and report security findings over what it declares:
privileges granted to PUBLIC, tables reachable with no row-level security, and
routines that run with their owner's privileges.

Each finding carries a code, a severity, the object it attaches to, the values
its suggestion names, and the suggestion itself. Severities are the ones the
rest of Ptah speaks: info reports and never blocks, warning asks for review,
error blocks.

A rule that cannot be answered on this target does not silently pass. It is
listed as skipped with its reason, because a rule that did not run is
indistinguishable from one that found nothing, and the difference is what makes
a clean report worth having.

The analysis is local: it reads the database named by --db-url and contacts
nothing else. It also reads only what Ptah models, so a clean report means
"nothing here matched a rule", never "this database is secure".`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaSecurity(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, securityDBURLFlag, "",
		"Database URL to analyze (required). Example: postgres://localhost:5432/dbname")
	flags.StringVar(&opts.schemas, securitySchemaFlag, "",
		"Comma-separated schemas to analyze (PostgreSQL-family only). Empty uses the connection default.")
	flags.StringVar(&opts.format, securityFormatFlag, "table", "Output format: table or json")
	flags.StringVar(&opts.failOn, securityFailOnFlag, securityFailOnError,
		"Failure threshold controlling the exit code: error, any or none")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runSchemaSecurity(cmd *cobra.Command, opts schemaSecurityOptions) error {
	if opts.dbURL == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--%s is required", securityDBURLFlag))
	}
	if opts.format != "table" && opts.format != "json" {
		return cmdutil.Fail(cmd, fmt.Errorf("--%s must be table or json, got %q",
			securityFormatFlag, opts.format))
	}
	if err := validateSecurityFailOn(opts.failOn); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.dbURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --%s: %w", securityDBURLFlag, err))
	}
	defer func() { _ = conn.Close() }()

	live, err := dbschema.ReadSchemaWithSchemas(conn, atlasschema.SplitSchemaNames([]string{opts.schemas}))
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("read schema: %w", err))
	}
	// The connection's own set rather than the dialect default: it is what the
	// session resolved, so a rule gated on a capability this server refines is
	// gated on what this server answered (stokaro/ptah#1230).
	report := schemasecurity.Analyze(
		dbschematogo.ConvertDBSchemaToGoSchema(live),
		schemasecurity.Options{
			Capabilities: conn.Info().Capabilities,
			// Non-nil even when the server has no memberships: this caller DID
			// read them, and the rules that need them must run rather than
			// report themselves skipped (stokaro/ptah#1950).
			RoleMemberships: roleMemberships(live),
		},
	)

	if err := writeSecurityReport(cmd.OutOrStdout(), opts.format, report); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write report: %w", err))
	}
	if securityReportFails(report, opts.failOn) {
		return exitcode.New(1, fmt.Errorf("security findings at or above --%s=%s",
			securityFailOnFlag, opts.failOn))
	}
	return nil
}

// roleMemberships carries the live role graph into the analysis.
//
// The conversion to a desired-state schema drops it, because Ptah does not
// model membership as a desired state -- it is a property of the cluster's role
// graph, and the analyzer takes it as its own input for exactly that reason.
func roleMemberships(live *dbschematypes.DBSchema) []schemasecurity.RoleMembership {
	memberships := make([]schemasecurity.RoleMembership, 0, len(live.RoleMemberships))
	for _, membership := range live.RoleMemberships {
		memberships = append(memberships, schemasecurity.RoleMembership{
			Role:        membership.Role,
			Member:      membership.Member,
			AdminOption: membership.AdminOption,
		})
	}
	return memberships
}

// validateSecurityFailOn rejects a threshold nothing understands, rather than
// treating it as the default and gating on something the operator did not ask
// for.
func validateSecurityFailOn(failOn string) error {
	switch failOn {
	case securityFailOnError, securityFailOnAny, securityFailOnNone:
		return nil
	default:
		return fmt.Errorf("--%s must be %s, %s or %s, got %q",
			securityFailOnFlag, securityFailOnError, securityFailOnAny, securityFailOnNone, failOn)
	}
}

// securityReportFails applies the threshold.
//
// No rule in this release is error-severity, so the default threshold reports
// and does not fail. That is deliberate rather than an oversight: the default
// is the one `ptah lint` uses, so a script that gates on errors gates the same
// way here the day a rule earns that severity.
func securityReportFails(report schemasecurity.Report, failOn string) bool {
	switch failOn {
	case securityFailOnAny:
		return len(report.Findings) > 0
	case securityFailOnError:
		return report.Summary.Error > 0
	default:
		return false
	}
}

func writeSecurityReport(w io.Writer, format string, report schemasecurity.Report) error {
	if format == "json" {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	return writeSecurityTable(w, report)
}

func writeSecurityTable(w io.Writer, report schemasecurity.Report) error {
	if err := writeSecurityFindings(w, report); err != nil {
		return err
	}
	return writeSecuritySkipped(w, report)
}

// writeSecurityFindings prints the findings, or says there were none.
func writeSecurityFindings(w io.Writer, report schemasecurity.Report) error {
	if len(report.Findings) == 0 {
		_, err := fmt.Fprintln(w, "No security findings.")
		return err
	}
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "CODE\tSEVERITY\tOBJECT\tFINDING"); err != nil {
		return err
	}
	for _, finding := range report.Findings {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s %s\t%s\n",
			finding.Code, finding.Severity, finding.Subject.Kind, finding.Subject.Name,
			finding.Message); err != nil {
			return err
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	// The suggestion is a sentence, and a sentence in a column wraps into the
	// column and stops being readable, so it goes under the table with its
	// code.
	if _, err := fmt.Fprintln(w, "\nSuggested:"); err != nil {
		return err
	}
	for _, finding := range report.Findings {
		if _, err := fmt.Fprintf(w, "  %s %s: %s\n",
			finding.Code, finding.Subject.Name, finding.Suggestion); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "\n%d error, %d warning, %d info\n",
		report.Summary.Error, report.Summary.Warning, report.Summary.Info)
	return err
}

// writeSecuritySkipped names the rules that did not run, so a clean report is
// distinguishable from an unasked question.
func writeSecuritySkipped(w io.Writer, report schemasecurity.Report) error {
	if len(report.SkippedRules) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "\nNot checked here:"); err != nil {
		return err
	}
	for _, skipped := range report.SkippedRules {
		if _, err := fmt.Fprintf(w, "  %s: %s\n", skipped.Code, skipped.Reason); err != nil {
			return err
		}
	}
	return nil
}
