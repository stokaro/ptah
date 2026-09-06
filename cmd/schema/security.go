package schema

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"ptah.run/catalog"
	"ptah.run/cmd/internal/cmdutil"
	"ptah.run/cmd/internal/exitcode"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/schemasecurity"
)

const (
	securityDBURLFlag  = "db-url"
	securitySchemaFlag = "schemas"
	securityFormatFlag = "format"
	securityFailOnFlag = "fail-on"
	// securityRoleUsageFlag names the file carrying the usage signal ROL01
	// needs. Collection is deliberately outside Ptah: no catalog records which
	// roles read which objects, so the observation comes from whatever did see
	// it -- pg_stat_statements, an audit stream, a proxy log
	// (stokaro/ptah#1961).
	securityRoleUsageFlag = "role-usage"
)

// The three thresholds `ptah lint` already spells, so one operator's CI script
// does not need two vocabularies for the same idea.
const (
	securityFailOnError = "error"
	securityFailOnAny   = "any"
	securityFailOnNone  = "none"
)

type schemaSecurityOptions struct {
	dbURL     string
	schemas   string
	format    string
	failOn    string
	roleUsage string
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

ROL01 -- a role holding privileges on objects it never uses -- cannot be
answered from a catalog: a privilege is not use, and no engine Ptah reads
records which roles read which objects. Supply --role-usage with a JSON file of
observations to run it:

  [{"role": "reporting", "kind": "table", "name": "orders"}]

Without the flag ROL01 is reported as skipped rather than passing quietly. An
empty list is a different answer from no file at all: it says the window was
observed and nothing used anything, so every grant in it is unused.

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
	flags.StringVar(&opts.roleUsage, securityRoleUsageFlag, "",
		"JSON file of observed role-object usage, which ROL01 needs. Omitted, ROL01 reports itself skipped.")
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

	// Read before connecting: a malformed usage file should fail before the
	// command opens a database connection to analyze.
	usage, err := readRoleUsage(opts.roleUsage)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.dbURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --%s: %w", securityDBURLFlag, err))
	}
	defer func() { _ = conn.Close() }()

	live, err := dbschema.ReadSchemaWithSchemasContext(cmd.Context(), conn, atlasschema.SplitSchemaNames([]string{opts.schemas}))
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("read schema: %w", err))
	}
	// The connection's own set rather than the dialect default: it is what the
	// session resolved, so a rule gated on a capability this server refines is
	// gated on what this server answered (stokaro/ptah#1230).
	report := schemasecurity.Analyze(
		dbschematogo.ConvertDBSchemaToGoSchema(live, conn.Info().Dialect),
		schemasecurity.Options{
			Capabilities: conn.Info().Capabilities,
			// Non-nil even when the server has no memberships: this caller DID
			// read them, and the rules that need them must run rather than
			// report themselves skipped (stokaro/ptah#1950).
			RoleMemberships: roleMemberships(live),
			ObjectOwners:    objectOwners(live),
			RoleObjectUsage: usage,
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
func roleMemberships(live *catalog.Database) []schemasecurity.RoleMembership {
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

// objectOwners carries who owns what into the analysis, with the owner's login
// flag as the catalog reported it.
func objectOwners(live *catalog.Database) []schemasecurity.ObjectOwner {
	owners := make([]schemasecurity.ObjectOwner, 0, len(live.ObjectOwners))
	for _, owner := range live.ObjectOwners {
		owners = append(owners, schemasecurity.ObjectOwner{
			Kind:          owner.Kind,
			Name:          owner.Name,
			Owner:         owner.Owner,
			OwnerCanLogin: owner.OwnerCanLogin,
		})
	}
	return owners
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

// readRoleUsage reads the observations ROL01 runs over, or nil when no file was
// named.
//
// nil and an empty slice are deliberately different returns: no file means the
// caller collected nothing and ROL01 must report itself skipped, while a file
// holding `[]` means the caller observed a window in which nothing was used,
// and every grant in it is then unused. Collapsing them would make the rule
// either permanently silent or permanently wrong.
func readRoleUsage(path string) ([]schemasecurity.RoleObjectUsage, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path) // #nosec G304 -- the operator named this file
	if err != nil {
		return nil, fmt.Errorf("read --%s: %w", securityRoleUsageFlag, err)
	}
	usage := make([]schemasecurity.RoleObjectUsage, 0)
	if err := json.Unmarshal(data, &usage); err != nil {
		return nil, fmt.Errorf("read --%s: %w", securityRoleUsageFlag, err)
	}
	for index, observation := range usage {
		if strings.TrimSpace(observation.Role) == "" || strings.TrimSpace(observation.Name) == "" {
			return nil, fmt.Errorf(
				"read --%s: observation %d has no role or no name, so it names no use",
				securityRoleUsageFlag, index)
		}
	}
	return usage, nil
}
