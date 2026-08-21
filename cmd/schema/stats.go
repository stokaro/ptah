package schema

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/schemastats"
)

const (
	statsDBURLFlag  = "db-url"
	statsSchemaFlag = "schemas"
)

type schemaStatsOptions struct {
	dbURL   string
	schemas string
}

// NewSchemaStatsCommand returns the native `schema stats` command, so the
// Atlas-compatible surface can forward its own verb to the same body.
func NewSchemaStatsCommand() *cobra.Command {
	return newSchemaStatsCommand()
}

// newSchemaStatsCommand implements `schema stats`.
//
// # What it reports, and what it deliberately does not
//
// Every metric is a COUNT of objects Ptah's reader returns. There is no row
// count, no table size and no index bloat: those are properties of the DATA,
// and reading them means scanning or trusting the planner's statistics, which
// is a much more expensive promise than "describe the schema". A caller who
// wants them has the database's own statistics views (stokaro/ptah#1711).
//
// The counts are therefore exactly as complete as the reader that produced
// them. Where Ptah reads no triggers for a dialect the trigger count is zero,
// and that zero means "Ptah sees none here" -- the same thing an empty
// collection means everywhere else in this tool.
func newSchemaStatsCommand() *cobra.Command {
	opts := schemaStatsOptions{}
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Count the objects in a live schema and emit them as OpenMetrics",
		Long: `Read a live database and emit a count of each schema object kind in the
OpenMetrics text format, so an existing metrics pipeline can chart schema shape
over time.

Every metric is a count of objects Ptah's reader returns: tables, columns,
indexes, constraints, views, functions, sequences and the rest. Nothing here
describes the DATA — no row counts, no table sizes, no index bloat. Those are
properties of the contents rather than of the schema, and reading them means
scanning or trusting the planner's statistics; the database's own statistics
views answer that question better.

Output goes to stdout and ends with the OpenMetrics "# EOF" line, so a
collector can tell a complete scrape from a truncated one. Metrics are labelled
with the dialect and, when --schemas selects one, the schema, so a pipeline
reading several databases can tell them apart without a job per database.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaStats(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, statsDBURLFlag, "",
		"Database URL to read (required). Example: postgres://localhost:5432/dbname")
	flags.StringVar(&opts.schemas, statsSchemaFlag, "",
		"Comma-separated schemas to count (PostgreSQL-family only). Empty uses the connection default.")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runSchemaStats(cmd *cobra.Command, opts schemaStatsOptions) error {
	if opts.dbURL == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--%s is required", statsDBURLFlag))
	}
	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.dbURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --%s: %w", statsDBURLFlag, err))
	}
	defer func() { _ = conn.Close() }()

	live, err := dbschema.ReadSchemaWithSchemas(conn, atlasschema.SplitSchemaNames([]string{opts.schemas}))
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("read schema: %w", err))
	}
	stats := schemastats.Collect(dbschematogo.ConvertDBSchemaToGoSchema(live))
	labels := map[string]string{"dialect": conn.Info().Dialect}
	if opts.schemas != "" {
		labels["schemas"] = opts.schemas
	}
	if err := schemastats.WriteOpenMetrics(cmd.OutOrStdout(), stats, labels); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write metrics: %w", err))
	}
	return nil
}
