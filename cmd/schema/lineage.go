package schema

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/serverversion"
	"go.5x5.cz/ptah/internal/schemalineage"
	"go.5x5.cz/ptah/internal/schemaload"
)

const (
	lineageRootDirFlag    = "root-dir"
	lineageSchemaFileFlag = "schema-file"
	lineageDialectFlag    = "dialect"
	lineageFormatFlag     = "format"
)

type schemaLineageOptions struct {
	rootDirs      []string
	schemaFiles   []string
	dialect       string
	serverVersion string
	format        string
	plainHTTP     bool
	configPath    string
	envName       string
}

// NewSchemaLineageCommand returns the native `schema lineage` command.
func NewSchemaLineageCommand() *cobra.Command {
	return newSchemaLineageCommand()
}

func newSchemaLineageCommand() *cobra.Command {
	opts := schemaLineageOptions{}
	cmd := &cobra.Command{
		Use:   "lineage",
		Short: "Trace which base columns feed each view column",
		Long: `Derive column-to-column dependencies from the view and materialized-view
bodies a schema declares, and write them as data.

This answers "what breaks if I drop this column" before the drop rather than
after: a view column resolves to the base columns it reads, so a column nothing
reads is visibly distinct from one three views depend on.

The analysis is static and local. It reads the schema Ptah already models and
contacts nothing.

A view body this cannot fully resolve -- a join, a subquery source, a computed
column with no alias -- is reported under "undecided" rather than omitted. A
view whose dependencies went unresolved must not look like a view with none,
because the difference decides whether the answer can be trusted.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaLineage(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.rootDirs, lineageRootDirFlag, nil,
		"Directory to scan for annotated Go entities (repeatable)")
	flags.StringArrayVar(&opts.schemaFiles, lineageSchemaFileFlag, nil,
		"Schema file to read (repeatable)")
	flags.StringVar(&opts.dialect, lineageDialectFlag, "postgres",
		"Dialect the schema is read for")
	serverversion.Register(flags, &opts.serverVersion)
	flags.StringVar(&opts.format, lineageFormatFlag, "table",
		"Output format: table or json")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runSchemaLineage(cmd *cobra.Command, opts schemaLineageOptions) error {
	if opts.format != "table" && opts.format != "json" {
		return cmdutil.Fail(cmd, fmt.Errorf("--%s must be table or json, got %q",
			lineageFormatFlag, opts.format))
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	schemaSourceEnv, err := dbcli.SchemaSourceProjectEnv(cmd, projectCfg)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	database, err := schemaload.LoadContext(cmd.Context(), schemaload.Options{
		RootDirs:        opts.rootDirs,
		SchemaFiles:     opts.schemaFiles,
		ProjectEnv:      schemaSourceEnv,
		EnvSelectorFlag: dbcli.SchemaSourceEnvSelectorFlag(cmd),
		Dialect:         opts.dialect,
		PlainHTTP:       opts.plainHTTP,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	result := schemalineage.Derive(database)
	if opts.format == "json" {
		return writeLineageJSON(cmd.OutOrStdout(), result)
	}
	return writeLineageTable(cmd.OutOrStdout(), result)
}

func writeLineageJSON(w io.Writer, result schemalineage.Result) error {
	// Edges is never null in the document: a consumer iterating it should not
	// have to special-case "no lineage" differently from "no views".
	if result.Edges == nil {
		result.Edges = make([]schemalineage.Edge, 0)
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func writeLineageTable(w io.Writer, result schemalineage.Result) error {
	if len(result.Edges) == 0 && len(result.Undecided) == 0 {
		_, err := fmt.Fprintln(w, "No view columns to trace.")
		return err
	}
	if len(result.Edges) > 0 {
		table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(table, "SOURCE\tFEEDS\tKIND"); err != nil {
			return err
		}
		for _, edge := range result.Edges {
			kind := "view"
			if edge.Materialized {
				kind = "materialized"
			}
			if _, err := fmt.Fprintf(table, "%s.%s\t%s.%s\t%s\n",
				edge.FromTable, edge.FromColumn, edge.ToView, edge.ToColumn, kind); err != nil {
				return err
			}
		}
		if err := table.Flush(); err != nil {
			return err
		}
	}
	if len(result.Undecided) == 0 {
		return nil
	}
	// Undecided is reported on the same channel as the edges, not as a warning
	// on stderr: it is part of the answer, and a reader who sees only the edges
	// would take an incomplete trace for a complete one.
	if _, err := fmt.Fprintf(w, "\n%d view(s) not fully resolved:\n", len(result.Undecided)); err != nil {
		return err
	}
	for _, undecided := range result.Undecided {
		if _, err := fmt.Fprintf(w, "  %s: %s\n", undecided.View, undecided.Reason); err != nil {
			return err
		}
	}
	return nil
}
