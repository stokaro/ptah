package schema

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"ptah.run/cmd/internal/cmdutil"
	"ptah.run/cmd/internal/dbcli"
	"ptah.run/cmd/internal/serverversion"
	"ptah.run/dbschema"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/schemalineage"
	"ptah.run/internal/schemaload"
)

const (
	lineageRootDirFlag    = "root-dir"
	lineageSchemaFileFlag = "schema-file"
	lineageDialectFlag    = "dialect"
	lineageFormatFlag     = "format"
	lineageDBURLFlag      = "db-url"
)

type schemaLineageOptions struct {
	rootDirs      []string
	schemaFiles   []string
	dbURL         string
	schemas       string
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
		Short: "Trace which base columns feed each view column and each routine",
		Long: `Derive column-to-column dependencies from the view and materialized-view
bodies a schema declares, from the routine bodies it can resolve, and the
tables and columns those routines write, and write them as data.

This answers "what breaks if I drop this column" before the drop rather than
after: a view column resolves to the base columns it reads, so a column nothing
reads is visibly distinct from one three views depend on.

The analysis is static. Without --db-url it reads a declared schema and contacts
nothing; with --db-url it reads the live schema and traces that, which is how
the same question -- what breaks if I drop this column -- is asked about a
database nobody has a declaration for.

A view body this cannot fully resolve -- a join, a subquery source, a computed
column with no alias -- is reported under "undecided" rather than omitted. A
view whose dependencies went unresolved must not look like a view with none,
because the difference decides whether the answer can be trusted.

Routines are traced on the same terms, and the same boundary applies: a routine
whose body is a single SELECT resolves to the columns it reads. A procedural
body is resolved for what it writes -- the tables and columns its INSERT,
UPDATE, DELETE and TRUNCATE statements name -- and is always reported as
undecided as well, because its reads are not derived and a silent routine would
read as one that reads nothing.`,
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
	flags.StringVar(&opts.dbURL, lineageDBURLFlag, "",
		"Trace the schema of a live database instead of a schema source")
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
	serverversion.Register(flags, &opts.serverVersion)
	flags.StringVar(&opts.format, lineageFormatFlag, "table",
		"Output format: table, json, or dot for a Graphviz digraph")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runSchemaLineage(cmd *cobra.Command, opts schemaLineageOptions) error {
	if !lineageFormatIsKnown(opts.format) {
		return cmdutil.Fail(cmd, fmt.Errorf("--%s must be table, json or dot, got %q",
			lineageFormatFlag, opts.format))
	}
	if opts.dbURL != "" {
		return runSchemaLineageLive(cmd, opts)
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
	document := lineageDocument{
		Result:   schemalineage.Derive(database),
		Routines: schemalineage.DeriveRoutines(database, opts.dialect),
	}
	return writeLineage(cmd.OutOrStdout(), opts.format, document)
}

// lineageFormatIsKnown reports whether the flag names a format this command
// writes.
//
// It is a list rather than a chain of comparisons because the validation and
// the dispatch below must agree: a format accepted here and unhandled there
// would fall through to the table, which is the quiet way a new format ships
// as a synonym for the old one.
func lineageFormatIsKnown(format string) bool {
	switch format {
	case "table", "json", "dot":
		return true
	default:
		return false
	}
}

// writeLineage renders the document in the format the operator named.
//
// One dispatcher for both the file-backed and the live path. They used to
// branch on the format separately, which is how a third format reaches one of
// them and not the other -- the same shape as a fix landing on only the branch
// an issue happened to name.
func writeLineage(w io.Writer, format string, document lineageDocument) error {
	switch format {
	case "json":
		return writeLineageJSON(w, document)
	case "dot":
		return writeLineageDOT(w, document)
	default:
		return writeLineageTable(w, document)
	}
}

// lineageDocument is what the command writes.
//
// [schemalineage.Result] is embedded rather than nested so its "edges" and
// "undecided" keys stay where they have always been: routine lineage is a key
// this document gained, not a rename of the keys a reader already parses.
type lineageDocument struct {
	schemalineage.Result
	Routines schemalineage.RoutineResult `json:"routines"`
}

// runSchemaLineage traces the schema of a live database.
//
// The same derivation over a schema read back from the server rather than one
// declared in a file. It is the only route by which this analysis reaches a
// database at all, which is what made integration coverage of it structurally
// impossible before (stokaro/ptah#1270, criterion 8).
func runSchemaLineageLive(cmd *cobra.Command, opts schemaLineageOptions) error {
	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.dbURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to database: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	live, err := dbschema.ReadSchemaWithSchemasContext(
		cmd.Context(), conn, dbcli.ParseSchemas(opts.schemas),
	)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("read database schema: %w", err))
	}
	// The dialect the server reports rather than the flag: a lineage traced
	// against a live database is about that database, and a routine body is
	// read by its own engine's parser.
	dialect := conn.Info().Dialect
	database := dbschematogo.ConvertDBSchemaToGoSchema(live, conn.Info().Dialect)
	document := lineageDocument{
		Result:   schemalineage.Derive(database),
		Routines: schemalineage.DeriveRoutines(database, dialect),
	}
	return writeLineage(cmd.OutOrStdout(), opts.format, document)
}

func writeLineageJSON(w io.Writer, document lineageDocument) error {
	// Neither edge list is ever null in the document: a consumer iterating one
	// should not have to special-case "no lineage" differently from "no views".
	if document.Edges == nil {
		document.Edges = make([]schemalineage.Edge, 0)
	}
	if document.Routines.Edges == nil {
		document.Routines.Edges = make([]schemalineage.RoutineEdge, 0)
	}
	if document.Routines.Writes == nil {
		document.Routines.Writes = make([]schemalineage.RoutineWrite, 0)
	}
	if document.Routines.Reads == nil {
		document.Routines.Reads = make([]schemalineage.RoutineRead, 0)
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(document)
}

func writeLineageTable(w io.Writer, document lineageDocument) error {
	result, routines := document.Result, document.Routines
	if len(result.Edges) == 0 && len(result.Undecided) == 0 &&
		len(routines.Edges) == 0 && len(routines.Reads) == 0 && len(routines.Writes) == 0 &&
		len(routines.Undecided) == 0 {
		_, err := fmt.Fprintln(w, "No view or routine columns to trace.")
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
	if len(routines.Edges) > 0 {
		table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(table, "SOURCE\tREAD BY\tKIND"); err != nil {
			return err
		}
		for _, edge := range routines.Edges {
			if _, err := fmt.Fprintf(table, "%s.%s\t%s\t%s\n",
				edge.FromTable, edge.FromColumn, edge.ToRoutine, edge.Kind); err != nil {
				return err
			}
		}
		if err := table.Flush(); err != nil {
			return err
		}
	}
	if err := writeLineageRoutineReads(w, routines); err != nil {
		return err
	}
	if err := writeLineageRoutineWrites(w, routines); err != nil {
		return err
	}
	if err := writeLineageUndecidedRoutines(w, routines); err != nil {
		return err
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

// writeLineageUndecidedRoutines reports the routines that did not resolve.
//
// Same channel as the edges, for the reason the view half gives: a reader who
// saw only what resolved would take an incomplete trace for a complete one.
func writeLineageUndecidedRoutines(w io.Writer, routines schemalineage.RoutineResult) error {
	if len(routines.Undecided) == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(w, "\n%d routine(s) not fully resolved:\n", len(routines.Undecided)); err != nil {
		return err
	}
	for _, undecided := range routines.Undecided {
		if _, err := fmt.Fprintf(w, "  %s: %s\n", undecided.Routine, undecided.Reason); err != nil {
			return err
		}
	}
	return nil
}

// writeLineageRoutineWrites reports the tables and columns routines write.
//
// A write is reported on the same channel as the reads, in its own table: the
// two answer different questions -- "what breaks if I drop this column" and
// "what changes this column" -- and merging them into one list would make
// neither readable.
func writeLineageRoutineWrites(w io.Writer, routines schemalineage.RoutineResult) error {
	if len(routines.Writes) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "TARGET\tWRITTEN BY\tSTATEMENT"); err != nil {
		return err
	}
	for _, write := range routines.Writes {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\n",
			writeTargetName(write), write.ByRoutine, write.Statement); err != nil {
			return err
		}
	}
	return table.Flush()
}

// writeTargetName renders a write target, naming the whole table where the
// statement named no column.
func writeTargetName(write schemalineage.RoutineWrite) string {
	if write.Column == "" {
		return write.Table
	}
	return write.Table + "." + write.Column
}

// writeLineageRoutineReads reports the columns routine bodies read.
//
// Its own table beside the writes: "what breaks if I drop this column" and
// "what changes this column" are different questions, and one merged list would
// answer neither cleanly.
func writeLineageRoutineReads(w io.Writer, routines schemalineage.RoutineResult) error {
	if len(routines.Reads) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "SOURCE\tREAD BY\tSTATEMENT"); err != nil {
		return err
	}
	for _, read := range routines.Reads {
		if _, err := fmt.Fprintf(table, "%s.%s\t%s\t%s\n",
			read.Table, read.Column, read.ByRoutine, read.Statement); err != nil {
			return err
		}
	}
	return table.Flush()
}
