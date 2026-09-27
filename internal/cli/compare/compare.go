// Package compare implements "ptah schema compare", which builds the desired
// schema from Go entities or schema files and reports how it differs from a
// live database.
package compare

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/config/projectconfig"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/diffreport"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/dbexprprobe"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/genexprprobe"
	"ptah.run/internal/schemaload"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/internal/undecidednote"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

const (
	rootDirFlag      = "root-dir"
	schemaFileFlag   = "schema-file"
	schemaCmdFlag    = "schema-cmd"
	schemaFormatFlag = "schema-format"
	dbURLFlag        = "db-url"
	devURLFlag       = "dev-url"
	exitCodeFlag     = "exit-code"
	plainHTTPFlag    = "plain-http"
)

type options struct {
	rootDirs         []string
	schemaFiles      []string
	schemaCmd        string
	schemaFormat     string
	dbURL            string
	devURL           string
	exitOnDiff       bool
	connectTimeout   string
	schemas          string
	plainHTTP        bool
	ignoreExtensions []string
}

func NewCompareCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare a desired schema with a live database",
		Long: `Compare a desired schema with a live database.

The desired schema may come from repeatable SQL, YAML, HCL, or DBML files,
directories of Go annotations, an OCI schema artifact, or an external program.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return compareCommand(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.rootDirs, rootDirFlag, nil, "Root directory to scan for Go entities (repeatable; multiple roots merge into one composite schema; defaults to ./)")
	flags.StringArrayVar(&opts.schemaFiles, schemaFileFlag, nil, "SQL, YAML, HCL, DBML, or OCI desired-schema source (repeatable; combines with other sources)")
	flags.StringVar(&opts.schemaCmd, schemaCmdFlag, "", `External program whose stdout is the desired schema; run without a shell, split on whitespace. Example: "go run ./loader"`)
	flags.StringVar(&opts.schemaFormat, schemaFormatFlag, "sql", "Format of the --schema-cmd output: sql, hcl, or yaml")
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.StringVar(&opts.devURL, devURLFlag, "", "Dev database URL, used to ask the target engine how it spells a declared generated-column expression. Only Oracle needs one; every other engine stores the expression it was given")
	flags.BoolVar(&opts.exitOnDiff, exitCodeFlag, false, "Exit with 1 when the schema diff is non-empty or a declared object could not be decided")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	flags.String(dbcli.ConfigFlagName, "", "Path to a ptah.yaml config file (default: ./ptah.yaml when present)")
	dbcli.RegisterProjectEnvFlag(flags)
	dbcli.RegisterExternalSchemaOptInFlag(flags)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
	dbcli.RegisterIgnoreExtensionFlag(flags, &opts.ignoreExtensions)
}

func compareCommand(cmd *cobra.Command, opts *options) error {
	out := cmd.OutOrStdout()

	if err := sqlitevirtual.ValidateExplicitURLToggle(opts.dbURL); err != nil {
		return err
	}
	configPath, err := cmd.Flags().GetString(dbcli.ConfigFlagName)
	if err != nil {
		return err
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, configPath)
	if err != nil {
		return err
	}
	commands, err := dbcli.ResolveExternalSchemaCommands(
		cmd,
		opts.schemaCmd,
		opts.schemaFormat,
		projectCfg,
	)
	if err != nil {
		return err
	}
	dbURL := dbcli.EffectiveString(
		cmd,
		dbURLFlag,
		opts.dbURL,
		projectCfg.StringValue(projectconfig.StringDatabaseURL),
	)
	schemasValue := dbcli.EffectiveString(
		cmd,
		dbcli.SchemasFlagName,
		opts.schemas,
		dbcli.JoinSchemasValue(projectCfg.SchemasValue()),
	)

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	dialect, err := atlasurl.DialectFromURL(dbURL)
	if err != nil {
		return err
	}
	if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
		return err
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}

	declaredVars, err := dbcli.DeclaredVars(cmd)
	if err != nil {
		return err
	}

	loadOpts := schemaload.Options{
		RootDirs:    opts.rootDirs,
		SchemaFiles: opts.schemaFiles,
		Commands:    commands,
		Dialect:     dialect,
		PlainHTTP:   opts.plainHTTP,
		Vars:        declaredVars,
	}

	fmt.Fprintf(out, "Comparing schema from %s with database %s\n", loadOpts.Sources(), dburldisplay.Format(dbURL))
	fmt.Fprintln(out, "=== SCHEMA COMPARISON ===")
	fmt.Fprintln(out)

	// 1. Resolve the desired schema from Go entities, schema files, and/or an
	// external command into one composite schema.
	result, err := schemaload.LoadContext(cmd.Context(), loadOpts)
	if err != nil {
		return err
	}

	// 2. Connect to database and read schema
	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	schemas := dbcli.ParseSchemas(schemasValue)
	dbSchema, err := dbschema.ReadSchemaWithSchemasContext(cmd.Context(), conn, schemas)
	if err != nil {
		return fmt.Errorf("error reading database schema: %w", err)
	}

	// 3. Compare schemas (dialect-aware: MySQL/MariaDB RESTRICT == NO ACTION)
	info := conn.Info()
	compareOpts, err := resolveGeneratedExpressions(cmd.Context(), opts, connectTimeout, info, result)
	if err != nil {
		return err
	}
	compareOpts = dbcli.CompareOptionsIgnoringExtensions(cmd, opts.ignoreExtensions, projectCfg, compareOpts)
	diff, undecided, err := schemadiff.CompareWithDatabaseReportingUndecidedAdditions(
		cmd.Context(), conn, result, dbSchema, compareOpts,
	)
	if err != nil {
		return fmt.Errorf("error comparing schemas: %w", err)
	}

	// 4. Display differences: every category the comparator recorded, then the
	// SQL that reconciles them.
	output, err := planner.GenerateSchemaDiffSQLWithOptions(diff, info.Dialect, planner.Options{
		Capabilities: info.Capabilities,
	})

	if err != nil {
		return fmt.Errorf("error generating schema diff SQL: %w", err)
	}
	writeComparison(out, cmd.ErrOrStderr(), diff, undecided, output, info.Dialect)

	if opts.exitOnDiff {
		return nonEmptyDiffExitCode(diff, undecided)
	}
	return nil
}

// writeComparison reports the comparison result: first the change categories
// the comparator recorded, then the planner's SQL for them.
//
// The categories are listed from the diff's own fields (see
// internal/cli/internal/diffreport), so a difference is reported whether or not the
// dialect planner turned it into a statement. Reporting only the SQL is what
// made "ptah schema compare" print an empty diff for row-level security
// changes it had detected (stokaro/ptah#1284): a category no planner path
// reads renders as nothing, and nothing is indistinguishable from agreement.
//
// The declared objects the comparison withheld as undecided are reported too,
// on both streams: named on standard output beside the differences, and
// explained on standard error in the warning `ptah-compat schema diff` prints.
// No statement is planned for one, so a report of the differences alone says
// "No schema differences detected." about a database the comparison could not
// check (stokaro/ptah#3834).
func writeComparison(
	out, errOut io.Writer,
	diff *difftypes.SchemaDiff,
	undecided []coverage.Object,
	sql, dialect string,
) {
	undecidednote.Report(errOut, undecided, "the database", "the desired schema")
	categories := diffreport.Categories(diff)
	if len(categories) == 0 && len(undecided) > 0 {
		fmt.Fprintf(out, "No differences planned, but %s:\n", undecidednote.Summary(len(undecided)))
		writeUndecided(out, undecided)
		return
	}
	if len(categories) == 0 {
		fmt.Fprintln(out, "No schema differences detected.")
		return
	}
	writeDifferences(out, errOut, categories, sql, dialect)
	if len(undecided) > 0 {
		fmt.Fprintln(out)
		fmt.Fprintf(out, "Undecided (%d):\n", len(undecided))
		writeUndecided(out, undecided)
	}
}

// writeDifferences lists the change categories and then the planner's SQL for
// them, warning when the dialect's planner produced none.
func writeDifferences(out, errOut io.Writer, categories []diffreport.Category, sql, dialect string) {
	fmt.Fprintf(out, "Differences detected (%d %s):\n", len(categories), pluralize("category", "categories", len(categories)))
	for _, category := range categories {
		fmt.Fprintf(out, "  %s (%d): %s\n", category.Name, category.Count(), strings.Join(category.Objects, ", "))
	}
	fmt.Fprintln(out)

	if strings.TrimSpace(sql) == "" {
		fmt.Fprintln(out, "Reconciling SQL: none.")
		fmt.Fprintf(
			errOut,
			"warning: the %s planner produced no statements for %s; the differences above cannot be reconciled by this dialect's planner\n",
			dialect,
			strings.Join(diffreport.Names(categories), ", "),
		)
		return
	}

	fmt.Fprintln(out, "Reconciling SQL:")
	fmt.Fprint(out, sql)
}

// writeUndecided names each undecided object by kind and name, sorted here so
// the report does not depend on the order its caller passes them in.
func writeUndecided(out io.Writer, undecided []coverage.Object) {
	names := make([]string, 0, len(undecided))
	for _, object := range undecided {
		names = append(names, fmt.Sprintf("%s %q", object.Kind, object.Name))
	}
	slices.Sort(names)
	for _, name := range names {
		fmt.Fprintf(out, "  %s\n", name)
	}
}

func pluralize(singular, plural string, count int) string {
	if count == 1 {
		return singular
	}
	return plural
}

// nonEmptyDiffExitCode is the answer --exit-code gives: 1 when the database
// differs from the desired schema, and 1 when the comparison could not rule
// that out.
//
// An undecided object is one the read did not look at, so the comparison
// cannot say it exists. Exiting 0 would tell a pipeline the database matches
// when nothing checked that it does. It is the same expected negative result a
// difference is -- the check ran and the database is not proven to match -- not
// a command failure, so it takes 1 rather than 2.
func nonEmptyDiffExitCode(diff *difftypes.SchemaDiff, undecided []coverage.Object) error {
	if diff.HasChanges() {
		return exitcode.New(1, errors.New("schema diff is non-empty"))
	}
	if len(undecided) > 0 {
		return exitcode.New(1, errors.New(undecidednote.Summary(len(undecided))))
	}
	return nil
}

// resolveGeneratedExpressions asks the dev database how the target itself
// spells each declared generated expression, and returns nil options when
// nothing needs asking.
//
// Only a target that stores a REWRITE of the expression needs this, which is
// Oracle alone: it quotes and upper-cases every column reference, drops the
// spaces around operators, and adds parentheses the declaration did not carry.
// Everywhere else the stored text is the declared text and the comparison is
// already sound, so no dev database is opened and no probe is rendered
// (stokaro/ptah#1915).
//
// Without a dev URL the expression stays uncompared rather than reported. The
// diagnostic says so rather than leaving it silent: an exclusion the user did
// not ask for is worth one line on stderr.
func resolveGeneratedExpressions(
	ctx context.Context,
	opts *options,
	connectTimeout time.Duration,
	info catalog.ServerInfo,
	declared *schemamodel.Database,
) (*config.CompareOptions, error) {
	probes, err := genexprprobe.For(info.Dialect, info.Capabilities, declared)
	if err != nil {
		return nil, err
	}
	if len(probes) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(opts.devURL) == "" {
		return nil, nil
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(ctx, connectTimeout)
	defer cancelConnect()
	dev, err := dbschema.ConnectToDatabase(connectCtx, opts.devURL)
	if err != nil {
		return nil, fmt.Errorf("connect to --%s: %w", devURLFlag, err)
	}
	defer dbschema.CloseAndWarn(dev)

	resolved, err := dbexprprobe.ResolveGeneratedExpressions(ctx, dev, probes)
	if err != nil {
		return nil, err
	}
	if len(resolved) == 0 {
		return nil, nil
	}
	compareOpts := config.DefaultCompareOptions()
	compareOpts.GeneratedExpressions = resolved
	return compareOpts, nil
}
