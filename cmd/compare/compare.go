// Package compare implements "ptah schema compare", which builds the desired
// schema from Go entities or schema files and reports how it differs from a
// live database.
package compare

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/diffreport"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/schemaload"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

const (
	rootDirFlag      = "root-dir"
	schemaFileFlag   = "schema-file"
	schemaCmdFlag    = "schema-cmd"
	schemaFormatFlag = "schema-format"
	dbURLFlag        = "db-url"
	exitCodeFlag     = "exit-code"
	plainHTTPFlag    = "plain-http"
)

type options struct {
	rootDirs       []string
	schemaFiles    []string
	schemaCmd      string
	schemaFormat   string
	dbURL          string
	exitOnDiff     bool
	connectTimeout string
	schemas        string
	plainHTTP      bool
}

func NewCompareCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare generated schema with database",
		Long: `Compare the schema generated from Go entities with the current database schema.

This command shows differences between what your Go entities define and what
currently exists in the database, helping you identify what needs to be migrated.`,
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
	flags.StringArrayVar(&opts.schemaFiles, schemaFileFlag, nil, "YAML, HCL, or SQL schema file to compare instead of, or combined with, Go entities (repeatable; multiple sources merge into one composite schema)")
	flags.StringVar(&opts.schemaCmd, schemaCmdFlag, "", `External program whose stdout is the desired schema; run without a shell, split on whitespace. Example: "go run ./loader"`)
	flags.StringVar(&opts.schemaFormat, schemaFormatFlag, "sql", "Format of the --schema-cmd output: sql, hcl, or yaml")
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.BoolVar(&opts.exitOnDiff, exitCodeFlag, false, "Exit with 1 when the schema diff is non-empty")
	flags.BoolVar(&opts.plainHTTP, plainHTTPFlag, false, "Use plain HTTP for OCI registry access")
	flags.String(dbcli.ConfigFlagName, "", "Path to a ptah.yaml config file (default: ./ptah.yaml when present)")
	dbcli.RegisterProjectEnvFlag(flags)
	dbcli.RegisterExternalSchemaOptInFlag(flags)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
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

	loadOpts := schemaload.Options{
		RootDirs:    opts.rootDirs,
		SchemaFiles: opts.schemaFiles,
		Commands:    commands,
		Dialect:     dialect,
		PlainHTTP:   opts.plainHTTP,
	}

	fmt.Fprintf(out, "Comparing schema from %s with database %s\n", loadOpts.Sources(), dbschema.FormatDatabaseURL(dbURL))
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
	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, schemas)
	if err != nil {
		return fmt.Errorf("error reading database schema: %w", err)
	}

	// 3. Compare schemas (dialect-aware: MySQL/MariaDB RESTRICT == NO ACTION)
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabase(cmd.Context(), conn, result, dbSchema, nil)
	if err != nil {
		return fmt.Errorf("error comparing schemas: %w", err)
	}

	// 4. Display differences: every category the comparator recorded, then the
	// SQL that reconciles them.
	output, err := planner.GenerateSchemaDiffSQLWithCapabilities(diff, result, info.Dialect, info.Capabilities)
	if err != nil {
		return fmt.Errorf("error generating schema diff SQL: %w", err)
	}
	writeComparison(out, cmd.ErrOrStderr(), diff, output, info.Dialect)

	if opts.exitOnDiff {
		return nonEmptyDiffExitCode(diff)
	}
	return nil
}

// writeComparison reports the comparison result: first the change categories
// the comparator recorded, then the planner's SQL for them.
//
// The categories are listed from the diff's own fields (see
// cmd/internal/diffreport), so a difference is reported whether or not the
// dialect planner turned it into a statement. Reporting only the SQL is what
// made "ptah schema compare" print an empty diff for row-level security
// changes it had detected (stokaro/ptah#1284): a category no planner path
// reads renders as nothing, and nothing is indistinguishable from agreement.
func writeComparison(out, errOut io.Writer, diff *difftypes.SchemaDiff, sql, dialect string) {
	categories := diffreport.Categories(diff)
	if len(categories) == 0 {
		fmt.Fprintln(out, "No schema differences detected.")
		return
	}

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

func pluralize(singular, plural string, count int) string {
	if count == 1 {
		return singular
	}
	return plural
}

func nonEmptyDiffExitCode(diff *difftypes.SchemaDiff) error {
	if diff.HasChanges() {
		return exitcode.New(1, errors.New("schema diff is non-empty"))
	}
	return nil
}
