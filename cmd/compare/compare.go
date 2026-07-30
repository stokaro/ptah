// Package compare implements "ptah schema compare", which builds the desired
// schema from Go entities or schema files and reports how it differs from a
// live database.
package compare

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/internal/schemaload"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
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
	flags.String(dbcli.EnvFlagName, "", "Project env name to read from ptah.yaml or atlas.hcl")
	dbcli.RegisterExternalSchemaOptInFlag(flags)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
}

func compareCommand(cmd *cobra.Command, opts *options) error {
	out := cmd.OutOrStdout()

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

	// 4. Display differences
	output, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(diff, result, info.Dialect, info.Capabilities)
	if err != nil {
		return fmt.Errorf("error generating schema diff SQL: %w", err)
	}
	fmt.Fprint(out, output)

	if opts.exitOnDiff {
		return nonEmptyDiffExitCode(diff)
	}
	return nil
}

func nonEmptyDiffExitCode(diff *difftypes.SchemaDiff) error {
	if diff.HasChanges() {
		return exitcode.New(1, errors.New("schema diff is non-empty"))
	}
	return nil
}
