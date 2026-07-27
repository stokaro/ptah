package migrate

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/schemaload"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/safety"
	"github.com/stokaro/ptah/migration/schemadiff"
)

const (
	rootDirFlag          = "root-dir"
	schemaFileFlag       = "schema-file"
	schemaCmdFlag        = "schema-cmd"
	schemaFormatFlag     = "schema-format"
	dbURLFlag            = "db-url"
	checkDestructiveFlag = "check-destructive"
	allowDestructiveFlag = "allow-destructive"
	reportFormatFlag     = "report"
)

type options struct {
	rootDirs         []string
	schemaFiles      []string
	schemaCmd        string
	schemaFormat     string
	dbURL            string
	checkDestructive bool
	allowDestructive bool
	reportFormat     string
	connectTimeout   string
	schemas          string
}

func NewMigrateCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Generate migration SQL from differences",
		Long: `Generate migration SQL statements based on differences between Go entities and database schema.

This command compares your Go entities with the current database schema and generates
the SQL statements needed to update the database to match your entities.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return migrateCommandWithOptions(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.rootDirs, rootDirFlag, nil, "Root directory to scan for Go entities (repeatable; multiple roots merge into one composite schema; defaults to ./)")
	flags.StringArrayVar(&opts.schemaFiles, schemaFileFlag, nil, "YAML, HCL, or SQL schema file to migrate toward instead of, or combined with, Go entities (repeatable; multiple sources merge into one composite schema)")
	flags.StringVar(&opts.schemaCmd, schemaCmdFlag, "", `External program whose stdout is the desired schema; run without a shell, split on whitespace. Example: "go run ./loader"`)
	flags.StringVar(&opts.schemaFormat, schemaFormatFlag, "sql", "Format of the --schema-cmd output: sql")
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.BoolVar(&opts.checkDestructive, checkDestructiveFlag, false, "Fail when generated migration SQL contains destructive statements")
	flags.BoolVar(&opts.allowDestructive, allowDestructiveFlag, false, "Allow destructive statements when --check-destructive is set")
	flags.StringVar(&opts.reportFormat, reportFormatFlag, "text", "Safety report format: text, html, or json")
	flags.String(dbcli.ConfigFlagName, "", "Path to a ptah.yaml config file (default: ./ptah.yaml when present)")
	flags.String(dbcli.EnvFlagName, "", "Project env name to read from ptah.yaml or atlas.hcl")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
}

func migrateCommandWithOptions(cmd *cobra.Command, opts *options) error {
	out := cmd.OutOrStdout()
	reportFormat := strings.ToLower(strings.TrimSpace(opts.reportFormat))

	configPath, err := cmd.Flags().GetString(dbcli.ConfigFlagName)
	if err != nil {
		return err
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, configPath)
	if err != nil {
		return err
	}
	dbURL := dbcli.EffectiveString(cmd, dbURLFlag, opts.dbURL, projectCfg.DatabaseURL)
	schemasValue := dbcli.EffectiveString(cmd, dbcli.SchemasFlagName, opts.schemas, dbcli.JoinSchemas(projectCfg.Schemas))

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if reportFormat != "text" && reportFormat != "html" && reportFormat != "json" {
		return fmt.Errorf("unsupported report format %q", reportFormat)
	}
	loadOpts := schemaload.Options{
		RootDirs:    opts.rootDirs,
		SchemaFiles: opts.schemaFiles,
		Commands:    dbcli.ExternalSchemaCommands(opts.schemaCmd, opts.schemaFormat, projectCfg),
	}
	rootsDisplay := loadOpts.Sources()

	if reportFormat == "text" {
		fmt.Fprintf(out, "Generating migration from %s to database %s\n", rootsDisplay, dbschema.FormatDatabaseURL(dbURL))
		fmt.Fprintln(out, "=== GENERATE MIGRATION SQL ===")
		fmt.Fprintln(out)
	}

	// 1. Resolve the desired schema from Go entities, schema files, and/or an
	// external command into one composite schema.
	result, err := schemaload.LoadContext(cmd.Context(), loadOpts)
	if err != nil {
		return err
	}

	// 2. Connect to database and read schema
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}

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

	// 4. Display differences summary
	astNodes, err := planner.GenerateSchemaDiffASTWithCapabilities(diff, result, info.Dialect, info.Capabilities)
	if err != nil {
		return fmt.Errorf("error generating migration plan: %w", err)
	}
	assessments, err := safety.AssessRenderedWithCapabilities(astNodes, info.Dialect, info.Capabilities)
	if err != nil {
		return fmt.Errorf("error assessing migration safety: %w", err)
	}
	if reportFormat == "html" || reportFormat == "json" {
		if err := renderSafetyReport(out, reportFormat, assessments); err != nil {
			return fmt.Errorf("error rendering safety report: %w", err)
		}
		if opts.checkDestructive && safety.HasDestructiveAssessment(assessments) && !opts.allowDestructive {
			return fmt.Errorf("destructive migration statements require --allow-destructive")
		}
		return nil
	}
	if err := renderSafetyReport(out, reportFormat, assessments); err != nil {
		return fmt.Errorf("error rendering safety report: %w", err)
	}
	if opts.checkDestructive && safety.HasDestructiveAssessment(assessments) && !opts.allowDestructive {
		return fmt.Errorf("destructive migration statements require --allow-destructive")
	}

	if !diff.HasChanges() {
		return nil
	}

	// 5. Generate migration SQL
	fmt.Fprintln(out, "=== MIGRATION SQL ===")
	fmt.Fprintln(out)

	migrationSQL, err := renderer.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, astNodes...)
	if err != nil {
		return fmt.Errorf("error rendering SQL: %w", err)
	}

	fmt.Fprintln(out, "-- Migration generated from schema differences")
	fmt.Fprintf(out, "-- Generated on: %s\n", "now") // You could add actual timestamp
	fmt.Fprintf(out, "-- Source: %s\n", rootsDisplay)
	fmt.Fprintf(out, "-- Target: %s\n", dbschema.FormatDatabaseURL(dbURL))
	fmt.Fprintln(out)

	fmt.Fprint(out, migrationSQL)
	if !strings.HasSuffix(migrationSQL, "\n") {
		fmt.Fprintln(out)
	}

	fmt.Fprintln(out)
	fmt.Fprintf(out, "Generated %d migration statements.\n", countRenderedStatements(migrationSQL))
	fmt.Fprintln(out, "⚠️  Review the SQL carefully before executing!")

	return nil
}

func countRenderedStatements(sql string) int {
	statements := sqlutil.SplitSQLStatements(sql)
	count := 0
	for _, statement := range statements {
		if strings.TrimSpace(sqlutil.StripComments(statement)) != "" {
			count++
		}
	}
	if count == 0 && strings.TrimSpace(sqlutil.StripComments(sql)) != "" {
		return 1
	}
	return count
}

func renderSafetyReport(w io.Writer, format string, assessments []safety.StatementAssessment) error {
	switch format {
	case "text":
		return safety.RenderText(w, assessments)
	case "html":
		return safety.RenderHTML(w, assessments)
	case "json":
		return safety.RenderJSON(w, assessments)
	default:
		return fmt.Errorf("unsupported report format %q", format)
	}
}
