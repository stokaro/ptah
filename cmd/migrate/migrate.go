package migrate

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/safety"
	"github.com/stokaro/ptah/migration/schemadiff"
)

const (
	rootDirFlag          = "root-dir"
	dbURLFlag            = "db-url"
	checkDestructiveFlag = "check-destructive"
	allowDestructiveFlag = "allow-destructive"
	reportFormatFlag     = "report"
)

type options struct {
	rootDir          string
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
	flags.StringVar(&opts.rootDir, rootDirFlag, "./", "Root directory to scan for Go entities")
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.BoolVar(&opts.checkDestructive, checkDestructiveFlag, false, "Fail when generated migration SQL contains destructive statements")
	flags.BoolVar(&opts.allowDestructive, allowDestructiveFlag, false, "Allow destructive statements when --check-destructive is set")
	flags.StringVar(&opts.reportFormat, reportFormatFlag, "text", "Safety report format: text, html, or json")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
}

func migrateCommandWithOptions(cmd *cobra.Command, opts *options) error {
	out := cmd.OutOrStdout()
	reportFormat := strings.ToLower(strings.TrimSpace(opts.reportFormat))

	if opts.dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if reportFormat != "text" && reportFormat != "html" && reportFormat != "json" {
		return fmt.Errorf("unsupported report format %q", reportFormat)
	}
	if reportFormat == "text" {
		fmt.Fprintf(out, "Generating migration from %s to database %s\n", opts.rootDir, dbschema.FormatDatabaseURL(opts.dbURL))
		fmt.Fprintln(out, "=== GENERATE MIGRATION SQL ===")
		fmt.Fprintln(out)
	}

	// 1. Parse Go entities
	absPath, err := filepath.Abs(opts.rootDir)
	if err != nil {
		return fmt.Errorf("error resolving path: %w", err)
	}

	result, err := goschema.ParseDir(absPath)
	if err != nil {
		return fmt.Errorf("error parsing Go entities: %w", err)
	}

	// 2. Connect to database and read schema
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return err
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	schemas := dbcli.ParseSchemas(opts.schemas)
	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, schemas)
	if err != nil {
		return fmt.Errorf("error reading database schema: %w", err)
	}

	// 3. Compare schemas (dialect-aware: MySQL/MariaDB RESTRICT == NO ACTION)
	diff := schemadiff.CompareWithDialect(result, dbSchema, conn.Info().Dialect)

	// 4. Display differences summary
	info := conn.Info()
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
	fmt.Fprintf(out, "-- Source: %s\n", opts.rootDir)
	fmt.Fprintf(out, "-- Target: %s\n", dbschema.FormatDatabaseURL(opts.dbURL))
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
