package migrate

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/safety"
	"github.com/stokaro/ptah/migration/schemadiff"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Generate migration SQL from differences",
	Long: `Generate migration SQL statements based on differences between Go entities and database schema.
	
This command compares your Go entities with the current database schema and generates
the SQL statements needed to update the database to match your entities.`,
	Args: cobra.NoArgs,
	RunE: migrateCommand,
}

var migrateFlagsRegistered bool

const (
	rootDirFlag          = "root-dir"
	dbURLFlag            = "db-url"
	checkDestructiveFlag = "check-destructive"
	allowDestructiveFlag = "allow-destructive"
	reportFormatFlag     = "report"
)

var migrateFlags = newMigrateFlags()

func newMigrateFlags() map[string]cobraflags.Flag {
	return map[string]cobraflags.Flag{
		rootDirFlag: &cobraflags.StringFlag{
			Name:  rootDirFlag,
			Value: "./",
			Usage: "Root directory to scan for Go entities",
		},
		dbURLFlag: &cobraflags.StringFlag{
			Name:  dbURLFlag,
			Value: "",
			Usage: "Database URL (required). Example: postgres://localhost:5432/dbname",
		},
		checkDestructiveFlag: &cobraflags.BoolFlag{
			Name:  checkDestructiveFlag,
			Value: false,
			Usage: "Fail when generated migration SQL contains destructive statements",
		},
		allowDestructiveFlag: &cobraflags.BoolFlag{
			Name:  allowDestructiveFlag,
			Value: false,
			Usage: "Allow destructive statements when --check-destructive is set",
		},
		reportFormatFlag: &cobraflags.StringFlag{
			Name:  reportFormatFlag,
			Value: "text",
			Usage: "Safety report format: text or html",
		},
		dbcli.ConnectTimeoutFlagName: dbcli.NewConnectTimeoutFlag(),
		dbcli.SchemasFlagName:        dbcli.NewSchemasFlag(),
	}
}

func NewMigrateCommand() *cobra.Command {
	if !migrateFlagsRegistered {
		cobraflags.RegisterMap(migrateCmd, migrateFlags)
		migrateFlagsRegistered = true
	}
	addMigrateGenerateCommand(migrateCmd)
	addMigrateNewCommand(migrateCmd)
	return migrateCmd
}

func addMigrateGenerateCommand(cmd *cobra.Command) {
	for _, child := range cmd.Commands() {
		if child.Name() == "generate" {
			return
		}
	}
	cmd.AddCommand(newMigrateGenerateCommand())
}

func addMigrateNewCommand(cmd *cobra.Command) {
	for _, child := range cmd.Commands() {
		if child.Name() == "new" {
			return
		}
	}
	cmd.AddCommand(newMigrateNewCommand())
}

func migrateCommand(cmd *cobra.Command, _ []string) error {
	return migrateCommandWithFlags(cmd, migrateFlags)
}

func migrateCommandWithFlags(cmd *cobra.Command, flags map[string]cobraflags.Flag) error {
	out := cmd.OutOrStdout()
	rootDir := flags[rootDirFlag].GetString()
	dbURL := flags[dbURLFlag].GetString()
	checkDestructive := flags[checkDestructiveFlag].GetBool()
	allowDestructive := flags[allowDestructiveFlag].GetBool()
	reportFormat := strings.ToLower(strings.TrimSpace(flags[reportFormatFlag].GetString()))

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if reportFormat != "text" && reportFormat != "html" {
		return fmt.Errorf("unsupported report format %q", reportFormat)
	}
	if reportFormat != "html" {
		fmt.Fprintf(out, "Generating migration from %s to database %s\n", rootDir, dbschema.FormatDatabaseURL(dbURL))
		fmt.Fprintln(out, "=== GENERATE MIGRATION SQL ===")
		fmt.Fprintln(out)
	}

	// 1. Parse Go entities
	absPath, err := filepath.Abs(rootDir)
	if err != nil {
		return fmt.Errorf("error resolving path: %w", err)
	}

	result, err := goschema.ParseDir(absPath)
	if err != nil {
		return fmt.Errorf("error parsing Go entities: %w", err)
	}

	// 2. Connect to database and read schema
	connectTimeout, err := dbcli.ParseConnectTimeout(flags[dbcli.ConnectTimeoutFlagName].GetString())
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

	schemas := dbcli.ParseSchemas(flags[dbcli.SchemasFlagName].GetString())
	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, schemas)
	if err != nil {
		return fmt.Errorf("error reading database schema: %w", err)
	}

	// 3. Compare schemas (dialect-aware: MySQL/MariaDB RESTRICT == NO ACTION)
	diff := schemadiff.CompareWithDialect(result, dbSchema, conn.Info().Dialect)

	// 4. Display differences summary
	info := conn.Info()
	astNodes := planner.GenerateSchemaDiffASTWithCapabilities(diff, result, info.Dialect, info.Capabilities)
	assessments, err := safety.AssessRenderedWithCapabilities(astNodes, info.Dialect, info.Capabilities)
	if err != nil {
		return fmt.Errorf("error assessing migration safety: %w", err)
	}
	if reportFormat == "html" {
		if err := safety.RenderHTML(out, assessments); err != nil {
			return fmt.Errorf("error rendering safety report: %w", err)
		}
		if checkDestructive && safety.HasDestructiveAssessment(assessments) && !allowDestructive {
			return fmt.Errorf("destructive migration statements require --allow-destructive")
		}
		return nil
	}
	fmt.Fprint(out, astNodes)
	if err := safety.RenderText(out, assessments); err != nil {
		return fmt.Errorf("error rendering safety report: %w", err)
	}
	if checkDestructive && safety.HasDestructiveAssessment(assessments) && !allowDestructive {
		return fmt.Errorf("destructive migration statements require --allow-destructive")
	}

	if !diff.HasChanges() {
		return nil
	}

	// 5. Generate migration SQL
	fmt.Fprintln(out, "=== MIGRATION SQL ===")
	fmt.Fprintln(out)

	statements, err := renderer.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, astNodes...)
	if err != nil {
		return fmt.Errorf("error rendering SQL: %w", err)
	}

	fmt.Fprintln(out, "-- Migration generated from schema differences")
	fmt.Fprintf(out, "-- Generated on: %s\n", "now") // You could add actual timestamp
	fmt.Fprintf(out, "-- Source: %s\n", rootDir)
	fmt.Fprintf(out, "-- Target: %s\n", dbschema.FormatDatabaseURL(dbURL))
	fmt.Fprintln(out)

	for _, statement := range statements {
		fmt.Fprintln(out, statement)
	}

	fmt.Fprintln(out)
	fmt.Fprintf(out, "Generated %d migration statements.\n", len(statements))
	fmt.Fprintln(out, "⚠️  Review the SQL carefully before executing!")

	return nil
}
