package migrate

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/generator"
)

const (
	generateRootDirFlag          = "root-dir"
	generateDBURLFlag            = "db-url"
	generateMigrationsDirFlag    = "migrations-dir"
	generateNameFlag             = "name"
	generateShadowDBFlag         = "shadow-db"
	generateCheckDestructiveFlag = "check-destructive"
	generateAllowDestructiveFlag = "allow-destructive"
	generateReportFormatFlag     = "report"
)

func newMigrateGenerateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate migration files from schema differences",
		Long: `Generate migration files by comparing Go entities with the current database schema.

When --shadow-db is set, or migrate.generate.shadow_db is configured in ptah.yaml, Ptah verifies
the generated candidate on the shadow database before writing files:
it drops all shadow objects, replays existing migrations, applies the candidate, re-introspects the schema,
and performs an up/down/up round-trip.`,
		RunE: migrateGenerateCommand,
	}

	flags := cmd.Flags()
	flags.String(generateRootDirFlag, "./", "Root directory to scan for Go entities")
	flags.String(generateDBURLFlag, "", "Database URL (required). Example: postgres://localhost:5432/dbname")
	flags.String(generateMigrationsDirFlag, "", "Directory containing existing migrations and receiving generated files (required)")
	flags.String(generateNameFlag, "migration", "Migration name")
	flags.String(generateShadowDBFlag, "", "Shadow database URL used to verify generated migrations before writing files")
	flags.Bool(generateCheckDestructiveFlag, false, "Fail when generated migration SQL contains destructive statements")
	flags.Bool(generateAllowDestructiveFlag, false, "Allow destructive statements when --check-destructive is set")
	flags.String(generateReportFormatFlag, "", `Safety report format next to the migration files: "", html, or json`)
	flags.String(dbcli.ConfigFlagName, "", "Path to a ptah.yaml config file (default: ./ptah.yaml when present)")
	flags.String(dbcli.ConnectTimeoutFlagName, dbcli.DefaultConnectTimeout.String(), "Initial database connection timeout")
	flags.String(dbcli.EnvFlagName, "", "Project env name to read from ptah.yaml or atlas.hcl")
	flags.String(dbcli.SchemasFlagName, "", "Comma-separated schemas to introspect when supported")

	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func migrateGenerateCommand(cmd *cobra.Command, _ []string) error {
	rootDir, err := cmd.Flags().GetString(generateRootDirFlag)
	if err != nil {
		return err
	}
	dbURL, err := cmd.Flags().GetString(generateDBURLFlag)
	if err != nil {
		return err
	}
	migrationsDir, err := cmd.Flags().GetString(generateMigrationsDirFlag)
	if err != nil {
		return err
	}
	name, err := cmd.Flags().GetString(generateNameFlag)
	if err != nil {
		return err
	}
	shadowDB, err := cmd.Flags().GetString(generateShadowDBFlag)
	if err != nil {
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
	dbURL = dbcli.EffectiveString(cmd, generateDBURLFlag, dbURL, projectCfg.DatabaseURL)
	migrationsDir = dbcli.EffectiveString(cmd, generateMigrationsDirFlag, migrationsDir, projectCfg.Migration.Dir)
	shadowDB = dbcli.EffectiveString(cmd, generateShadowDBFlag, shadowDB, projectCfg.DevURL)
	reportFormat, err := cmd.Flags().GetString(generateReportFormatFlag)
	if err != nil {
		return err
	}
	checkDestructive, err := cmd.Flags().GetBool(generateCheckDestructiveFlag)
	if err != nil {
		return err
	}
	allowDestructive, err := cmd.Flags().GetBool(generateAllowDestructiveFlag)
	if err != nil {
		return err
	}
	connectTimeoutValue, err := cmd.Flags().GetString(dbcli.ConnectTimeoutFlagName)
	if err != nil {
		return err
	}
	schemasValue, err := cmd.Flags().GetString(dbcli.SchemasFlagName)
	if err != nil {
		return err
	}
	schemasValue = dbcli.EffectiveString(cmd, dbcli.SchemasFlagName, schemasValue, dbcli.JoinSchemas(projectCfg.Schemas))
	connectTimeoutValue = dbcli.EffectiveString(cmd, dbcli.ConnectTimeoutFlagName, connectTimeoutValue, projectCfg.Migration.ConnectTimeout)

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	migrationsDir, err = pathguard.ResolveCLIPath(migrationsDir)
	if err != nil {
		return fmt.Errorf("invalid migrations directory: %w", err)
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(connectTimeoutValue)
	if err != nil {
		return err
	}
	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	defer cancelConnect()

	files, err := generator.GenerateMigration(connectCtx, generator.GenerateMigrationOptions{
		GoEntitiesDir:     rootDir,
		DatabaseURL:       dbURL,
		MigrationName:     name,
		OutputDir:         migrationsDir,
		Schemas:           dbcli.ParseSchemas(schemasValue),
		CheckDestructive:  checkDestructive,
		AllowDestructive:  allowDestructive,
		ReportFormat:      reportFormat,
		ShadowDatabaseURL: shadowDB,
	})
	if err != nil {
		return err
	}
	if files == nil {
		return nil
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Generated migration files for %s:\n", dbschema.FormatDatabaseURL(dbURL))
	for _, pair := range files.Files {
		fmt.Fprintf(out, "UP:   %s\n", pair.UpFile)
		fmt.Fprintf(out, "DOWN: %s\n", pair.DownFile)
		if pair.ReportFile != "" {
			fmt.Fprintf(out, "REPORT: %s\n", pair.ReportFile)
		}
	}
	return nil
}
