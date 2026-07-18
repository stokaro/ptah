package migraterepair

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

var migrateRepairCmd = &cobra.Command{
	Use:   "migrate-repair",
	Short: "Repair dirty migration metadata",
	Long: `Repair dirty migration metadata after an operator has fixed a
half-applied migration manually, or resume the migration from a specific
statement.`,
	RunE: migrateRepairCommand,
}

const (
	dbURLFlag      = "db-url"
	migrationsFlag = "migrations-dir"
	versionFlag    = "version"
	dirFormatFlag  = "dir-format"
	atlasEnvFlag   = "atlas-env"
	forceFlag      = "force"
	resumeFromFlag = "resume-from"
)

var migrateRepairFlags = map[string]cobraflags.Flag{
	dbURLFlag: &cobraflags.StringFlag{
		Name:  dbURLFlag,
		Value: "",
		Usage: "Database URL (required). Example: postgres://localhost:5432/dbname",
	},
	migrationsFlag: &cobraflags.StringFlag{
		Name:  migrationsFlag,
		Value: "",
		Usage: "Directory containing migration files (required)",
	},
	versionFlag: &cobraflags.StringFlag{
		Name:  versionFlag,
		Value: "",
		Usage: "Migration version to repair (required)",
	},
	dirFormatFlag: &cobraflags.StringFlag{
		Name:  dirFormatFlag,
		Value: string(migrator.MigrationDirFormatAuto),
		Usage: "Migration directory format: auto, ptah, or atlas",
	},
	atlasEnvFlag: &cobraflags.StringFlag{
		Name:  atlasEnvFlag,
		Value: "",
		Usage: "Value exposed as .Env when rendering Atlas SQL template migrations",
	},
	forceFlag: &cobraflags.BoolFlag{
		Name:  forceFlag,
		Value: false,
		Usage: "Rewrite or create the revision row even when it is not dirty",
	},
	resumeFromFlag: &cobraflags.StringFlag{
		Name:  resumeFromFlag,
		Value: "",
		Usage: "Execute remaining up statements starting from this 1-based statement number before marking applied",
	},
	dbcli.ConnectTimeoutFlagName:      dbcli.NewConnectTimeoutFlag(),
	dbcli.MigrationsSchemaFlagName:    dbcli.NewMigrationsSchemaFlag(),
	dbcli.MigrationsTableFlagName:     dbcli.NewMigrationsTableFlag(),
	dbcli.RevisionTableFormatFlagName: dbcli.NewRevisionTableFormatFlag(),
}

func NewMigrateRepairCommand() *cobra.Command {
	cobraflags.RegisterMap(migrateRepairCmd, migrateRepairFlags)
	return migrateRepairCmd
}

func migrateRepairCommand(_ *cobra.Command, _ []string) error {
	dbURL := migrateRepairFlags[dbURLFlag].GetString()
	migrationsDir := migrateRepairFlags[migrationsFlag].GetString()
	versionValue := migrateRepairFlags[versionFlag].GetString()
	dirFormatValue := migrateRepairFlags[dirFormatFlag].GetString()
	atlasEnv := migrateRepairFlags[atlasEnvFlag].GetString()
	force := migrateRepairFlags[forceFlag].GetBool()
	resumeFromValue := migrateRepairFlags[resumeFromFlag].GetString()
	migrationsSchema := migrateRepairFlags[dbcli.MigrationsSchemaFlagName].GetString()
	migrationsTable := migrateRepairFlags[dbcli.MigrationsTableFlagName].GetString()
	revisionFormatValue := migrateRepairFlags[dbcli.RevisionTableFormatFlagName].GetString()

	if dbURL == "" {
		return fmt.Errorf("database URL is required")
	}
	if migrationsDir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	if versionValue == "" {
		return fmt.Errorf("migration version is required")
	}

	version, err := strconv.ParseInt(versionValue, 10, 64)
	if err != nil || version <= 0 {
		return fmt.Errorf("invalid migration version %q", versionValue)
	}
	resumeFrom, err := parseResumeFrom(resumeFromValue)
	if err != nil {
		return err
	}
	dirFormat, err := migrator.ParseMigrationDirFormat(dirFormatValue)
	if err != nil {
		return err
	}
	revisionFormat, err := migrator.ParseRevisionTableFormat(revisionFormatValue)
	if err != nil {
		return err
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(migrateRepairFlags[dbcli.ConnectTimeoutFlagName].GetString())
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

	mig, err := migrator.NewFSMigrator(
		conn,
		os.DirFS(migrationsDir),
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: atlasEnv}),
	)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(migrationsSchema, migrationsTable).
		WithRevisionTableFormat(revisionFormat)

	err = mig.RepairMigration(context.Background(), migrator.RepairMigrationOptions{
		Version:    version,
		Force:      force,
		ResumeFrom: resumeFrom,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Repaired migration %d\n", version)
	return nil
}

func parseResumeFrom(value string) (int, error) {
	if value == "" {
		return 0, nil
	}
	resumeFrom, err := strconv.Atoi(value)
	if err != nil || resumeFrom <= 0 {
		return 0, fmt.Errorf("invalid resume-from value %q", value)
	}
	return resumeFrom, nil
}
