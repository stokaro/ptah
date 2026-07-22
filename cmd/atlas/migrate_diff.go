package atlas

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/internal/schemafile"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

const atlasRevisionTableName = "atlas_schema_revisions"

type atlasMigrateDiffOptions struct {
	toURLs    []string
	devURL    string
	dirURL    string
	dirFormat string
	format    string
}

func newAtlasMigrateDiffCommand() *cobra.Command {
	opts := atlasMigrateDiffOptions{}
	cmd := &cobra.Command{
		Use:   "diff [name]",
		Short: "Compute migration diff against a desired schema",
		Long: `Atlas OSS ` + "`atlas migrate diff`" + ` command path.

Drops all tables in the --dev-url database, replays the local migration
directory on it, compares the resulting state to local --to schema files, and
writes a new Atlas-style single-file migration plus atlas.sum when changes are
found. Use a disposable dev database. This implementation currently supports
local file:// migration directories and local .hcl, .yaml, .yml, or .sql schema
files. Database URLs, env:// URLs, custom output templates, schema filters, lock
flags, and Docker dev databases remain explicit follow-up gaps.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := "migration"
			if len(args) == 1 {
				name = args[0]
			}
			return runAtlasMigrateDiff(cmd, opts, name)
		},
	}
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema target URL")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used to replay migrations and compute the diff")
	flags.StringVar(&opts.dirURL, "dir", "file://migrations", "Migration directory URL")
	flags.StringVar(&opts.dirFormat, "dir-format", "atlas", "Migration directory format; only atlas is implemented")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.StringArray("schema", nil, "Schemas to diff when database URLs are used")
	flags.String("lock-timeout", "", "Timeout for acquiring Atlas migration directory locks")
	cmdutil.ConfigureCommandArgs(cmd, nil)
	return cmd
}

func runAtlasMigrateDiff(cmd *cobra.Command, opts atlasMigrateDiffOptions, name string) error {
	if err := validateAtlasMigrateDiffOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	migrationsDir, err := atlasLocalDirValue(opts.dirURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("--dir %q: %w", opts.dirURL, err))
	}
	migrationsDir, err = pathguard.ResolveCLIPath(migrationsDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("resolve migration directory: %w", err))
	}
	if err := os.MkdirAll(migrationsDir, 0755); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("create migration directory: %w", err))
	}
	if err := verifyAtlasMigrationDirSum(migrationsDir); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(context.Background(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.devURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --dev-url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	if err := replayAtlasMigrationDir(context.Background(), conn, migrationsDir); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	current, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("read dev database schema: %w", err))
	}
	current = withoutAtlasRevisionTable(current)

	dialect := conn.Info().Dialect
	desired, err := schemafile.LoadAll(opts.toURLs, schemafile.Options{Dialect: dialect})
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("load --to schema: %w", err))
	}
	diff := schemadiff.CompareWithDialect(desired, current, dialect)
	if !diff.HasChanges() {
		fmt.Fprintln(cmd.OutOrStdout(), "The migration directory is synced with the desired state, no changes to be made")
		return nil
	}

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, desired, dialect)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("generate migration SQL: %w", err))
	}
	path, err := writeAtlasMigrationFile(migrationsDir, name, atlasMigrationSQL(statements))
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if _, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas); err != nil {
		_ = os.Remove(path)
		return cmdutil.Fail(cmd, fmt.Errorf("write atlas.sum: %w", err))
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Created migration file: %s\n", path)
	fmt.Fprintf(cmd.OutOrStdout(), "Updated migration checksum: %s\n", filepath.Join(migrationsDir, migratesum.AtlasFileName))
	return nil
}

func validateAtlasMigrateDiffOptions(cmd *cobra.Command, opts atlasMigrateDiffOptions) error {
	if len(opts.toURLs) == 0 {
		return fmt.Errorf("--to is required")
	}
	if strings.TrimSpace(opts.devURL) == "" {
		return fmt.Errorf("--dev-url is required")
	}
	if strings.TrimSpace(opts.format) != "" {
		return fmt.Errorf("atlas migrate diff accepts --format, but Ptah does not implement its behavior yet")
	}
	dirFormat := strings.ToLower(strings.TrimSpace(opts.dirFormat))
	if dirFormat != "" && dirFormat != string(migrator.MigrationDirFormatAtlas) {
		return fmt.Errorf("atlas migrate diff currently writes Atlas-format migration directories only")
	}
	if values, err := cmd.Flags().GetStringArray("schema"); err == nil && len(values) > 0 {
		return fmt.Errorf("atlas migrate diff accepts --schema, but Ptah only supports local schema files for this command yet")
	}
	if value, err := cmd.Flags().GetString("lock-timeout"); err == nil && strings.TrimSpace(value) != "" {
		return fmt.Errorf("atlas migrate diff accepts --lock-timeout, but Ptah does not implement migration directory locking yet")
	}
	if strings.HasPrefix(strings.TrimSpace(opts.devURL), "docker://") {
		return fmt.Errorf("atlas migrate diff accepts docker --dev-url values, but Ptah requires a directly connectable dev database URL")
	}
	return ensureLocalSchemaURLs("--to", opts.toURLs)
}

func verifyAtlasMigrationDirSum(migrationsDir string) error {
	result, err := migratesum.VerifyDirWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	if errors.Is(err, migratesum.ErrSumFileMissing) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("migration directory checksum verification failed: %w", err)
	}
	if !result.OK() {
		return fmt.Errorf("migration directory checksum verification failed:\n%s", result.Describe())
	}
	return nil
}

func replayAtlasMigrationDir(ctx context.Context, conn *dbschema.DatabaseConnection, migrationsDir string) error {
	if err := conn.SchemaWriter().DropAllTables(); err != nil {
		return fmt.Errorf("clean dev database: %w", err)
	}
	provider, err := migrator.NewFSMigrationProvider(
		os.DirFS(migrationsDir),
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	if err != nil {
		return fmt.Errorf("load migration directory: %w", err)
	}
	for _, migration := range provider.Migrations() {
		if err := migration.Up(ctx, conn); err != nil {
			return fmt.Errorf("replay migration %d on --dev-url: %w", migration.Version, err)
		}
	}
	return nil
}

func withoutAtlasRevisionTable(schema *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	if schema == nil {
		return &dbschematypes.DBSchema{}
	}
	out := *schema
	out.Tables = filterByTable(out.Tables, func(table dbschematypes.DBTable) bool {
		return !strings.EqualFold(table.Name, atlasRevisionTableName)
	})
	out.Indexes = filterByTable(out.Indexes, func(index dbschematypes.DBIndex) bool {
		return !strings.EqualFold(index.TableName, atlasRevisionTableName)
	})
	out.Constraints = filterByTable(out.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		return !strings.EqualFold(constraint.TableName, atlasRevisionTableName)
	})
	return &out
}

func filterByTable[T any](values []T, keep func(T) bool) []T {
	out := make([]T, 0, len(values))
	for _, value := range values {
		if keep(value) {
			out = append(out, value)
		}
	}
	return out
}

func atlasMigrationSQL(statements []string) string {
	var out strings.Builder
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		out.WriteString(strings.TrimSuffix(stmt, ";"))
		out.WriteString(";\n")
	}
	return out.String()
}

func writeAtlasMigrationFile(dir, name, sql string) (string, error) {
	if strings.TrimSpace(sql) == "" {
		return "", fmt.Errorf("migration SQL is empty")
	}
	version, err := nextAtlasMigrationVersion(dir)
	if err != nil {
		return "", err
	}
	slug := atlasMigrationSlug(name)
	for {
		path := filepath.Join(dir, fmt.Sprintf("%d_%s.sql", version, slug))
		err := writeNewAtlasMigrationFile(path, sql)
		if err == nil {
			return path, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("write migration file: %w", err)
		}
		version++
	}
}

func writeNewAtlasMigrationFile(path, sql string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(sql); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("write migration SQL: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close migration file: %w", err)
	}
	return nil
}

func nextAtlasMigrationVersion(dir string) (int64, error) {
	files, err := migrator.DiscoverMigrationFiles(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	if err != nil {
		return 0, err
	}
	version := migrator.GetNextMigrationVersion()
	for _, file := range files {
		if file.Version >= version {
			version = file.Version + 1
		}
	}
	return version, nil
}

var atlasMigrationSlugInvalidChars = regexp.MustCompile(`[^a-z0-9_]+`)

func atlasMigrationSlug(name string) string {
	slug := strings.ToLower(strings.TrimSpace(name))
	slug = strings.ReplaceAll(slug, "-", "_")
	slug = strings.ReplaceAll(slug, " ", "_")
	slug = atlasMigrationSlugInvalidChars.ReplaceAllString(slug, "")
	slug = strings.Trim(slug, "_")
	if slug == "" {
		return "migration"
	}
	return slug
}
