package generator

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/convert/dbschematogo"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// GenerateMigrationOptions contains options for migration generation
type GenerateMigrationOptions struct {
	// RootDir is the directory to scan for Go entities
	RootDir string
	// DatabaseURL is the connection string for the database
	DatabaseURL string
	// DBConn is the database connection (optional, if not provided, a new connection will be created)
	DBConn *dbschema.DatabaseConnection
	// MigrationName is the name for the migration (optional, defaults to "migration")
	MigrationName string
	// OutputDir is the directory where migration files will be saved
	OutputDir string
}

// MigrationFiles represents the generated migration files
type MigrationFiles struct {
	UpFile   string // Path to the up migration file
	DownFile string // Path to the down migration file
	Version  int    // Migration version (timestamp)
}

// GenerateMigration generates both up and down migration files by comparing
// the desired schema (from Go entities) with the current database state
func GenerateMigration(opts GenerateMigrationOptions) (*MigrationFiles, error) {
	// Set default migration name if not provided
	if opts.MigrationName == "" {
		opts.MigrationName = "migration"
	}

	// 1. Parse Go entities to get desired schema
	absPath, err := filepath.Abs(opts.RootDir)
	if err != nil {
		return nil, fmt.Errorf("error resolving root directory path: %w", err)
	}

	generated, err := goschema.ParseDir(absPath)
	if err != nil {
		return nil, fmt.Errorf("error parsing Go entities: %w", err)
	}

	// 2. Connect to database and read current schema
	var conn *dbschema.DatabaseConnection

	if opts.DBConn != nil {
		conn = opts.DBConn
	} else {
		conn, err := dbschema.ConnectToDatabase(opts.DatabaseURL)
		if err != nil {
			return nil, fmt.Errorf("error connecting to database: %w", err)
		}
		defer conn.Close()
	}

	dbSchema, err := conn.Reader().ReadSchema()
	if err != nil {
		return nil, fmt.Errorf("error reading database schema: %w", err)
	}

	// 3. Calculate the diff between desired and current schema
	diff := schemadiff.Compare(generated, dbSchema)

	// Check if there are any changes
	if !diff.HasChanges() {
		return nil, fmt.Errorf("no schema changes detected")
	}

	// 4. Generate migration version (timestamp)
	version := migrator.GetNextMigrationVersion()
	slog.Debug("Generated migration version", "version", version)

	// 5. Generate up migration SQL
	upSQL, err := generateUpMigrationSQL(diff, generated, conn.Info().Dialect)
	if err != nil {
		return nil, fmt.Errorf("error generating up migration SQL: %w", err)
	}

	// 6. Generate down migration SQL
	downSQL, err := generateDownMigrationSQL(diff, dbSchema, conn.Info().Dialect)
	if err != nil {
		return nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}

	// 7. Create migration files
	files, err := createMigrationFiles(opts.OutputDir, version, opts.MigrationName, upSQL, downSQL)
	if err != nil {
		return nil, fmt.Errorf("error creating migration files: %w", err)
	}

	return files, nil
}

// generateUpMigrationSQL generates the SQL for the up migration
func generateUpMigrationSQL(diff *types.SchemaDiff, generated *goschema.Database, dialect string) (string, error) {
	statements := planner.GenerateSchemaDiffSQLStatements(diff, generated, dialect)

	if len(statements) == 0 {
		return "", fmt.Errorf("no migration statements generated")
	}

	// Add header comment
	header := fmt.Sprintf("-- Migration generated from schema differences\n-- Generated on: %s\n-- Direction: UP\n\n",
		time.Now().Format(time.RFC3339))

	return header + strings.Join(statements, ";\n") + ";", nil
}

// generateDownMigrationSQL generates the SQL for the down migration by reversing the diff
func generateDownMigrationSQL(diff *types.SchemaDiff, dbSchema *dbschematypes.DBSchema, dialect string) (string, error) {
	// Create a reverse diff to generate down migration
	reverseDiff := reverseSchemaDiff(diff)

	// For down migrations, we need to use the current database schema as the "generated" schema
	// since we're reverting back to the current state
	dbAsGoSchema := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	statements := planner.GenerateSchemaDiffSQLStatements(reverseDiff, dbAsGoSchema, dialect)

	if len(statements) == 0 {
		// If no statements generated, create a simple comment
		header := fmt.Sprintf("-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n-- No rollback operations needed\n",
			time.Now().Format(time.RFC3339))
		return header, nil
	}

	// Add header comment
	header := fmt.Sprintf("-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n",
		time.Now().Format(time.RFC3339))

	return header + strings.Join(statements, ";\n") + ";", nil
}

// reverseSchemaDiff creates a reverse diff for generating down migrations
func reverseSchemaDiff(diff *types.SchemaDiff) *types.SchemaDiff {
	return &types.SchemaDiff{
		// Reverse table operations
		TablesAdded:    diff.TablesRemoved, // Tables to remove become tables to add
		TablesRemoved:  diff.TablesAdded,   // Tables to add become tables to remove
		TablesModified: reverseTableDiffs(diff.TablesModified),

		// Reverse enum operations
		EnumsAdded:    diff.EnumsRemoved, // Enums to remove become enums to add
		EnumsRemoved:  diff.EnumsAdded,   // Enums to add become enums to remove
		EnumsModified: reverseEnumDiffs(diff.EnumsModified),

		// Reverse index operations
		IndexesAdded:   diff.IndexesRemoved, // Indexes to remove become indexes to add
		IndexesRemoved: diff.IndexesAdded,   // Indexes to add become indexes to remove
	}
}

// reverseTableDiffs reverses table modifications for down migrations
func reverseTableDiffs(tableDiffs []types.TableDiff) []types.TableDiff {
	reversed := make([]types.TableDiff, len(tableDiffs))
	for i, tableDiff := range tableDiffs {
		reversed[i] = types.TableDiff{
			TableName:       tableDiff.TableName,
			ColumnsAdded:    tableDiff.ColumnsRemoved, // Columns to remove become columns to add
			ColumnsRemoved:  tableDiff.ColumnsAdded,   // Columns to add become columns to remove
			ColumnsModified: reverseColumnDiffs(tableDiff.ColumnsModified),
		}
	}
	return reversed
}

// reverseColumnDiffs reverses column modifications for down migrations
func reverseColumnDiffs(columnDiffs []types.ColumnDiff) []types.ColumnDiff {
	reversed := make([]types.ColumnDiff, len(columnDiffs))
	for i, columnDiff := range columnDiffs {
		// For column changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range columnDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = types.ColumnDiff{
			ColumnName: columnDiff.ColumnName,
			Changes:    reversedChanges,
		}
	}
	return reversed
}

// reverseEnumDiffs reverses enum modifications for down migrations
func reverseEnumDiffs(enumDiffs []types.EnumDiff) []types.EnumDiff {
	reversed := make([]types.EnumDiff, len(enumDiffs))
	for i, enumDiff := range enumDiffs {
		reversed[i] = types.EnumDiff{
			EnumName:      enumDiff.EnumName,
			ValuesAdded:   enumDiff.ValuesRemoved, // Values to remove become values to add
			ValuesRemoved: enumDiff.ValuesAdded,   // Values to add become values to remove
		}
	}
	return reversed
}

// createMigrationFiles creates the up and down migration files
func createMigrationFiles(outputDir string, version int, migrationName, upSQL, downSQL string) (*MigrationFiles, error) {
	// Ensure output directory exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate file names using the existing utility
	upFileName := migrator.GenerateMigrationFileName(version, migrationName, "up")
	downFileName := migrator.GenerateMigrationFileName(version, migrationName, "down")

	upFilePath := filepath.Join(outputDir, upFileName)
	downFilePath := filepath.Join(outputDir, downFileName)

	for {
		info, err := os.Stat(upFilePath)
		if err != nil || info.Size() == 0 {
			break
		}

		version++
		upFileName = migrator.GenerateMigrationFileName(version, migrationName, "up")
		downFileName = migrator.GenerateMigrationFileName(version, migrationName, "down")
		upFilePath = filepath.Join(outputDir, upFileName)
		downFilePath = filepath.Join(outputDir, downFileName)
	}

	// Write up migration file
	if err := os.WriteFile(upFilePath, []byte(upSQL), 0644); err != nil { //nolint:gosec // 0644 is fine
		return nil, fmt.Errorf("failed to write up migration file: %w", err)
	}

	// Write down migration file
	if err := os.WriteFile(downFilePath, []byte(downSQL), 0644); err != nil { //nolint:gosec // 0644 is fine
		return nil, fmt.Errorf("failed to write down migration file: %w", err)
	}

	return &MigrationFiles{
		UpFile:   upFilePath,
		DownFile: downFilePath,
		Version:  version,
	}, nil
}
