package generate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/generator"
)

var generateCmd = &cobra.Command{
	Use:   "generate [schema|migration]",
	Short: "Generate schema from Go entities or create empty migration files",
	Long: `Generate database schema from Go entities or create empty migration files for manual editing.

Default behavior (no subcommand): Generate schema from Go entities (for backward compatibility)

Available subcommands:
  schema     - Generate database schema from Go entities  
  migration  - Generate empty migration files for manual editing

Examples:
  ptah generate                                    # Generate schema (default)
  ptah generate schema --dialect postgres         # Generate schema for specific dialect
  ptah generate migration --name add_users        # Generate empty migration files`,
	RunE: schemaCommand, // Default to schema generation for backward compatibility
}

// Schema generation flags
const (
	rootDirFlag = "root-dir"
	dialectFlag = "dialect"
)

var schemaFlags = map[string]cobraflags.Flag{
	rootDirFlag: &cobraflags.StringFlag{
		Name:  rootDirFlag,
		Value: "./",
		Usage: "Root directory to scan for Go entities",
	},
	dialectFlag: &cobraflags.StringFlag{
		Name:  dialectFlag,
		Value: "",
		Usage: "Database dialect (postgres, mysql, mariadb). If empty, generates for all dialects",
	},
}

// Migration generation flags
const (
	nameFlag      = "name"
	outputDirFlag = "output-dir"
)

var migrationFlags = map[string]cobraflags.Flag{
	nameFlag: &cobraflags.StringFlag{
		Name:  nameFlag,
		Value: "",
		Usage: "Name for the migration (required)",
	},
	outputDirFlag: &cobraflags.StringFlag{
		Name:  outputDirFlag,
		Value: "./migrations",
		Usage: "Directory where migration files will be saved",
	},
}

func NewGenerateCommand() *cobra.Command {
	// Register schema flags on the main command for backward compatibility
	cobraflags.RegisterMap(generateCmd, schemaFlags)
	
	// Add subcommands
	generateCmd.AddCommand(newSchemaCommand())
	generateCmd.AddCommand(newMigrationCommand())
	return generateCmd
}

// newSchemaCommand creates the schema subcommand (existing functionality)
func newSchemaCommand() *cobra.Command {
	schemaCmd := &cobra.Command{
		Use:   "schema",
		Short: "Generate database schema from Go entities",
		Long: `Generate database schema from Go entities in the specified directory.
	
This command scans the directory recursively for Go files with migrator directives
and generates SQL schema for the specified database dialect(s).`,
		RunE: schemaCommand,
	}

	// Register flags on the subcommand as well for direct usage
	cobraflags.RegisterMap(schemaCmd, schemaFlags)
	return schemaCmd
}

// newMigrationCommand creates the migration subcommand for empty migration files
func newMigrationCommand() *cobra.Command {
	migrationCmd := &cobra.Command{
		Use:   "migration",
		Short: "Generate empty migration files for manual editing",
		Long: `Generate empty skeleton migration files with proper timestamps and naming conventions.

This command creates both up and down migration files that you can manually edit
to add custom SQL operations, data migrations, or complex schema changes that
can't be expressed through Go struct annotations.`,
		RunE: migrationCommand,
	}

	cobraflags.RegisterMap(migrationCmd, migrationFlags)
	return migrationCmd
}

func schemaCommand(_ *cobra.Command, _ []string) error {
	rootDir := schemaFlags[rootDirFlag].GetString()
	dialect := schemaFlags[dialectFlag].GetString()

	// Convert to absolute path
	absPath, err := filepath.Abs(rootDir)
	if err != nil {
		return fmt.Errorf("error resolving path: %w", err)
	}

	// Check if directory exists
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return fmt.Errorf("directory does not exist: %s", absPath)
	}

	fmt.Printf("Scanning directory: %s\n", absPath)
	fmt.Println("=" + strings.Repeat("=", len(absPath)+19))
	fmt.Println()

	// Parse the entire package recursively
	result, err := goschema.ParseDir(absPath)
	if err != nil {
		return fmt.Errorf("error parsing package: %w", err)
	}

	// Print summary
	fmt.Printf("Found %d tables, %d fields, %d indexes, %d enums, %d embedded fields\n",
		len(result.Tables), len(result.Fields), len(result.Indexes), len(result.Enums), len(result.EmbeddedFields))
	fmt.Println()

	// Print dependency information
	fmt.Println(goschema.GetDependencyInfo(result))
	fmt.Println()

	// Determine which dialects to generate
	dialects := []string{"postgres", "mysql", "mariadb"}
	if dialect != "" {
		dialects = []string{dialect}
	}

	// Generate SQL for each dialect
	for _, d := range dialects {
		fmt.Printf("=== %s SCHEMA ===\n", strings.ToUpper(d))
		fmt.Println()

		// Generate enum statements first (only once per dialect)
		if len(result.Enums) > 0 {
			fmt.Println("-- ENUMS --")
			for _, enum := range result.Enums {
				if d == "postgres" {
					fmt.Printf("CREATE TYPE %s AS ENUM (%s);\n", enum.Name,
						strings.Join(func() []string {
							quoted := make([]string, len(enum.Values))
							for i, v := range enum.Values {
								quoted[i] = "'" + v + "'"
							}
							return quoted
						}(), ", "))
				} else {
					fmt.Printf("-- Enum %s: %v (handled in table definitions)\n", enum.Name, enum.Values)
				}
			}
			fmt.Println()
		}

		// Generate table statements
		statements := renderer.GetOrderedCreateStatements(result, d)

		for i, statement := range statements {
			fmt.Printf("-- Table %d/%d\n", i+1, len(result.Tables))
			fmt.Println(statement)
			fmt.Println()
		}

		fmt.Println()
	}

	return nil
}

func migrationCommand(_ *cobra.Command, _ []string) error {
	migrationName := migrationFlags[nameFlag].GetString()
	outputDir := migrationFlags[outputDirFlag].GetString()

	if migrationName == "" {
		return fmt.Errorf("migration name is required (use --name flag)")
	}

	fmt.Printf("Generating empty migration: %s\n", migrationName)
	fmt.Printf("Output directory: %s\n", outputDir)
	fmt.Println()

	// Generate empty migration files
	files, err := generator.GenerateEmptyMigration(generator.GenerateEmptyMigrationOptions{
		MigrationName: migrationName,
		OutputDir:     outputDir,
	})
	if err != nil {
		return fmt.Errorf("error generating migration files: %w", err)
	}

	fmt.Printf("Generated migration files:\n")
	fmt.Printf("  UP:   %s\n", files.UpFile)
	fmt.Printf("  DOWN: %s\n", files.DownFile)
	fmt.Printf("  Version: %d\n", files.Version)
	fmt.Println()
	fmt.Println("✅ Empty migration files created successfully!")
	fmt.Println("You can now edit these files to add your custom SQL.")

	return nil
}
