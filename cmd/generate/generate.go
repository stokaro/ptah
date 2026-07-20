package generate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/core/atlashcl"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/core/yamlschema"
)

var generateCmd = &cobra.Command{
	Use:   "render",
	Short: "Generate schema from Go entities, YAML schema files, or Atlas HCL schema files",
	Long: `Generate database schema from Go entities in the specified directory or from a schema file.
	
By default, this command scans the directory recursively for Go files with migrator directives.
When --schema-file is set, it reads a language-agnostic YAML schema or Atlas HCL schema instead.`,
	RunE: generateCommand,
}

const (
	rootDirFlag    = "root-dir"
	schemaFileFlag = "schema-file"
	dialectFlag    = "dialect"
)

var generateFlags = map[string]cobraflags.Flag{
	rootDirFlag: &cobraflags.StringFlag{
		Name:  rootDirFlag,
		Value: "./",
		Usage: "Root directory to scan for Go entities",
	},
	schemaFileFlag: &cobraflags.StringFlag{
		Name:  schemaFileFlag,
		Value: "",
		Usage: "YAML or Atlas HCL schema file to generate from instead of scanning Go entities",
	},
	dialectFlag: &cobraflags.StringFlag{
		Name:  dialectFlag,
		Value: "",
		Usage: "Database dialect (postgres, mysql, mariadb, sqlite, clickhouse, cockroachdb, yugabytedb, spanner). If empty, generates for all dialects",
	},
}

var generateFlagsRegistered bool

func NewGenerateCommand() *cobra.Command {
	if !generateFlagsRegistered {
		cobraflags.RegisterMap(generateCmd, generateFlags)
		generateFlagsRegistered = true
	}
	cmdutil.ConfigureCommand(generateCmd)

	return generateCmd
}

func generateCommand(_ *cobra.Command, _ []string) error {
	rootDir := generateFlags[rootDirFlag].GetString()
	schemaFile := generateFlags[schemaFileFlag].GetString()
	dialect := generateFlags[dialectFlag].GetString()

	result, err := loadSchema(rootDir, schemaFile)
	if err != nil {
		return err
	}

	// Print summary
	fmt.Printf("Found %d tables, %d fields, %d indexes, %d enums, %d embedded fields\n",
		len(result.Tables), len(result.Fields), len(result.Indexes), len(result.Enums), len(result.EmbeddedFields))
	fmt.Println()

	// Print dependency information
	fmt.Println(goschema.GetDependencyInfo(result))
	fmt.Println()

	// Determine which dialects to generate
	dialects := []string{"postgres", "mysql", "mariadb", "sqlite", "clickhouse", "cockroachdb", "yugabytedb", "spanner"}
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
				switch platform.NormalizeDialect(d) {
				case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
					fmt.Printf("CREATE TYPE %s AS ENUM (%s);\n", enum.Name,
						strings.Join(func() []string {
							quoted := make([]string, len(enum.Values))
							for i, v := range enum.Values {
								quoted[i] = "'" + v + "'"
							}
							return quoted
						}(), ", "))
				default:
					fmt.Printf("-- Enum %s: %v (handled in table definitions)\n", enum.Name, enum.Values)
				}
			}
			fmt.Println()
		}

		// Generate table statements
		statements, err := renderer.GetOrderedCreateStatements(result, d)
		if err != nil {
			return fmt.Errorf("error rendering %s schema: %w", d, err)
		}

		for i, statement := range statements {
			fmt.Printf("-- Statement %d/%d\n", i+1, len(statements))
			fmt.Println(statement)
			fmt.Println()
		}

		fmt.Println()
	}

	return nil
}

func loadSchema(rootDir, schemaFile string) (*goschema.Database, error) {
	if schemaFile != "" {
		return loadSchemaFile(schemaFile)
	}

	// Convert to absolute path
	absPath, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("error resolving path: %w", err)
	}

	// Check if directory exists
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("directory does not exist: %s", absPath)
	}

	fmt.Printf("Scanning directory: %s\n", absPath)
	fmt.Println("=" + strings.Repeat("=", len(absPath)+19))
	fmt.Println()

	// Parse the entire package recursively
	result, err := goschema.ParseDir(absPath)
	if err != nil {
		return nil, fmt.Errorf("error parsing package: %w", err)
	}
	return result, nil
}

func loadSchemaFile(schemaFile string) (*goschema.Database, error) {
	absPath, err := filepath.Abs(schemaFile)
	if err != nil {
		return nil, fmt.Errorf("error resolving schema file: %w", err)
	}

	info, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("schema file does not exist: %s", absPath)
		}
		return nil, fmt.Errorf("stat schema file: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("schema file is a directory: %s", absPath)
	}

	switch strings.ToLower(filepath.Ext(absPath)) {
	case ".yaml", ".yml":
	case ".hcl":
	default:
		return nil, fmt.Errorf("unsupported schema file extension %q: only .yaml, .yml, and .hcl are supported", filepath.Ext(absPath))
	}

	fmt.Printf("Reading schema file: %s\n", absPath)
	fmt.Println("=" + strings.Repeat("=", len(absPath)+21))
	fmt.Println()

	var result *goschema.Database
	switch strings.ToLower(filepath.Ext(absPath)) {
	case ".hcl":
		result, err = atlashcl.ParseFile(absPath)
	default:
		result, err = yamlschema.ParseFile(absPath)
	}
	if err != nil {
		return nil, fmt.Errorf("error parsing schema file: %w", err)
	}
	return result, nil
}
