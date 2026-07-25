package generate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/schemafile"
)

const (
	rootDirFlag    = "root-dir"
	schemaFileFlag = "schema-file"
	dialectFlag    = "dialect"
)

type options struct {
	rootDirs    []string
	schemaFiles []string
	dialect     string
}

func NewGenerateCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "render",
		Short: "Generate schema from Go entities or local schema files",
		Long: `Generate database schema from Go entities in the specified directory or from a schema file.

By default, this command scans the directory recursively for Go files with migrator directives.
When --schema-file is set, it reads a language-agnostic YAML schema, HCL
schema, or SQL schema file instead.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return generateCommand(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)

	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.rootDirs, rootDirFlag, nil, "Root directory to scan for Go entities (repeatable; multiple roots merge into one composite schema; defaults to ./)")
	flags.StringArrayVar(&opts.schemaFiles, schemaFileFlag, nil, "YAML, HCL, or SQL schema file to generate from instead of, or combined with, Go entities (repeatable; multiple sources merge into one composite schema)")
	flags.StringVar(&opts.dialect, dialectFlag, "", "Database dialect (postgres, mysql, mariadb, sqlite, clickhouse, cockroachdb, yugabytedb, spanner). If empty, generates for all dialects")
}

func generateCommand(_ *cobra.Command, opts *options) error {
	result, err := loadSchema(opts.rootDirs, opts.schemaFiles)
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
	if opts.dialect != "" {
		dialects = []string{opts.dialect}
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

func loadSchema(rootDirs, schemaFiles []string) (*goschema.Database, error) {
	// With no source of any kind, default to scanning the current directory for
	// Go entities (the historical behavior).
	if len(rootDirs) == 0 && len(schemaFiles) == 0 {
		rootDirs = []string{"./"}
	}

	// Single-source fast paths, unchanged from before: Go roots only, or exactly
	// one schema file.
	if len(schemaFiles) == 0 {
		return loadGoRoots(rootDirs)
	}
	if len(rootDirs) == 0 && len(schemaFiles) == 1 {
		return loadSchemaFile(schemaFiles[0])
	}

	// Composite: merge the Go roots (parsed un-finalized so Merge can run the
	// single finalize pass) with each schema file.
	var sources []*goschema.Database
	if len(rootDirs) > 0 {
		absRoots, err := resolveRootDirs(rootDirs)
		if err != nil {
			return nil, err
		}
		for _, absPath := range absRoots {
			fmt.Printf("Scanning directory: %s\n", absPath)
		}
		goDB, err := goschema.ParseDirRaw(absRoots...)
		if err != nil {
			return nil, fmt.Errorf("error parsing packages: %w", err)
		}
		sources = append(sources, goDB)
	}
	for _, schemaFile := range schemaFiles {
		fmt.Printf("Loading schema file: %s\n", schemaFile)
		fileDB, err := loadSchemaFile(schemaFile)
		if err != nil {
			return nil, err
		}
		sources = append(sources, fileDB)
	}
	fmt.Println()

	result, err := goschema.Merge(sources...)
	if err != nil {
		return nil, fmt.Errorf("error merging composite schema: %w", err)
	}
	return result, nil
}

// loadGoRoots parses one or more Go entity roots into a finalized composite
// schema.
func loadGoRoots(rootDirs []string) (*goschema.Database, error) {
	absRoots, err := resolveRootDirs(rootDirs)
	if err != nil {
		return nil, err
	}

	for _, absPath := range absRoots {
		fmt.Printf("Scanning directory: %s\n", absPath)
	}
	fmt.Println()

	result, err := goschema.ParseDirs(absRoots...)
	if err != nil {
		return nil, fmt.Errorf("error parsing packages: %w", err)
	}
	return result, nil
}

// resolveRootDirs turns each root into an absolute path and fails fast if any
// does not exist.
func resolveRootDirs(rootDirs []string) ([]string, error) {
	absRoots := make([]string, 0, len(rootDirs))
	for _, rootDir := range rootDirs {
		absPath, err := filepath.Abs(rootDir)
		if err != nil {
			return nil, fmt.Errorf("error resolving path: %w", err)
		}
		if _, err := os.Stat(absPath); os.IsNotExist(err) {
			return nil, fmt.Errorf("directory does not exist: %s", absPath)
		}
		absRoots = append(absRoots, absPath)
	}
	return absRoots, nil
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

	ext := strings.ToLower(filepath.Ext(absPath))
	switch ext {
	case ".yaml", ".yml", ".hcl", ".sql":
	default:
		return nil, fmt.Errorf("unsupported schema file extension %q: only .yaml, .yml, .hcl, and .sql are supported", filepath.Ext(absPath))
	}

	fmt.Printf("Reading schema file: %s\n", absPath)
	fmt.Println("=" + strings.Repeat("=", len(absPath)+21))
	fmt.Println()

	result, err := schemafile.LoadPath(absPath, schemafile.Options{})
	if err != nil {
		return nil, fmt.Errorf("error parsing schema file: %w", err)
	}
	return result, nil
}
