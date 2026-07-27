package generate

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/internal/schemaload"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
)

const (
	rootDirFlag      = "root-dir"
	schemaFileFlag   = "schema-file"
	schemaCmdFlag    = "schema-cmd"
	schemaFormatFlag = "schema-format"
	dialectFlag      = "dialect"
)

const schemaCmdUsage = "External program whose stdout is the desired schema " +
	"(for example an ORM exporter). Run directly without a shell, split on " +
	`whitespace, so arguments cannot contain spaces. Example: "go run ./loader"`

type options struct {
	rootDirs     []string
	schemaFiles  []string
	schemaCmd    string
	schemaFormat string
	dialect      string
	configPath   string
	envName      string
}

func NewGenerateCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "render",
		Short: "Generate schema from Go entities or local schema files",
		Long: `Generate database schema from Go entities in the specified directory or from a schema file.

By default, this command scans the directory recursively for Go files with migrator directives.
When --schema-file is set, it reads a language-agnostic YAML schema, HCL
schema, or SQL schema file instead. An external program configured with
--schema-cmd or ptah.yaml external_schema can provide the desired schema too.`,
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
	flags.StringVar(&opts.schemaCmd, schemaCmdFlag, "", schemaCmdUsage)
	flags.StringVar(&opts.schemaFormat, schemaFormatFlag, "sql", "Format of the --schema-cmd output: sql, hcl, or yaml")
	flags.StringVar(&opts.dialect, dialectFlag, "", "Database dialect (postgres, mysql, mariadb, sqlite, clickhouse, cockroachdb, yugabytedb, spanner). If empty, generates for all dialects")
	flags.StringVar(&opts.configPath, dbcli.ConfigFlagName, "", "Path to a ptah.yaml config file (default: ./ptah.yaml when present)")
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	dbcli.RegisterExternalSchemaOptInFlag(flags)
}

func generateCommand(cmd *cobra.Command, opts *options) error {
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return err
	}
	commands, err := dbcli.ResolveExternalSchemaCommands(
		cmd,
		opts.schemaCmd,
		opts.schemaFormat,
		projectCfg,
	)
	if err != nil {
		return err
	}

	// The render dialect also hints SQL parsing for both schema files and command
	// output, so the two SQL sources are treated consistently.
	result, err := schemaload.LoadContext(cmd.Context(), schemaload.Options{
		RootDirs:    opts.rootDirs,
		SchemaFiles: opts.schemaFiles,
		Commands:    commands,
		Dialect:     opts.dialect,
		Logf:        func(format string, args ...any) { fmt.Printf(format+"\n", args...) },
	})
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
