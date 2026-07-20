// Package introspect implements live database import into annotated Go models.
package introspect

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
	"github.com/stokaro/ptah/internal/convert/goschematogo"
	"github.com/stokaro/ptah/internal/pathguard"
)

const (
	dbURLFlag           = "db-url"
	outFlag             = "out"
	packageFlag         = "package"
	perTableFlag        = "per-table"
	singleFileFlag      = "single-file"
	lowercaseFieldsFlag = "lowercase-fields"
	addJSONTagsFlag     = "add-json-tags"
	addDBTagsFlag       = "add-db-tags"
)

type options struct {
	dbURL           string
	outDir          string
	packageName     string
	perTable        bool
	singleFile      bool
	lowercaseFields bool
	addJSONTags     bool
	addDBTags       bool
	schemasRaw      string
	connectTimeout  string
}

// NewIntrospectCommand returns the native baseline-import command.
func NewIntrospectCommand() *cobra.Command {
	opts := options{
		packageName:    "models",
		connectTimeout: dbcli.DefaultConnectTimeout.String(),
	}

	cmd := &cobra.Command{
		Use:   "introspect",
		Short: "Generate annotated Go models from a live database",
		Long: `Generate annotated Go models from a live database.

This command reads the current database schema and writes Ptah's Go annotation
source-of-truth representation:

  ptah introspect --db-url postgres://localhost/db --out ./models --package models`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Database URL to introspect (required)")
	flags.StringVar(&opts.outDir, outFlag, "", "Output directory for generated Go models (required)")
	flags.StringVar(&opts.packageName, packageFlag, "models", "Go package name for generated files")
	flags.BoolVar(&opts.perTable, perTableFlag, false, "Write one Go file per table")
	flags.BoolVar(&opts.singleFile, singleFileFlag, false, "Write all generated annotations to one schema.go file")
	flags.BoolVar(&opts.lowercaseFields, lowercaseFieldsFlag, false, "Generate lowercase struct field names")
	flags.BoolVar(&opts.addJSONTags, addJSONTagsFlag, false, "Add json struct tags using database column names")
	flags.BoolVar(&opts.addDBTags, addDBTagsFlag, false, "Add db struct tags using database column names")
	flags.StringVar(&opts.schemasRaw, dbcli.SchemasFlagName, "", "Comma-separated database schemas to introspect (PostgreSQL-family only). Empty uses the connection default schema.")
	flags.StringVar(&opts.connectTimeout, dbcli.ConnectTimeoutFlagName, dbcli.DefaultConnectTimeout.String(), "Maximum time to wait while opening the database connection, e.g. 5s or 500ms. Use 0 to disable the timeout.")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func run(cmd *cobra.Command, opts options) error {
	if err := validateOptions(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	outDir, err := pathguard.ResolveCLIPath(opts.outDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("invalid output directory: %w", err))
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(context.Background(), connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancelConnect()
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to database: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, dbcli.ParseSchemas(opts.schemasRaw))
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("read database schema: %w", err))
	}
	goSchema := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)
	files, err := goschematogo.Render(goSchema, goschematogo.Options{
		PackageName:     opts.packageName,
		PerTable:        opts.perTable,
		SingleFile:      opts.singleFile,
		LowercaseFields: opts.lowercaseFields,
		AddJSONTags:     opts.addJSONTags,
		AddDBTags:       opts.addDBTags,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := goschematogo.WriteDir(outDir, files); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Generated %d Go file(s) in %s\n", len(files), outDir)
	fmt.Fprintf(cmd.OutOrStdout(), "Imported %d table(s), %d field(s), %d enum(s)\n", len(goSchema.Tables), len(goSchema.Fields), len(goSchema.Enums))
	return nil
}

func validateOptions(opts options) error {
	if strings.TrimSpace(opts.dbURL) == "" {
		return fmt.Errorf("--db-url is required")
	}
	if strings.TrimSpace(opts.outDir) == "" {
		return fmt.Errorf("--out is required")
	}
	if strings.TrimSpace(opts.packageName) == "" {
		return fmt.Errorf("--package is required")
	}
	if opts.singleFile && opts.perTable {
		return fmt.Errorf("--single-file and --per-table are mutually exclusive")
	}
	return nil
}
