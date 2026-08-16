// Package schema contains schema-source conversion commands.
package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/compare"
	"go.5x5.cz/ptah/cmd/drift"
	"go.5x5.cz/ptah/cmd/generate"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/schemapull"
	"go.5x5.cz/ptah/cmd/schemapush"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/annotationschema"
	"go.5x5.cz/ptah/internal/goannotationexport"
	"go.5x5.cz/ptah/internal/graphqlrender"
	"go.5x5.cz/ptah/internal/openapirender"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/protobufrender"
	"go.5x5.cz/ptah/internal/schemaexport"
)

const (
	exportFromFlag           = "from"
	exportToFlag             = "to"
	exportRootDirFlag        = "root-dir"
	exportSchemaFileFlag     = "schema-file"
	exportOutFlag            = "out"
	exportIncludeTablesFlag  = "include-tables"
	exportExcludeTablesFlag  = "exclude-tables"
	exportTitleFlag          = "title"
	graphqlOperationsFlag    = "graphql-operations"
	cleanupGoAnnotationsFlag = "cleanup-go-annotations"
	cleanupDryRunFlag        = "cleanup-dry-run"
	cleanupDiffFlag          = "cleanup-diff"
	exportFormatGo           = "go"
	exportFormatYAML         = "yaml"
	exportFormatSQL          = "sql"
	exportFormatHCL          = "hcl"
	exportFormatLegacyHCL    = "atlas-hcl"
	exportFormatOpenAPI      = "openapi-v3"
	exportFormatGraphQL      = "graphql"
	exportFormatProtobuf     = "protobuf"

	// exportSourceDB is named so --from db is refused with the reason and the
	// command that does read a live database, rather than with a bare list.
	exportSourceDB = "db"

	protoPackageFlag              = "proto-package"
	protoGoPackageFlag            = "go-package"
	protoTypeRemovalFlag          = "proto-type-removal"
	protoOnIncompatibleChangeFlag = "proto-on-incompatible-change"
	protoOnNameReuseFlag          = "proto-on-name-reuse"
	protoOnFieldRemovalFlag       = "proto-on-field-removal"
	protoSplitFlag                = "proto-split"
	protoOnTypeMoveFlag           = "proto-on-type-move"
	protoCommentsFlag             = "proto-comments"
)

// NewSchemaCommand returns the native schema command tree.
func NewSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Work with desired schema definitions",
		Long: `Work with desired schema definitions.

This is Ptah's native schema namespace. Atlas-compatible schema commands live
in the separate ptah-compat binary.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	cmd.AddCommand(newSchemaAnnotationsCommand())
	cmd.AddCommand(newSchemaExportCommand())
	cmd.AddCommand(newSchemaApplyCommand())
	cmd.AddCommand(newSchemaPlanCommand())
	cmd.AddCommand(newSchemaInspectCommand())
	cmd.AddCommand(newSchemaDiffCommand())
	cmd.AddCommand(newSchemaFmtCommand())
	cmd.AddCommand(schemapush.NewSchemaPushCommand())
	cmd.AddCommand(schemapull.NewSchemaPullCommand())
	renderCmd := generate.NewGenerateCommand()
	renderCmd.Short = "Render desired schema SQL"
	renderCmd.Long = "Render desired schema SQL from Go annotations, YAML, HCL, or SQL schema files, or an external schema command."
	cmd.AddCommand(renderCmd)

	compareCmd := compare.NewCompareCommand()
	compareCmd.Short = "Compare desired schema with a live database"
	compareCmd.Long = "Compare desired schema with a live database."
	cmd.AddCommand(compareCmd)

	driftCmd := drift.NewDriftCommand()
	driftCmd.Short = "Check live database drift against desired schema"
	driftCmd.Long = "Check live database drift against desired schema."
	cmd.AddCommand(driftCmd)

	cmd.AddCommand(NewSchemaTestCommand())
	return cmd
}

func newSchemaAnnotationsCommand() *cobra.Command {
	var format string
	var outPath string

	cmd := &cobra.Command{
		Use:   "annotations",
		Short: "Export Ptah Go annotation metadata",
		Long: `Export Ptah Go annotation metadata.

The JSON Schema output describes the parsed representation of every supported
//ptah directive and attribute:

  ptah schema annotations --format json-schema --out schemas/ptah-annotations.schema.json`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAnnotations(cmd, format, outPath)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&format, "format", "json-schema", "Annotation metadata format: json-schema")
	flags.StringVar(&outPath, exportOutFlag, "", "Output JSON Schema file")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAnnotations(cmd *cobra.Command, format, outPath string) error {
	if strings.TrimSpace(format) != "json-schema" {
		return cmdutil.Fail(cmd, fmt.Errorf("unsupported --format %q: expected json-schema", format))
	}
	data, err := annotationschema.Generate()
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if strings.TrimSpace(outPath) == "" {
		fmt.Fprint(cmd.OutOrStdout(), string(data))
		return nil
	}
	resolved, err := resolveOutputPath(outPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := os.WriteFile(resolved, data, 0o600); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write annotation JSON Schema: %w", err))
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Exported annotation JSON Schema to %s\n", resolved)
	return nil
}

func newSchemaExportCommand() *cobra.Command {
	var from string
	var to string
	var rootDir string
	var schemaFiles []string
	var outPath string
	var includeTables []string
	var excludeTables []string
	var title string
	var graphqlOperations []string
	var cleanupAnnotations bool
	var cleanupDryRun bool
	var cleanupDiff bool
	var plainHTTP bool
	var protoPackage string
	var protoGoPackage string
	var protoTypeRemoval string
	var protoOnIncompatibleChange string
	var protoOnNameReuse string
	var protoOnFieldRemoval string
	var protoSplit string
	var protoOnTypeMove string
	var protoComments string

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export one schema source format to another",
		Long: `Export a Ptah schema to another format.

Convert a desired schema to an HCL schema, an OpenAPI 3.0 component schema, a
GraphQL SDL, or a Protobuf definition:

  ptah schema export --to hcl         --root-dir ./models --out schema.hcl
  ptah schema export --to openapi-v3  --root-dir ./models --out openapi.yaml
  ptah schema export --to graphql     --root-dir ./models --out schema.graphql
  ptah schema export --to protobuf    --root-dir ./models \
    --out ./proto/acme/inventory/v1/schema.proto --proto-package acme.inventory.v1

The source is Go annotations under --root-dir by default. The openapi-v3,
graphql, and protobuf targets also read a YAML, HCL, or SQL schema file, which
--schema-file names as "ptah schema render" does:

  ptah schema export --to protobuf --schema-file schema.yaml \
    --out ./proto/acme/inventory/v1/schema.proto --proto-package acme.inventory.v1

--from declares that file's format and is checked against its extension; leave
it unset to take the format from the extension. The hcl target reads Go
annotations only, because it rewrites the files it reads.

For openapi-v3 and graphql, --out is optional; the schema is written to stdout
when omitted. Use --include-tables / --exclude-tables to select which tables are
exported.

The graphql target emits data types only. Operation shapes are opt-in through
--graphql-operations, which takes any combination of list, by-id, create-input
and update-input (or none, the default):

  ptah schema export --to graphql --root-dir ./models \
    --graphql-operations list,by-id

Ptah generates no resolvers, data access, or authorization, so a generated
operation shape is a type declaration and nothing more.

The protobuf target is stateful: field numbers are persistent wire identifiers,
so --out is required and the previously generated file is read back as the
source of every number it already pins. Commit that file; deleting it starts a
new, incompatible numbering history.

--proto-split=table writes one file per exported table next to --out, which then
holds the generated enums and the inventory of the set. Every file of the set is
part of the compatibility state, so all of them must be committed together.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runExport(cmd, exportOptions{
				from:                      from,
				fromExplicit:              cmd.Flags().Changed(exportFromFlag),
				to:                        to,
				rootDir:                   rootDir,
				rootDirExplicit:           cmd.Flags().Changed(exportRootDirFlag),
				schemaFiles:               schemaFiles,
				outPath:                   outPath,
				includeTables:             includeTables,
				excludeTables:             excludeTables,
				title:                     title,
				graphqlOperations:         graphqlOperations,
				cleanupAnnotations:        cleanupAnnotations,
				cleanupDryRun:             cleanupDryRun,
				cleanupDiff:               cleanupDiff,
				plainHTTP:                 plainHTTP,
				protoPackage:              protoPackage,
				protoGoPackage:            protoGoPackage,
				protoTypeRemoval:          protoTypeRemoval,
				protoOnIncompatibleChange: protoOnIncompatibleChange,
				protoOnNameReuse:          protoOnNameReuse,
				protoOnFieldRemoval:       protoOnFieldRemoval,
				protoSplit:                protoSplit,
				protoOnTypeMove:           protoOnTypeMove,
				protoComments:             protoComments,
			})
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&from, exportFromFlag, exportFormatGo, "Source schema format: go, yaml, hcl, or sql")
	flags.StringVar(&to, exportToFlag, exportFormatHCL, "Target schema format: hcl, openapi-v3, graphql, or protobuf")
	flags.StringVar(&rootDir, exportRootDirFlag, ".", "Root directory to scan for Go annotations")
	flags.StringArrayVar(&schemaFiles, exportSchemaFileFlag, nil,
		"YAML, HCL, or SQL schema file to export instead of Go annotations (repeatable; "+
			"merged with --root-dir into one composite schema when both are given; "+
			"not supported for --to hcl)")
	flags.StringVar(&outPath, exportOutFlag, "", "Output file (optional for openapi-v3/graphql; required for protobuf)")
	flags.StringSliceVar(&includeTables, exportIncludeTablesFlag, nil, "Only export these tables (comma-separated); applies to openapi-v3/graphql/protobuf")
	flags.StringSliceVar(&excludeTables, exportExcludeTablesFlag, nil, "Exclude these tables (comma-separated); applies to openapi-v3/graphql/protobuf")
	flags.StringVar(&title, exportTitleFlag, "", "OpenAPI info.title (openapi-v3 only)")
	flags.StringSliceVar(&graphqlOperations, graphqlOperationsFlag, nil,
		"Operation shapes to generate (comma-separated): none, list, by-id, create-input, "+
			"or update-input; the default is a types-only schema (graphql only)")
	flags.BoolVar(&cleanupAnnotations, cleanupGoAnnotationsFlag, false, "Remove Ptah schema annotations after a lossless HCL export")
	flags.BoolVar(&cleanupDryRun, cleanupDryRunFlag, false, "Show cleanup summary without modifying Go files")
	flags.BoolVar(&cleanupDiff, cleanupDiffFlag, false, "Print cleanup diff without modifying Go files")
	dbcli.RegisterPlainHTTPFlag(flags, &plainHTTP)
	flags.StringVar(&protoPackage, protoPackageFlag, "", "Protobuf package for the generated file (required for protobuf)")
	flags.StringVar(&protoGoPackage, protoGoPackageFlag, "", "Emit option go_package with this value (protobuf only)")
	flags.StringVar(&protoTypeRemoval, protoTypeRemovalFlag, string(protobufrender.RemovalError),
		"Behavior when a whole message or enum disappears: error, tombstone, or drop (protobuf only)")
	flags.StringVar(&protoOnIncompatibleChange, protoOnIncompatibleChangeFlag, string(protobufrender.ChangeError),
		"Behavior when a retained field's protobuf type or cardinality changes: error or renumber (protobuf only)")
	flags.StringVar(&protoOnNameReuse, protoOnNameReuseFlag, string(protobufrender.NameReuseError),
		"Behavior when a reserved field or enum value name comes back: error or release (protobuf only)")
	flags.StringVar(&protoOnFieldRemoval, protoOnFieldRemovalFlag, string(protobufrender.FieldRemovalError),
		"Behavior when a field disappears from a retained message: error or reserve (protobuf only)")
	flags.StringVar(&protoSplit, protoSplitFlag, string(protobufrender.SplitNone),
		"How many files to write: none for a single file at --out, or table for one file per exported table next to it (protobuf only)")
	flags.StringVar(&protoOnTypeMove, protoOnTypeMoveFlag, string(protobufrender.MoveError),
		"Behavior when an already-exported type would change files: error or relocate (protobuf only)")
	flags.StringVar(&protoComments, protoCommentsFlag, string(protobufrender.CommentsNone),
		"Copy source schema comments into the published contract: all or none (protobuf only)")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

type exportOptions struct {
	from string
	// fromExplicit records whether --from was passed. The flag defaults to "go",
	// so without this an explicit "--from go" and an unset --from are the same
	// value, and --schema-file could not infer its format from the extension
	// without contradicting the default.
	fromExplicit bool
	to           string
	rootDir      string
	// rootDirExplicit records whether --root-dir was passed, so its "." default
	// is not merged into a schema-file export as a second source.
	rootDirExplicit    bool
	schemaFiles        []string
	outPath            string
	includeTables      []string
	excludeTables      []string
	title              string
	graphqlOperations  []string
	cleanupAnnotations bool
	cleanupDryRun      bool
	cleanupDiff        bool
	plainHTTP          bool

	protoPackage              string
	protoGoPackage            string
	protoTypeRemoval          string
	protoOnIncompatibleChange string
	protoOnNameReuse          string
	protoOnFieldRemoval       string
	protoSplit                string
	protoOnTypeMove           string
	protoComments             string
}

func runExport(cmd *cobra.Command, opts exportOptions) error {
	// Normalize format selectors up front so validation and routing agree; an
	// untrimmed --to must never fall through routing while still reaching the
	// annotation-cleanup step below.
	opts.from = strings.TrimSpace(opts.from)
	opts.to = normalizeExportFormat(opts.to)
	if err := validateExportOptions(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if opts.to == exportFormatHCL &&
		(len(opts.includeTables) > 0 || len(opts.excludeTables) > 0 || strings.TrimSpace(opts.title) != "") {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"warning: --%s/--%s/--%s are ignored for --%s %s\n",
			exportIncludeTablesFlag, exportExcludeTablesFlag, exportTitleFlag, exportToFlag, exportFormatHCL)
	}
	if opts.to == exportFormatProtobuf && strings.TrimSpace(opts.title) != "" {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"warning: --%s is ignored for --%s %s\n", exportTitleFlag, exportToFlag, exportFormatProtobuf)
	}
	if opts.to == exportFormatHCL {
		rootDir, err := exportGoRootDir(opts)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		return runHCLExport(cmd, opts, rootDir)
	}

	db, err := loadExportSchema(cmd, opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	switch opts.to {
	case exportFormatOpenAPI:
		rendered, err := openapirender.Render(db, openapirender.Options{
			IncludeTables: opts.includeTables,
			ExcludeTables: opts.excludeTables,
			Title:         opts.title,
		})
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if err := emitAPISchema(cmd, opts, db, rendered.Data, rendered.Diagnostics, "OpenAPI schema"); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	case exportFormatGraphQL:
		operations, err := graphqlrender.ParseOperations(opts.graphqlOperations)
		if err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("--%s: %w", graphqlOperationsFlag, err))
		}
		rendered, err := graphqlrender.Render(db, graphqlrender.Options{
			IncludeTables: opts.includeTables,
			ExcludeTables: opts.excludeTables,
			Operations:    operations,
		})
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if err := emitAPISchema(cmd, opts, db, rendered.Data, rendered.Diagnostics, "GraphQL schema"); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	case exportFormatProtobuf:
		if err := runProtobufExport(cmd, opts, db); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	default:
		// validateExportOptions rejects unknown formats; this guards against a
		// selector reaching routing un-handled and silently running cleanup.
		return cmdutil.Fail(cmd, fmt.Errorf("unsupported --%s %q", exportToFlag, opts.to))
	}

	return nil
}

// runHCLExport adapts the reusable Go-to-HCL workflow to Cobra streams.
func runHCLExport(cmd *cobra.Command, opts exportOptions, rootDir string) error {
	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    rootDir,
		OutputPath: opts.outPath,
		Cleanup:    opts.cleanupAnnotations,
		DryRun:     opts.cleanupDryRun,
		Diff:       opts.cleanupDiff,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	errOut := cmd.ErrOrStderr()
	for _, diagnostic := range result.Diagnostics {
		fmt.Fprintf(errOut, "%s: %s: %s\n", diagnostic.Severity, diagnostic.Path, diagnostic.Message)
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Exported HCL schema to %s\n", result.OutputPath)
	fmt.Fprintf(out, "Found %d table(s), %d field(s), %d enum(s)\n", result.Tables, result.Fields, result.Enums)
	if len(result.Diagnostics) > 0 {
		fmt.Fprintf(out, "%d export warning(s) reported\n", len(result.Diagnostics))
	}
	for _, cleanup := range result.Cleanup {
		if opts.cleanupDiff && cleanup.Diff != "" {
			fmt.Fprint(out, cleanup.Diff)
		}
	}
	if opts.cleanupAnnotations {
		action := "Cleaned"
		if opts.cleanupDryRun || opts.cleanupDiff {
			action = "Would clean"
		}
		fmt.Fprintf(
			out,
			"%s %d file(s), removed %d annotation line(s)\n",
			action,
			len(result.Cleanup),
			result.RemovedLines,
		)
	}
	return nil
}

// emitAPISchema writes an OpenAPI or GraphQL export. With no --out the schema is
// written verbatim to stdout (for piping to a validator); with --out it is
// written to the file and a human-readable summary is printed. Diagnostics always
// go to stderr so they never corrupt a piped schema.
func emitAPISchema(cmd *cobra.Command, opts exportOptions, db *goschema.Database, data []byte, diagnostics []schemaexport.Diagnostic, label string) error {
	errOut := cmd.ErrOrStderr()
	for _, diagnostic := range diagnostics {
		fmt.Fprintf(errOut, "%s: %s: %s\n", diagnostic.Severity, diagnostic.Path, diagnostic.Message)
	}

	if opts.outPath == "" {
		_, err := cmd.OutOrStdout().Write(data)
		return err
	}

	outPath, err := resolveOutputPath(opts.outPath)
	if err != nil {
		return err
	}
	if err := os.WriteFile(outPath, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", label, err)
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Exported %s to %s\n", label, outPath)
	fmt.Fprintf(out, "Found %d table(s), %d field(s), %d enum(s)\n", len(db.Tables), len(db.Fields), len(db.Enums))
	if len(diagnostics) > 0 {
		fmt.Fprintf(out, "%d export warning(s) reported\n", len(diagnostics))
	}
	return nil
}

func normalizeExportFormat(format string) string {
	trimmed := strings.TrimSpace(format)
	if trimmed == exportFormatLegacyHCL {
		return exportFormatHCL
	}
	return trimmed
}

func validateExportOptions(opts exportOptions) error {
	if err := validateExportSource(opts); err != nil {
		return err
	}
	switch opts.to {
	case exportFormatHCL, exportFormatOpenAPI, exportFormatGraphQL, exportFormatProtobuf:
	default:
		return fmt.Errorf("unsupported --to %q: expected %s, %s, %s, or %s",
			opts.to, exportFormatHCL, exportFormatOpenAPI, exportFormatGraphQL, exportFormatProtobuf)
	}
	if opts.to == exportFormatHCL && strings.TrimSpace(opts.outPath) == "" {
		return fmt.Errorf("--out is required for --%s %s", exportToFlag, exportFormatHCL)
	}
	if opts.to == exportFormatProtobuf {
		if err := validateProtobufExportOptions(opts); err != nil {
			return err
		}
	} else if err := rejectProtobufOnlyFlags(opts); err != nil {
		return err
	}
	// The operation selector is resolved here as well as at render time, so an
	// invalid or misplaced value is refused before the schema is loaded rather
	// than after.
	if opts.to == exportFormatGraphQL {
		if _, err := graphqlrender.ParseOperations(opts.graphqlOperations); err != nil {
			return fmt.Errorf("--%s: %w", graphqlOperationsFlag, err)
		}
	} else if len(opts.graphqlOperations) > 0 {
		return fmt.Errorf("--%s is only supported with --%s %s",
			graphqlOperationsFlag, exportToFlag, exportFormatGraphQL)
	}
	if (opts.cleanupDryRun || opts.cleanupDiff) && !opts.cleanupAnnotations {
		return fmt.Errorf("--cleanup-dry-run and --cleanup-diff require --cleanup-go-annotations")
	}
	if opts.cleanupAnnotations && opts.to != exportFormatHCL {
		return fmt.Errorf("--%s is only supported with --%s %s", cleanupGoAnnotationsFlag, exportToFlag, exportFormatHCL)
	}
	return nil
}

func resolveOutputPath(path string) (string, error) {
	cleaned, err := pathguard.ResolveCLIPath(path)
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(cleaned)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("create output directory: %w", err)
	}
	return cleaned, nil
}
