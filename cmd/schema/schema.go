// Package schema contains schema-source conversion commands.
package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/compare"
	"github.com/stokaro/ptah/cmd/drift"
	"github.com/stokaro/ptah/cmd/generate"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/annotationschema"
	"github.com/stokaro/ptah/internal/atlashclrender"
	"github.com/stokaro/ptah/internal/goannotationcleanup"
	"github.com/stokaro/ptah/internal/graphqlrender"
	"github.com/stokaro/ptah/internal/openapirender"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/internal/schemaexport"
)

const (
	exportFromFlag           = "from"
	exportToFlag             = "to"
	exportRootDirFlag        = "root-dir"
	exportOutFlag            = "out"
	exportIncludeTablesFlag  = "include-tables"
	exportExcludeTablesFlag  = "exclude-tables"
	exportTitleFlag          = "title"
	cleanupGoAnnotationsFlag = "cleanup-go-annotations"
	cleanupDryRunFlag        = "cleanup-dry-run"
	cleanupDiffFlag          = "cleanup-diff"
	exportFormatGo           = "go"
	exportFormatAtlasHCL     = "atlas-hcl"
	exportFormatOpenAPI      = "openapi-v3"
	exportFormatGraphQL      = "graphql"
)

// NewSchemaCommand returns the native schema command tree.
func NewSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Work with desired schema definitions",
		Long: `Work with desired schema definitions.

This is Ptah's native schema namespace. Atlas-compatible schema commands stay
under ptah atlas.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	cmd.AddCommand(newSchemaAnnotationsCommand())
	cmd.AddCommand(newSchemaExportCommand())
	renderCmd := generate.NewGenerateCommand()
	renderCmd.Short = "Render desired schema SQL"
	renderCmd.Long = "Render desired schema SQL from Go annotations, YAML schema files, or Atlas HCL schema files."
	cmd.AddCommand(renderCmd)

	compareCmd := compare.NewCompareCommand()
	compareCmd.Short = "Compare desired schema with a live database"
	compareCmd.Long = "Compare desired schema with a live database."
	cmd.AddCommand(compareCmd)

	driftCmd := drift.NewDriftCommand()
	driftCmd.Short = "Check live database drift against desired schema"
	driftCmd.Long = "Check live database drift against desired schema."
	cmd.AddCommand(driftCmd)
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
//migrator directive and attribute:

  ptah schema annotations --format json-schema --out schemas/migrator-annotations.schema.json`,
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
	var outPath string
	var includeTables []string
	var excludeTables []string
	var title string
	var cleanupAnnotations bool
	var cleanupDryRun bool
	var cleanupDiff bool

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export one schema source format to another",
		Long: `Export a Ptah schema to another format.

Convert Go annotations to an Atlas schema HCL, an OpenAPI 3.0 component schema, or
a GraphQL SDL:

  ptah schema export --to atlas-hcl   --root-dir ./models --out schema.hcl
  ptah schema export --to openapi-v3  --root-dir ./models --out openapi.yaml
  ptah schema export --to graphql     --root-dir ./models --out schema.graphql

For openapi-v3 and graphql, --out is optional; the schema is written to stdout
when omitted. Use --include-tables / --exclude-tables to select which tables are
exported.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runExport(cmd, exportOptions{
				from:               from,
				to:                 to,
				rootDir:            rootDir,
				outPath:            outPath,
				includeTables:      includeTables,
				excludeTables:      excludeTables,
				title:              title,
				cleanupAnnotations: cleanupAnnotations,
				cleanupDryRun:      cleanupDryRun,
				cleanupDiff:        cleanupDiff,
			})
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&from, exportFromFlag, exportFormatGo, "Source schema format: go")
	flags.StringVar(&to, exportToFlag, exportFormatAtlasHCL, "Target schema format: atlas-hcl, openapi-v3, or graphql")
	flags.StringVar(&rootDir, exportRootDirFlag, ".", "Root directory to scan for Go annotations")
	flags.StringVar(&outPath, exportOutFlag, "", "Output file (optional for openapi-v3/graphql; writes to stdout when omitted)")
	flags.StringSliceVar(&includeTables, exportIncludeTablesFlag, nil, "Only export these tables (comma-separated); applies to openapi-v3/graphql")
	flags.StringSliceVar(&excludeTables, exportExcludeTablesFlag, nil, "Exclude these tables (comma-separated); applies to openapi-v3/graphql")
	flags.StringVar(&title, exportTitleFlag, "", "OpenAPI info.title (openapi-v3 only)")
	flags.BoolVar(&cleanupAnnotations, cleanupGoAnnotationsFlag, false, "Remove Ptah schema annotations from Go source after a successful export")
	flags.BoolVar(&cleanupDryRun, cleanupDryRunFlag, false, "Show cleanup summary without modifying Go files")
	flags.BoolVar(&cleanupDiff, cleanupDiffFlag, false, "Print cleanup diff without modifying Go files")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

type exportOptions struct {
	from               string
	to                 string
	rootDir            string
	outPath            string
	includeTables      []string
	excludeTables      []string
	title              string
	cleanupAnnotations bool
	cleanupDryRun      bool
	cleanupDiff        bool
}

func runExport(cmd *cobra.Command, opts exportOptions) error {
	// Normalize format selectors up front so validation and routing agree; an
	// untrimmed --to must never fall through routing while still reaching the
	// annotation-cleanup step below.
	opts.from = strings.TrimSpace(opts.from)
	opts.to = strings.TrimSpace(opts.to)
	if err := validateExportOptions(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if opts.to == exportFormatAtlasHCL &&
		(len(opts.includeTables) > 0 || len(opts.excludeTables) > 0 || strings.TrimSpace(opts.title) != "") {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"warning: --%s/--%s/--%s are ignored for --%s %s\n",
			exportIncludeTablesFlag, exportExcludeTablesFlag, exportTitleFlag, exportToFlag, exportFormatAtlasHCL)
	}
	rootDir, err := pathguard.ResolveCLIPath(opts.rootDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("invalid root directory: %w", err))
	}
	if err := cmdutil.StatDir(rootDir); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	db, err := goschema.ParseDir(rootDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("parse Go annotations: %w", err))
	}

	switch opts.to {
	case exportFormatAtlasHCL:
		if err := runAtlasHCLExport(cmd, opts, db); err != nil {
			return err
		}
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
		rendered, err := graphqlrender.Render(db, graphqlrender.Options{
			IncludeTables: opts.includeTables,
			ExcludeTables: opts.excludeTables,
		})
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if err := emitAPISchema(cmd, opts, db, rendered.Data, rendered.Diagnostics, "GraphQL schema"); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	default:
		// validateExportOptions rejects unknown formats; this guards against a
		// selector reaching routing un-handled and silently running cleanup.
		return cmdutil.Fail(cmd, fmt.Errorf("unsupported --%s %q", exportToFlag, opts.to))
	}

	out := cmd.OutOrStdout()
	if opts.cleanupAnnotations {
		results, err := goannotationcleanup.CleanDir(goannotationcleanup.Options{
			RootDir: rootDir,
			DryRun:  opts.cleanupDryRun,
			Diff:    opts.cleanupDiff,
		})
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		for _, result := range results {
			if opts.cleanupDiff && result.Diff != "" {
				fmt.Fprint(out, result.Diff)
			}
		}
		action := "Cleaned"
		if opts.cleanupDryRun || opts.cleanupDiff {
			action = "Would clean"
		}
		fmt.Fprintf(out, "%s %d file(s), removed %d annotation line(s)\n", action, len(results), removedAnnotationLines(results))
	}
	return nil
}

// runAtlasHCLExport renders the schema to Atlas HCL and writes it to the required
// --out file. This is the original export path and its output is unchanged.
func runAtlasHCLExport(cmd *cobra.Command, opts exportOptions, db *goschema.Database) error {
	outPath, err := resolveOutputPath(opts.outPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	rendered, err := atlashclrender.Render(db)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := os.WriteFile(outPath, rendered.Data, 0o600); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write Atlas HCL schema: %w", err))
	}

	errOut := cmd.ErrOrStderr()
	for _, diagnostic := range rendered.Diagnostics {
		fmt.Fprintf(errOut, "%s: %s: %s\n", diagnostic.Severity, diagnostic.Path, diagnostic.Message)
	}

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Exported Atlas HCL schema to %s\n", outPath)
	fmt.Fprintf(out, "Found %d table(s), %d field(s), %d enum(s)\n", len(db.Tables), len(db.Fields), len(db.Enums))
	if len(rendered.Diagnostics) > 0 {
		fmt.Fprintf(out, "%d export warning(s) reported\n", len(rendered.Diagnostics))
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

func validateExportOptions(opts exportOptions) error {
	if strings.TrimSpace(opts.from) != exportFormatGo {
		return fmt.Errorf("unsupported --from %q: expected %s", opts.from, exportFormatGo)
	}
	switch strings.TrimSpace(opts.to) {
	case exportFormatAtlasHCL, exportFormatOpenAPI, exportFormatGraphQL:
	default:
		return fmt.Errorf("unsupported --to %q: expected %s, %s, or %s",
			opts.to, exportFormatAtlasHCL, exportFormatOpenAPI, exportFormatGraphQL)
	}
	if strings.TrimSpace(opts.to) == exportFormatAtlasHCL && strings.TrimSpace(opts.outPath) == "" {
		return fmt.Errorf("--out is required for --%s %s", exportToFlag, exportFormatAtlasHCL)
	}
	if (opts.cleanupDryRun || opts.cleanupDiff) && !opts.cleanupAnnotations {
		return fmt.Errorf("--cleanup-dry-run and --cleanup-diff require --cleanup-go-annotations")
	}
	if opts.cleanupAnnotations && strings.TrimSpace(opts.to) != exportFormatAtlasHCL {
		return fmt.Errorf("--%s is only supported with --%s %s", cleanupGoAnnotationsFlag, exportToFlag, exportFormatAtlasHCL)
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

func removedAnnotationLines(results []goannotationcleanup.Result) int {
	total := 0
	for _, result := range results {
		total += result.RemovedLines
	}
	return total
}
