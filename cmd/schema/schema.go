// Package schema contains schema-source conversion commands.
package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/core/atlashclrender"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/goannotationcleanup"
	"github.com/stokaro/ptah/internal/pathguard"
)

const (
	exportFromFlag           = "from"
	exportToFlag             = "to"
	exportRootDirFlag        = "root-dir"
	exportOutFlag            = "out"
	cleanupGoAnnotationsFlag = "cleanup-go-annotations"
	cleanupDryRunFlag        = "cleanup-dry-run"
	cleanupDiffFlag          = "cleanup-diff"
	exportFormatGo           = "go"
	exportFormatAtlasHCL     = "atlas-hcl"
)

// NewSchemaCommand returns the schema conversion command tree.
func NewSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Convert schema sources",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newSchemaExportCommand())
	return cmd
}

func newSchemaExportCommand() *cobra.Command {
	var from string
	var to string
	var rootDir string
	var outPath string
	var cleanupAnnotations bool
	var cleanupDryRun bool
	var cleanupDiff bool

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export one schema source format to another",
		Long: `Export one schema source format to another.

The initial export path is Go annotations to Atlas schema HCL:

  ptah schema export --from go --to atlas-hcl --root-dir ./models --out schema.hcl`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runExport(cmd, exportOptions{
				from:               from,
				to:                 to,
				rootDir:            rootDir,
				outPath:            outPath,
				cleanupAnnotations: cleanupAnnotations,
				cleanupDryRun:      cleanupDryRun,
				cleanupDiff:        cleanupDiff,
			})
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&from, exportFromFlag, exportFormatGo, "Source schema format: go")
	flags.StringVar(&to, exportToFlag, exportFormatAtlasHCL, "Target schema format: atlas-hcl")
	flags.StringVar(&rootDir, exportRootDirFlag, ".", "Root directory to scan for Go annotations")
	flags.StringVar(&outPath, exportOutFlag, "", "Output Atlas HCL schema file")
	flags.BoolVar(&cleanupAnnotations, cleanupGoAnnotationsFlag, false, "Remove Ptah schema annotations from Go source after a successful export")
	flags.BoolVar(&cleanupDryRun, cleanupDryRunFlag, false, "Show cleanup summary without modifying Go files")
	flags.BoolVar(&cleanupDiff, cleanupDiffFlag, false, "Print cleanup diff without modifying Go files")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

type exportOptions struct {
	from               string
	to                 string
	rootDir            string
	outPath            string
	cleanupAnnotations bool
	cleanupDryRun      bool
	cleanupDiff        bool
}

func runExport(cmd *cobra.Command, opts exportOptions) error {
	if err := validateExportOptions(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	rootDir, err := pathguard.ResolveCLIPath(opts.rootDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("invalid root directory: %w", err))
	}
	if err := cmdutil.StatDir(rootDir); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	outPath, err := resolveOutputPath(opts.outPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	db, err := goschema.ParseDir(rootDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("parse Go annotations: %w", err))
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

func validateExportOptions(opts exportOptions) error {
	if strings.TrimSpace(opts.from) != exportFormatGo {
		return fmt.Errorf("unsupported --from %q: expected %s", opts.from, exportFormatGo)
	}
	if strings.TrimSpace(opts.to) != exportFormatAtlasHCL {
		return fmt.Errorf("unsupported --to %q: expected %s", opts.to, exportFormatAtlasHCL)
	}
	if strings.TrimSpace(opts.outPath) == "" {
		return fmt.Errorf("--out is required")
	}
	if (opts.cleanupDryRun || opts.cleanupDiff) && !opts.cleanupAnnotations {
		return fmt.Errorf("--cleanup-dry-run and --cleanup-diff require --cleanup-go-annotations")
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
