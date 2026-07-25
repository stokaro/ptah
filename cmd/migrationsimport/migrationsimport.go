// Package migrationsimport implements the migrations import command: it converts
// a migration directory produced by another versioned-migration tool
// (golang-migrate and Goose; Flyway and Liquibase planned) into Ptah's
// native NNNNNNNNNN_description.up.sql / .down.sql layout, preserving version
// order and history (#667).
package migrationsimport

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/migration/importer"
)

type options struct {
	from          string
	sourceDir     string
	migrationsDir string
	dryRun        bool
}

// NewMigrationsImportCommand returns the migrations import command.
func NewMigrationsImportCommand() *cobra.Command {
	opts := &options{}

	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import a migration directory from another tool into Ptah format",
		Long: `migrations import converts an existing migration directory produced by another
versioned-migration tool into Ptah's native NNNNNNNNNN_description.up.sql /
.down.sql layout, preserving version order and rewriting ptah.sum, so a team can
adopt Ptah without hand-rewriting its migration history.

Supported source tools: golang-migrate and Goose. (Flyway and Liquibase are
planned.) The source tool is auto-detected from the directory layout, or set it
explicitly with --from.

A source migration with no rollback file gets a placeholder down migration. The
command refuses to overwrite an existing migration file in the output directory.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runImport(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.from, "from", "", "Source migration tool (auto-detected when omitted). Supported: golang-migrate, goose")
	flags.StringVar(&opts.sourceDir, "source-dir", "", "Directory containing the source tool's migrations (required)")
	flags.StringVar(&opts.migrationsDir, "migrations-dir", "./migrations", "Output directory for the generated Ptah migrations")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Print the migrations that would be written without writing them")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runImport(cmd *cobra.Command, opts *options) error {
	if opts.sourceDir == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--source-dir is required"))
	}
	if err := cmdutil.StatDir(opts.sourceDir); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	var parser importer.Parser
	if opts.from != "" {
		selected, err := importer.ParserByName(opts.from)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		parser = selected
	}

	result, err := importer.Import(os.DirFS(opts.sourceDir), parser, opts.migrationsDir, importer.Options{DryRun: opts.dryRun})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	out := cmd.OutOrStdout()
	if opts.dryRun {
		fmt.Fprintf(out, "Dry run: would write %d migration file(s) to %s\n", len(result.Files), opts.migrationsDir)
	} else {
		fmt.Fprintf(out, "Wrote %d migration file(s) to %s\n", len(result.Files), opts.migrationsDir)
		if result.SumFile != "" {
			fmt.Fprintf(out, "Wrote %s/%s\n", opts.migrationsDir, result.SumFile)
		}
	}
	for _, name := range result.Files {
		fmt.Fprintf(out, "  %s\n", name)
	}
	return nil
}
