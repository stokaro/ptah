// Package migrationsimport implements the migrations import command: it converts
// a migration directory produced by another versioned-migration tool
// (golang-migrate, Goose, Flyway, Liquibase, and dbmate) into Ptah's native
// NNNNNNNNNN_description.up.sql / .down.sql layout, preserving version order and
// history (#667).
package migrationsimport

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/core/platform"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/serverversion"
	"ptah.run/internal/servertarget"
	"ptah.run/migration/importer"
)

type options struct {
	from          string
	sourceDir     string
	migrationsDir string
	dryRun        bool
	allowPartial  bool
	dialect       string
	serverVersion string
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

Supported source tools: golang-migrate, Goose, Flyway, Liquibase, and dbmate.
The source tool is auto-detected from the directory layout, or set it
explicitly with --from. For Liquibase, all four serializations are read:
formatted SQL, and the XML, YAML and JSON changelogs. The sql and sqlFile
changes carry SQL and import as they are. The typed changes createTable,
dropTable, addColumn, dropColumn, createIndex, dropIndex, addPrimaryKey,
addForeignKeyConstraint, renameTable and renameColumn have no SQL until a
database is chosen, so they import only with --dialect, and the migrations
they become are written for that dialect alone. A changeset without an
explicit rollback gets the rollback Liquibase would derive, where every change
in it can be undone. Any other construct -- include, preConditions, contexts,
labels, and the remaining change types -- is refused by name rather than
dropped, so an import either carries the whole changelog or does not happen.

A source migration with no rollback file gets a placeholder down migration. A
Flyway repeatable (R__) migration is imported as a one-time migration ordered
after the versioned ones, since Ptah has no repeatable concept. The command
refuses to overwrite an existing migration file in the output directory.

Every file under the source directory is accounted for: it is converted, or it
is named on stderr with the reason it was not. An import that declined anything
is refused before ptah.sum is written, because a checksum over the subset that
survived would validate clean and leave no trace of the SQL that did not make
it. Pass --allow-partial to accept the declined set and import the rest. Flyway
sources are read recursively, which is Flyway's own contract for a location;
the other tools read only the top level, matching their own readers, and say so
for each file they leave behind.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runImport(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.from, "from", "", fmt.Sprintf("Source migration tool (auto-detected when omitted). Supported: %s", strings.Join(importer.SupportedTools(), ", ")))
	flags.StringVar(&opts.sourceDir, "source-dir", "", "Directory containing the source tool's migrations (required)")
	flags.StringVar(&opts.migrationsDir, "migrations-dir", "./migrations", "Output directory for the generated Ptah migrations")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Print the migrations that would be written without writing them")
	flags.BoolVar(&opts.allowPartial, "allow-partial", false, "Import and write ptah.sum even though some source files were not converted")
	flags.StringVar(&opts.dialect, "dialect", "", "Target dialect Liquibase typed changes such as createTable are rendered for (Liquibase sources only)")
	serverversion.Register(flags, &opts.serverVersion)
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

	source := os.DirFS(opts.sourceDir)
	var parser importer.Parser
	if opts.from != "" {
		selected, err := importer.ParserByName(opts.from)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		parser = selected
	}
	if opts.serverVersion != "" && opts.dialect == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--%s requires --dialect", serverversion.FlagName))
	}
	if opts.dialect != "" {
		// The dialect belongs to the parser, so a detected one is resolved
		// here rather than inside Import; WithDialectCapabilities then refuses
		// a source tool whose migrations are SQL already.
		if parser == nil {
			detected, err := importer.DetectParser(source)
			if err != nil {
				return cmdutil.Fail(cmd, err)
			}
			parser = detected
		}
		rendering, err := renderingParser(cmd, parser, opts)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		parser = rendering
	}

	result, err := importer.Import(source, parser, opts.migrationsDir, importer.Options{
		DryRun:       opts.dryRun,
		AllowPartial: opts.allowPartial,
	})
	if err != nil {
		if partial, ok := errors.AsType[*importer.PartialImportError](err); ok {
			// The declined set is the point of the refusal, so it is printed in
			// full rather than summarized into the error line.
			reportDeclined(cmd.ErrOrStderr(), partial.Declined)
		}
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
	reportDeclined(cmd.ErrOrStderr(), result.Declined)
	return nil
}

// renderingParser sets parser to render typed changes for --dialect, against
// the capability preset --server-version names.
//
// Nothing here connects to a server, so without --server-version the preset is
// the dialect's default, chosen by nobody; the resolver's note says so when a
// version resolved to something other than the release line it named.
func renderingParser(cmd *cobra.Command, parser importer.Parser, opts *options) (importer.Parser, error) {
	dialect := platform.NormalizeDialect(opts.dialect)
	if dialect == "" {
		return nil, fmt.Errorf("--dialect: unsupported dialect %q", opts.dialect)
	}
	target, err := servertarget.Resolve(dialect, opts.serverVersion)
	if err != nil {
		// The resolver's sentence names the value, the dialect and the remedy.
		return nil, fmt.Errorf("invalid --%s: %w", serverversion.FlagName, err)
	}
	if target.Note != "" {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s\n", target.Note)
	}
	rendering, err := importer.WithDialectCapabilities(parser, dialect, target.Capabilities)
	if err != nil {
		return nil, fmt.Errorf("--dialect: %w", err)
	}
	return rendering, nil
}

// reportDeclined names every source file the import did not convert, and why.
//
// It writes to stderr for the same reason the apply path does: the list is a
// warning about the input rather than part of the output a caller pipes on. A
// silent skip is the defect this answers -- the importer cannot tell a README
// from an author's migration that missed the naming rule by one character, so
// reporting both the same way means reporting neither (stokaro/ptah#2231).
func reportDeclined(errOut io.Writer, declined []importer.DeclinedFile) {
	if len(declined) == 0 {
		return
	}
	fmt.Fprintf(errOut, "Declined %d source file(s):\n", len(declined))
	for _, entry := range declined {
		fmt.Fprintf(errOut, "  %s: %s\n", entry.Path, entry.Reason)
	}
}
