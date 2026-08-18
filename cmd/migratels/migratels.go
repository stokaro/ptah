// Package migratels implements the migrations ls command: it lists the
// migration files a migration directory holds, without connecting to a database
// (#1618).
package migratels

import (
	"fmt"
	"io"
	"path"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/migrateread"
	"go.5x5.cz/ptah/migration/migrator"
)

// options are the flags this command owns on top of the shared read surface.
type options struct {
	read   migrateread.Options
	short  bool
	latest bool
}

// NewMigrateLsCommand returns the migrations ls command.
func NewMigrateLsCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List the migration files in a migration directory",
		Long: `migrations ls lists the migration files a migration directory holds, oldest
version first. It reads the directory and nothing else: no database is contacted
and none of the SQL is executed, so it answers for a directory whose migrations
have never been applied anywhere.

A reversible Ptah migration is a pair of files, so both halves are listed;
--short collapses each migration to its version instead.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runLs(cmd, opts)
		},
	}
	migrateread.RegisterFlags(cmd.Flags(), &opts.read,
		"Verify the migration directory against its ptah.sum or atlas.sum before listing it, "+
			"and fail when it carries neither. ls reads the directory and executes none of its "+
			"SQL, so it runs no gate without this flag")
	cmd.Flags().BoolVar(&opts.short, "short", false,
		"Print only each migration's version, omitting the description and the file extension")
	cmd.Flags().BoolVar(&opts.latest, "latest", false,
		"Print only the newest migration")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runLs(cmd *cobra.Command, opts *options) error {
	dir, err := opts.read.Resolve(cmd.Context(), cmd.ErrOrStderr())
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := writeListing(cmd.OutOrStdout(), listing(dir.Files, opts)); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return nil
}

// listing renders the lines this invocation prints.
func listing(files []migrator.MigrationFile, opts *options) []string {
	if opts.latest {
		files = newest(files)
	}
	if opts.short {
		return versions(files)
	}
	return names(files)
}

// newest returns the files belonging to the highest version present, or nothing
// when the directory holds no migrations. Files arrive ordered by version, so
// the last one names the version to keep.
func newest(files []migrator.MigrationFile) []migrator.MigrationFile {
	if len(files) == 0 {
		return nil
	}
	latest := files[len(files)-1].Version
	kept := make([]migrator.MigrationFile, 0, len(files))
	for _, file := range files {
		if file.Version == latest {
			kept = append(kept, file)
		}
	}
	return kept
}

// names returns one line per migration file, named as it sits in the directory.
func names(files []migrator.MigrationFile) []string {
	lines := make([]string, 0, len(files))
	for _, file := range files {
		lines = append(lines, path.Base(file.Path))
	}
	return lines
}

// versions returns one line per migration, collapsing the up and down halves of
// a reversible pair onto the single version they share.
//
// The revision token is used rather than the numeric version because an Atlas
// repeatable migration records itself under an opaque token such as "R", and
// printing 0 for it would name a version that does not exist.
func versions(files []migrator.MigrationFile) []string {
	lines := make([]string, 0, len(files))
	seen := make(map[string]struct{}, len(files))
	for _, file := range files {
		version := file.RevisionVersion()
		if _, duplicate := seen[version]; duplicate {
			continue
		}
		seen[version] = struct{}{}
		lines = append(lines, version)
	}
	return lines
}

func writeListing(out io.Writer, lines []string) error {
	for _, line := range lines {
		if _, err := fmt.Fprintln(out, line); err != nil {
			return fmt.Errorf("write migration listing: %w", err)
		}
	}
	return nil
}
