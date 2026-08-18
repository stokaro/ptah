// Package migrateshow implements the migrations show command: it prints the SQL
// stored in a migration directory, without connecting to a database (#1618).
package migrateshow

import (
	"fmt"
	"io"
	"io/fs"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/migrateread"
	"go.5x5.cz/ptah/migration/migrator"
)

// directionUp and directionDown are the two halves of a reversible migration,
// spelled as the file names spell them.
const (
	directionUp   = "up"
	directionDown = "down"
)

// options are the flags this command owns on top of the shared read surface.
type options struct {
	read      migrateread.Options
	versions  []string
	direction string
}

// NewMigrateShowCommand returns the migrations show command.
func NewMigrateShowCommand() *cobra.Command {
	opts := &options{}
	cmd := &cobra.Command{
		Use:   "show",
		Short: "Print the SQL of one or more migrations",
		Long: `migrations show prints the SQL a migration directory stores, exactly as it sits
on disk. It reads the directory and nothing else: no database is contacted and
none of the SQL is executed.

Repeat --version to print more than one migration; the bodies are printed in the
order the flags were given, separated by a blank line, and a version named twice
is printed once. Nothing is written until every requested migration has been
read, so a run naming one version the directory does not hold prints nothing at
all.

A reversible Ptah migration is a pair of files. --direction selects which half is
printed and defaults to the up migration, which is the SQL 'ptah migrations up'
would run.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runShow(cmd, opts)
		},
	}
	migrateread.RegisterFlags(cmd.Flags(), &opts.read,
		"Verify the migration directory against its ptah.sum or atlas.sum before printing any of "+
			"it, and fail when it carries neither. show reads the directory and executes none of "+
			"its SQL, so it runs no gate without this flag")
	cmd.Flags().StringArrayVar(&opts.versions, "version", nil,
		"Migration version to print; repeat the flag to print more than one (required)")
	cmd.Flags().StringVar(&opts.direction, "direction", directionUp,
		"Half of a reversible migration to print: up or down")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runShow(cmd *cobra.Command, opts *options) error {
	direction, err := parseDirection(opts.direction)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	requested, err := parseVersions(opts.versions)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	dir, err := opts.read.Resolve(cmd.Context(), cmd.ErrOrStderr())
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	bodies, err := read(dir, requested, direction)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if _, err := io.WriteString(cmd.OutOrStdout(), strings.Join(bodies, "\n")); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("write migration SQL: %w", err))
	}
	return nil
}

// parseDirection normalizes the --direction value.
func parseDirection(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case directionUp:
		return directionUp, nil
	case directionDown:
		return directionDown, nil
	default:
		return "", fmt.Errorf("unknown --direction %q: expected up or down", value)
	}
}

// parseVersions parses the requested versions in the order they were given and
// drops a repeat, so naming one version twice prints it once.
func parseVersions(values []string) ([]int64, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("--version is required")
	}
	versions := make([]int64, 0, len(values))
	seen := make(map[int64]struct{}, len(values))
	for _, value := range values {
		version, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("invalid --version %q: must be a positive integer", value)
		}
		if _, duplicate := seen[version]; duplicate {
			continue
		}
		seen[version] = struct{}{}
		versions = append(versions, version)
	}
	return versions, nil
}

// read assembles every requested body and returns them together.
//
// Returning them rather than writing them as they are found is what makes a run
// naming one version the directory does not hold print nothing at all: the
// caller writes the assembled result in a single call, so a failure anywhere
// leaves standard output untouched instead of holding half a listing that a
// pipeline downstream has already begun consuming.
func read(
	dir migrateread.Directory,
	requested []int64,
	direction string,
) ([]string, error) {
	bodies := make([]string, 0, len(requested))
	for _, version := range requested {
		file, err := selectFile(dir, version, direction)
		if err != nil {
			return nil, err
		}
		body, err := fs.ReadFile(dir.FS, file.Path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", file.Path, err)
		}
		bodies = append(bodies, string(body))
	}
	return bodies, nil
}

// selectFile returns the file holding one version's SQL for one direction.
//
// A file carrying no direction is the whole migration, which is how an
// Atlas-format directory stores one, so it answers for the up direction and
// leaves the down direction unanswered rather than pretending a reverse exists.
// The two refusals are distinct on purpose: a version nobody wrote and a
// version written without a reverse are different mistakes.
func selectFile(
	dir migrateread.Directory,
	version int64,
	direction string,
) (migrator.MigrationFile, error) {
	var matched, selected migrator.MigrationFile
	found, chosen := false, false
	for _, file := range dir.Files {
		if file.Repeatable || file.Version != version {
			continue
		}
		matched, found = file, true
		if directionOf(file) == direction {
			selected, chosen = file, true
		}
	}
	if !found {
		return migrator.MigrationFile{}, fmt.Errorf(
			"migration version %d not found in %s", version, dir.Display)
	}
	if !chosen {
		return migrator.MigrationFile{}, fmt.Errorf(
			"migration version %d in %s has no %s migration (found %s)",
			version, dir.Display, direction, matched.Path)
	}
	return selected, nil
}

// directionOf reports which half of a reversible migration a file holds. Only
// the down half is spelled out in a file name; everything else is the forward
// migration, including an Atlas-format file that names no direction at all.
func directionOf(file migrator.MigrationFile) string {
	if file.Direction == directionDown {
		return directionDown
	}
	return directionUp
}
