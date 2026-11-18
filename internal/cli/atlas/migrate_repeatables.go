package atlas

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/internal/atlasmigrateimport"
	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/internal/envbool"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// atlasRepeatableMigrationsEnvVar reads a top-level R__<identity>.sql of a
// directory in the Atlas layout as a native repeatable migration on `migrate
// apply` and `migrate status`: it runs after the versioned files whenever its
// content changed, and its record is a table beside the revision table, named
// after it with _repeatable. Unset, the file is the one-time version R, as
// the community binary reads it.
//
// It is a variable rather than a flag because the compatibility surface
// registers no flag the community binary does not have.
const atlasRepeatableMigrationsEnvVar = "PTAH_ATLAS_REPEATABLE_MIGRATIONS"

// atlasRepeatableMigrations is the declaration of the variable. See
// [ptah.run/internal/envbool].
//
// It is [ptah.run/internal/envbool.Gated]: the pinned community binary has no
// re-run-on-change semantics. It runs R__<name>.sql once, as the version R,
// and never again after its content changes, so strict compatibility refuses
// an enabled value rather than run a file the oracle would leave alone.
var atlasRepeatableMigrations = envbool.New(atlasRepeatableMigrationsEnvVar, false, envbool.Gated)

// resolveAtlasRepeatableMigrations reads the run's choice. A malformed value
// is refused before the command reads its directory or opens the database,
// whatever layout the directory turns out to have.
func resolveAtlasRepeatableMigrations() (bool, error) {
	return atlasRepeatableMigrations.Resolve()
}

// atlasReadsRepeatables reports whether a run that resolved enabled reads the
// repeatable migrations of a directory in format. The reading applies to the
// Atlas layout alone: every other layout is converted, and gives R__ its own
// meaning or none.
func atlasReadsRepeatables(enabled bool, format atlasmigrateimport.Format) bool {
	return enabled && format == atlasmigrateimport.FormatAtlas
}

// atlasRepeatablesHint adds to a refusal of a database that records repeatable
// migrations the way to read them: the variable, which this run did not set.
func atlasRepeatablesHint(err error) error {
	if _, ok := errors.AsType[*migrator.AtlasRepeatableRecordError](err); ok {
		return fmt.Errorf("%w: set %s=1", err, atlasRepeatableMigrationsEnvVar)
	}
	return err
}

// atlasRepeatableFiles returns the top-level R__<name>.sql files of fsys, in
// byte order: the files the variable reads as repeatable migrations on the
// verbs that honor it.
func atlasRepeatableFiles(fsys fs.FS) ([]string, error) {
	files, err := fs.Glob(fsys, migrationfile.RepeatableFilePrefix+"*.sql")
	if err != nil {
		return nil, fmt.Errorf("list the repeatable migration files: %w", err)
	}
	return files, nil
}

// atlasRepeatableReading is the resolved variable, for a verb that has to say
// what it does with an R__ file.
type atlasRepeatableReading struct {
	enabled bool
}

// resolveAtlasRepeatableReading resolves the variable.
func resolveAtlasRepeatableReading() (atlasRepeatableReading, error) {
	enabled, err := resolveAtlasRepeatableMigrations()
	return atlasRepeatableReading{enabled: enabled}, err
}

// reads narrows the reading to a directory read in format: the variable
// applies to the Atlas layout alone; see [atlasReadsRepeatables].
func (r atlasRepeatableReading) reads(format atlasmigrateimport.Format) atlasRepeatableReading {
	return atlasRepeatableReading{enabled: atlasReadsRepeatables(r.enabled, format)}
}

// noteReadAsVersions writes one line on w when the variable is set, the
// directory is in the layout it applies to and holds an R__ file, on a verb
// that keeps the Atlas reading of it, so the difference from migrate apply is
// said.
func (r atlasRepeatableReading) noteReadAsVersions(
	w io.Writer,
	verb string,
	format atlasmigrateimport.Format,
	fsys fs.FS,
) error {
	if !r.reads(format).enabled {
		return nil
	}
	files, err := atlasRepeatableFiles(fsys)
	if err != nil || len(files) == 0 {
		return err
	}
	_, err = fmt.Fprintf(w,
		"note: %s is set, but migrate %s reads %s as the one-time version R; "+
			"only migrate apply and migrate status read them as repeatable migrations\n",
		atlasRepeatableMigrationsEnvVar, verb, strings.Join(files, ", "))
	return err
}

// atlasDownRepeatablesError refuses `migrate down` under the variable over a
// directory that holds an R__ file. The verb reads such a file as the
// one-time version R, where migrate apply reads it as a repeatable migration,
// so the two would disagree about what the directory holds. A database whose
// record holds a row is refused by the rollback itself, whatever the variable
// says, as a native rollback is.
type atlasDownRepeatablesError struct {
	files []string
}

// Error names the files and the way out.
func (e *atlasDownRepeatablesError) Error() string {
	return fmt.Sprintf("%s is set and the directory holds %s, which migrate down reads as the one-time version R "+
		"while migrate apply runs them as repeatable migrations; a repeatable migration has no rollback, so roll "+
		"forward with a new migration instead",
		atlasRepeatableMigrationsEnvVar, strings.Join(e.files, ", "))
}

// refuseAtlasDownRepeatables refuses `migrate down` when the variable is
// enabled and the directory the invocation names holds an R__ file, and a
// malformed value in any case.
func refuseAtlasDownRepeatables(cmd *cobra.Command, verb atlasVerb, args []string) error {
	enabled, err := resolveAtlasRepeatableMigrations()
	if err != nil || !enabled {
		return err
	}
	files, err := atlasDownRepeatableFiles(cmd, verb, args)
	if err != nil || len(files) == 0 {
		return err
	}
	return &atlasDownRepeatablesError{files: files}
}

// atlasDownRepeatableFiles reads the directory a `migrate down` invocation
// names, with its project file, and returns its R__ files. An invocation that
// names no directory holds none here; the verb refuses it itself. It runs only
// while the variable is set, so a run without it reads the project file once.
func atlasDownRepeatableFiles(cmd *cobra.Command, verb atlasVerb, args []string) (files []string, runErr error) {
	parentFlags, parentChanged, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return nil, err
	}
	project, remaining, err := extractAtlasProjectArgs(args)
	if err != nil {
		return nil, err
	}
	project = mergeAtlasProjectArgs(atlasProjectArgValues{flags: parentFlags, changed: parentChanged}, project)
	opts, err := parseAtlasMigrateDownFormatArgs(verb, remaining)
	if err != nil {
		return nil, err
	}
	// The verb reads the project file again and says what it has to say about
	// it, so this read writes nothing.
	loadedProject, err := applyAtlasMigrateDownFormatProjectConfig(cmd.Context(), io.Discard, opts, project)
	if err != nil {
		return nil, err
	}
	defer closeAtlasProject(&loadedProject, &runErr)
	if opts.dir == "" {
		return nil, nil
	}
	var source migrationsource.LocalSource
	if loadedProject.isVirtualMigrationDir(loadedProject.migrationDir) {
		source, err = loadedProject.captureLocal(loadedProject.migrationDir)
	} else {
		source, err = migrationsource.CaptureLocal(opts.dir, opts.dirOptions)
	}
	if err != nil {
		return nil, fmt.Errorf("atlas migrate down --dir: %w", err)
	}
	return atlasRepeatableFiles(source.FileSystem)
}
