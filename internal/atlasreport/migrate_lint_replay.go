package atlasreport

import (
	"errors"
	"fmt"
	"io"
	"time"

	"ptah.run/internal/migrationreplay"
	migrationlint "ptah.run/migration/lint"
)

// MigrateLintReplayFailure is the migration file a dev-database replay stopped
// at, as the Atlas-compatible lint report presents it: one file entry carrying
// the error, a failed replay step, and a text report with no analyzed version.
//
// The pinned community binary v1.3.0 reports a statement its dev database
// refused this way, as report content and exit 1, rather than as a bare error.
// Measured on SQLite with `ALTER TABLE users RENAME COLUMN nope TO x` in the
// failing file, `.Files` is exactly that file, named, with the error and
// nothing else, whatever the analysis would have found in the files the
// replay never reached.
type MigrateLintReplayFailure struct {
	// File is the failing file's name, spelled as the report's other file
	// entries spell it.
	File string
	// Error is the report's account of why the file failed.
	Error string
	// version is the failing file's execution-order key, the key
	// [MigrateLintOptions.FileNames] renames a converted file by. A failure
	// built as a literal has none and keeps File as written.
	version    int64
	hasVersion bool
}

// reportedName is the name the report gives the failing file: the name it has
// in the operator's directory when its layout was converted, File otherwise.
func (f MigrateLintReplayFailure) reportedName(names map[int64]string) string {
	if !f.hasVersion {
		return f.File
	}
	if name, ok := names[f.version]; ok {
		return name
	}
	return f.File
}

// NewMigrateLintReplayFailure reads the migration a failed dev-database replay
// stopped at out of err, and words the failure the way the community binary
// does. It returns nil when err names no migration of analysis, which is every
// failure before the first migration or after the last one: a connection, a
// lock, a dev database that was not clean.
//
// A statement the dev database answered with an error reads
// `executing statement: <answer>` in a file the run did not select, the base
// the selection starts from, and `executing statement: <statement>: <answer>`
// in a file it did, the statement quoted as the file spells it. That split is
// the community binary's: it replays the base without per-statement analysis.
// The answer is the driver's message as it stands. Ptah's SQLite driver puts
// the engine's result code around SQLite's own text (`SQL logic error: ... (1)`)
// where the community binary's driver prints the text alone; the driver keeps
// no separate copy of that text, so recovering it would mean taking the
// driver's message apart, and every other Ptah surface prints the message
// intact.
//
// A migration that stopped at anything else -- a statement the replay guard
// refused before it ran, a body that could not be prepared -- carries Ptah's
// own reason, because no statement was executed.
func NewMigrateLintReplayFailure(
	analysis migrationlint.Analysis,
	revisionVersions map[int64]string,
	err error,
) *MigrateLintReplayFailure {
	failed, ok := errors.AsType[*migrationreplay.MigrationError](err)
	if !ok {
		return nil
	}
	file, ok := replayedLintFile(analysis, revisionVersions, failed)
	if !ok {
		return nil
	}
	return &MigrateLintReplayFailure{
		File:       file.Name,
		Error:      migrateLintReplayErrorText(file, failed),
		version:    file.Version,
		hasVersion: true,
	}
}

// replayedLintFile finds the up file of analysis that failed carries the
// identity of. A down file shares its version and is never replayed.
func replayedLintFile(
	analysis migrationlint.Analysis,
	revisionVersions map[int64]string,
	failed *migrationreplay.MigrationError,
) (migrationlint.File, bool) {
	for _, file := range analysis.Files() {
		if file.Direction != "up" || file.Version != failed.Version {
			continue
		}
		if migrateLintVersionKey(file, revisionVersions) == failed.VersionKey {
			return file, true
		}
	}
	return migrationlint.File{}, false
}

func migrateLintReplayErrorText(file migrationlint.File, failed *migrationreplay.MigrationError) string {
	statement := failed.Statement
	if statement == nil {
		return failed.Err.Error()
	}
	if !file.Selected {
		return "executing statement: " + statement.Answer.Error()
	}
	return "executing statement: " + statement.Written + ": " + statement.Answer.Error()
}

// writeMigrateLintTextReplayFailure renders the text report of a run whose
// replay failed, as the community binary's default template renders a report
// whose only file entry is an error: the header, the error, and the summary of
// one version with errors. No version was analyzed, so none is listed and no
// schema change or diagnostic is counted.
//
// The header names the selection exactly as a report that analyzed it would.
// A run that selected nothing still reports the failing file, and the template
// counts that one file.
func writeMigrateLintTextReplayFailure(
	w io.Writer,
	analysis migrationlint.Analysis,
	revisionVersions map[int64]string,
	failure MigrateLintReplayFailure,
	now func() time.Time,
) error {
	start := now()
	header := buildMigrateLintText(analysis, revisionVersions).header
	if header == "" {
		header = "Analyzing changes (1 migration in total):"
	}
	_, err := fmt.Fprintf(
		w,
		"%s\n\n  Error: %s\n\n  -------------------------\n  -- %s\n  -- %s\n",
		header,
		failure.Error,
		now().Sub(start),
		versionSummary(0, 0, 1),
	)
	return err
}
