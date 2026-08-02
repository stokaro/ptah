package atlasreport

import (
	"errors"
	"io"
	"io/fs"
	"regexp"
	"slices"
	"strconv"
	"time"

	"go.5x5.cz/ptah/migration/migrator"
)

var atlasMigrateDownFailedVersionRe = regexp.MustCompile(`failed to revert migration ([0-9]+)`)

// MigrateDownResultOptions carries the native down engine's results into the
// Atlas migrate down report. PlannedVersions and RevertedVersions are in
// revert order (newest first).
type MigrateDownResultOptions struct {
	Driver           string
	URL              string
	Dir              string
	FS               fs.FS
	Migrations       []*migrator.Migration
	PlannedVersions  []int64
	RevertedVersions []int64
	CurrentVersion   int64
	TargetVersion    int64
	Reverted         bool
	StartedAt        time.Time
	EndedAt          time.Time
	ErrorText        string
	DownError        error
}

// atlasMigrateDownResult mirrors the fields Atlas's own migrate down report
// exposes to --format templates (cmdlog.MigrateDown): the migration
// environment, the planned files, the (possibly partial) reverted files, the
// current and target version states, the total number of files to revert, and
// the run window. The cloud-only plan-approval fields (URL, Status) are not
// modeled: Ptah has no registry and reverts through local down migrations.
type atlasMigrateDownResult struct {
	atlasEnv
	Env      atlasEnv                        `json:"-"`
	Planned  []atlasMigrateApplyFile         `json:"Planned,omitempty"`
	Reverted []*atlasMigrateDownRevertedFile `json:"Reverted,omitempty"`
	Current  string                          `json:"Current,omitempty"`
	Target   string                          `json:"Target,omitempty"`
	Total    int                             `json:"Total,omitempty"`
	Start    time.Time
	End      time.Time
	Error    string `json:"Error,omitempty"`
}

type atlasMigrateDownRevertedFile struct {
	atlasMigrateApplyFile
	Start   time.Time
	End     time.Time
	Skipped int
	Applied []string
	Error   *atlasMigrateApplyStatementError
}

// WriteMigrateDownFormat renders Atlas migrate down --format output from the
// native down engine's results.
func WriteMigrateDownFormat(w io.Writer, format string, opts MigrateDownResultOptions) error {
	result, err := buildAtlasMigrateDownResult(opts)
	if err != nil {
		return err
	}
	return renderAtlasGoTemplate(w, "atlas-migrate-down-format", format, result)
}

// ValidateMigrateDownTemplate parses the migrate down --format template so an
// invalid template fails before any database work.
func ValidateMigrateDownTemplate(format string) error {
	return validateAtlasGoTemplate("atlas-migrate-down-format", format)
}

func buildAtlasMigrateDownResult(opts MigrateDownResultOptions) (atlasMigrateDownResult, error) {
	if opts.FS == nil {
		return atlasMigrateDownResult{}, errors.New("migrate down format requires migration filesystem")
	}
	filesByVersion, err := atlasMigrateApplyFilesByVersion(opts.FS)
	if err != nil {
		return atlasMigrateDownResult{}, err
	}
	env := atlasEnv{
		Driver: opts.Driver,
		URL:    atlasRedactedURL(opts.URL),
		Dir:    opts.Dir,
	}
	result := atlasMigrateDownResult{
		atlasEnv: env,
		Env:      env,
		Planned:  atlasMigrateApplyPendingFiles(filesByVersion, opts.PlannedVersions),
		Current:  atlasMigrateApplyVersionString(opts.CurrentVersion),
		Target:   atlasMigrateDownTarget(opts),
		Total:    len(opts.PlannedVersions),
		Start:    opts.StartedAt,
		End:      opts.EndedAt,
		Error:    opts.ErrorText,
	}
	if opts.Reverted {
		result.Reverted = atlasMigrateDownRevertedFiles(filesByVersion, opts)
	}
	return result, nil
}

// atlasMigrateDownTarget reports the version state the run aims at: the
// requested target version (or the current version when nothing is above it).
// A rollback that empties the revision history reports an empty Target,
// matching the empty Current a fresh database reports on apply.
func atlasMigrateDownTarget(opts MigrateDownResultOptions) string {
	target := int64(0)
	if opts.CurrentVersion <= opts.TargetVersion {
		target = opts.CurrentVersion
	} else if opts.TargetVersion > 0 {
		target = opts.TargetVersion
	}
	return atlasMigrateApplyVersionString(target)
}

func atlasMigrateDownRevertedFiles(
	files map[int64]atlasMigrateApplyFile,
	opts MigrateDownResultOptions,
) []*atlasMigrateDownRevertedFile {
	migrations := atlasMigrateApplyMigrationsByVersion(opts.Migrations)
	dialect := opts.Driver
	reverted := make([]*atlasMigrateDownRevertedFile, 0, len(opts.RevertedVersions)+1)
	for _, version := range opts.RevertedVersions {
		file, ok := files[version]
		if !ok {
			continue
		}
		revertedFile := &atlasMigrateDownRevertedFile{
			atlasMigrateApplyFile: file,
			Start:                 opts.StartedAt,
			End:                   opts.EndedAt,
		}
		if migration := migrations[version]; migration != nil {
			revertedFile.Applied = atlasMigrateApplySplitStatements(migration.DownSQL, dialect)
		}
		reverted = append(reverted, revertedFile)
	}
	if opts.DownError == nil {
		return reverted
	}
	failedVersion := atlasMigrateDownFailedVersion(opts)
	file, ok := files[failedVersion]
	if !ok {
		return reverted
	}
	failedFile := &atlasMigrateDownRevertedFile{
		atlasMigrateApplyFile: file,
		Start:                 opts.StartedAt,
		End:                   opts.EndedAt,
	}
	execErr := atlasMigrateApplyExecutionError(opts.DownError)
	if execErr == nil {
		execErr = &migrator.MigrationExecutionError{Err: opts.DownError}
	}
	if migration := migrations[failedVersion]; migration != nil {
		statements := atlasMigrateApplySplitStatements(migration.DownSQL, dialect)
		failedFile.Applied = appliedStatementsBeforeError(statements, execErr.StatementIndex)
	}
	failedFile.Error = &atlasMigrateApplyStatementError{
		Stmt: execErr.Statement,
		Text: execErr.Unwrap().Error(),
	}
	return append(reverted, failedFile)
}

// atlasMigrateDownFailedVersion extracts the failing version from the down
// engine's "failed to revert migration N" wrapping, falling back to the first
// planned version that was not fully reverted.
func atlasMigrateDownFailedVersion(opts MigrateDownResultOptions) int64 {
	matches := atlasMigrateDownFailedVersionRe.FindStringSubmatch(opts.DownError.Error())
	if len(matches) == 2 {
		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err == nil {
			return version
		}
	}
	for _, version := range opts.PlannedVersions {
		if !slices.Contains(opts.RevertedVersions, version) {
			return version
		}
	}
	return 0
}
