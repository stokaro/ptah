package atlasreport

import (
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.5x5.cz/ptah/migration/migrator"
)

type MigrateStatusOptions struct {
	Driver           string
	URL              string
	Dir              string
	FS               fs.FS
	Status           *migrator.MigrationStatus
	AppliedRevisions []migrator.MigrationRevision
}

type MigrateStatus struct {
	Env       atlasEnv                `json:"Env"`
	Available []MigrateStatusFile     `json:"Available,omitempty"`
	Applied   []MigrateStatusRevision `json:"Applied,omitempty"`
	Pending   []MigrateStatusFile     `json:"Pending,omitempty"`
	Current   string                  `json:"Current,omitempty"`
	Next      string                  `json:"Next,omitempty"`
	Status    string                  `json:"Status,omitempty"`
	Count     int                     `json:"Count,omitempty"`
	Total     int                     `json:"Total,omitempty"`
	Error     string                  `json:"Error,omitempty"`
	SQL       string                  `json:"SQL,omitempty"`
}

type MigrateStatusFile struct {
	Name        string `json:"Name,omitempty"`
	Version     string `json:"Version,omitempty"`
	Description string `json:"Description,omitempty"`
	Type        string `json:"Type,omitempty"`
}

type MigrateStatusRevision struct {
	Version         string        `json:"Version,omitempty"`
	Description     string        `json:"Description,omitempty"`
	Type            string        `json:"Type,omitempty"`
	Applied         int           `json:"Applied"`
	Total           int           `json:"Total"`
	ExecutedAt      time.Time     `json:"ExecutedAt,omitzero"`
	ExecutionTime   time.Duration `json:"ExecutionTime"`
	Error           string        `json:"Error,omitempty"`
	ErrorStmt       string        `json:"ErrorStmt,omitempty"`
	OperatorVersion string        `json:"OperatorVersion,omitempty"`
}

func WriteMigrateStatusFormat(w io.Writer, format string, opts MigrateStatusOptions) error {
	result, err := NewMigrateStatus(opts)
	if err != nil {
		return err
	}
	return renderAtlasGoTemplate(w, "atlas-migrate-status-format", format, result)
}

func ValidateMigrateStatusTemplate(format string) error {
	return validateAtlasGoTemplate("atlas-migrate-status-format", format)
}

func NewMigrateStatus(opts MigrateStatusOptions) (MigrateStatus, error) {
	if opts.Status == nil {
		return MigrateStatus{}, fmt.Errorf("migrate status format requires migration status")
	}
	files, err := migrateStatusFiles(opts.FS)
	if err != nil {
		return MigrateStatus{}, err
	}
	result := MigrateStatus{
		Env: atlasEnv{
			Driver: opts.Driver,
			URL:    atlasRedactedURL(opts.URL),
			Dir:    opts.Dir,
		},
		Available: files,
		Applied:   migrateStatusAppliedRevisions(files, opts.AppliedRevisions),
		Pending:   selectedMigrateStatusFiles(files, opts.Status.PendingMigrations, ""),
		Current:   migrateStatusCurrent(opts.Status.CurrentVersion),
		Next:      migrateStatusNext(opts.Status.PendingMigrations),
		Status:    migrateStatusLabel(opts.Status),
	}
	applyMigrateStatusPartial(&result, opts.AppliedRevisions)
	return result, nil
}

// applyMigrateStatusPartial fills Count, Total, Error and SQL from the highest
// revision row when that row is half-applied.
//
// Count/Total are the statement counters the report prints as "(N statements
// applied)" and "(M statements left)"; Error/SQL are the failure the "Last
// migration attempt had errors" block names. Measured against the pinned
// community binary v1.3.0: on a clean database all four are absent from
// `--format '{{ json . }}'`, and on a database whose second migration failed
// mid-body they read Count=1, Total=2, Error and SQL. Before this they were
// declared and never written, so `{{ .Total }}` rendered 0 on a wedged database
// and the failing statement was unreachable from a template.
//
// The highest row is the selector because the revision query orders by version
// and a run stops at its first failure, so a half-applied row is the last one.
func applyMigrateStatusPartial(result *MigrateStatus, revisions []migrator.MigrationRevision) {
	if len(revisions) == 0 {
		return
	}
	last := revisions[len(revisions)-1]
	if last.Error == "" && last.Applied >= last.Total {
		return
	}
	result.Count = last.Applied
	result.Total = last.Total
	result.Error = migrateStatusOneLine(last.Error)
	result.SQL = migrateStatusOneLine(last.ErrorStatement)
}

// migrateStatusOneLine folds a stored newline into a space.
//
// The report's own Error and SQL are one line each; the revision row's are not
// necessarily, and a multi-line value would otherwise break the `  -- ` column
// a parser keys on. Measured on the pinned community binary v1.3.0 against a
// migration whose failing statement spans four lines: `{{ printf "%q" .SQL }}`
// reads "CREATE TABLE a (   id integer,   name text );" while the same row read
// as `{{ range .Applied }}{{ printf "%q" .ErrorStmt }}` keeps its "\n" — so the
// fold belongs to the top-level report field, not to the revision, and it is a
// newline-to-space substitution rather than a collapse of runs of whitespace.
func migrateStatusOneLine(value string) string {
	return strings.ReplaceAll(value, "\n", " ")
}

func migrateStatusAppliedRevisions(
	files []MigrateStatusFile,
	revisions []migrator.MigrationRevision,
) []MigrateStatusRevision {
	out := make([]MigrateStatusRevision, 0, len(revisions))
	descriptions := migrateStatusFileDescriptions(files)
	for _, revision := range revisions {
		version := strconv.FormatInt(revision.Version, 10)
		description := descriptions[revision.Version]
		if description == "" {
			description = revision.Description
		}
		out = append(out, MigrateStatusRevision{
			Version:         version,
			Description:     description,
			Type:            revision.AtlasType.String(),
			Applied:         revision.Applied,
			Total:           revision.Total,
			ExecutedAt:      revision.AppliedAt,
			ExecutionTime:   revision.ExecutionTime,
			Error:           revision.Error,
			ErrorStmt:       revision.ErrorStatement,
			OperatorVersion: revision.OperatorVersion,
		})
	}
	return out
}

func migrateStatusFileDescriptions(files []MigrateStatusFile) map[int64]string {
	descriptions := make(map[int64]string, len(files))
	for _, file := range files {
		descriptions[migrateStatusFileVersion(file)] = file.Description
	}
	return descriptions
}

func migrateStatusFiles(fsys fs.FS) ([]MigrateStatusFile, error) {
	discovered, err := migrator.DiscoverMigrationFiles(fsys, migrator.MigrationDirFormatAtlas)
	if err != nil {
		return nil, fmt.Errorf("discover Atlas migration files: %w", err)
	}
	files := make([]MigrateStatusFile, 0, len(discovered))
	for _, file := range discovered {
		if file.Repeatable || file.Direction == "down" {
			continue
		}
		files = append(files, MigrateStatusFile{
			Name:        file.Path,
			Version:     strconv.FormatInt(file.Version, 10),
			Description: atlasMigrationFileDescription(file.Path),
		})
	}
	return files, nil
}

func selectedMigrateStatusFiles(
	files []MigrateStatusFile,
	versions []int64,
	fileType string,
) []MigrateStatusFile {
	out := make([]MigrateStatusFile, 0, len(versions))
	for _, file := range files {
		if !slices.Contains(versions, migrateStatusFileVersion(file)) {
			continue
		}
		file.Type = fileType
		out = append(out, file)
	}
	return out
}

func migrateStatusFileVersion(file MigrateStatusFile) int64 {
	version, err := strconv.ParseInt(file.Version, 10, 64)
	if err != nil {
		return 0
	}
	return version
}

func migrateStatusCurrent(version int64) string {
	if version <= 0 {
		return "No migration applied yet"
	}
	return strconv.FormatInt(version, 10)
}

func migrateStatusNext(pending []int64) string {
	if len(pending) == 0 {
		return "Already at latest version"
	}
	return strconv.FormatInt(pending[0], 10)
}

func migrateStatusLabel(status *migrator.MigrationStatus) string {
	if status.HasPendingChanges {
		return "PENDING"
	}
	return "OK"
}
