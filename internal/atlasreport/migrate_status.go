package atlasreport

import (
	"fmt"
	"io"
	"io/fs"
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
	RevisionVersions map[int64]string
}

type MigrateStatus struct {
	Env       atlasEnv                `json:"Env"`
	Available []MigrateStatusFile     `json:"Available,omitempty"`
	Applied   []MigrateStatusRevision `json:"Applied,omitempty"`
	Pending   []MigrateStatusFile     `json:"Pending,omitempty"`
	// OutOfOrder lists pending files whose version sorts below a version the
	// database has already applied. The pinned community binary reports these
	// separately from Pending and refuses to name a Next Version while any
	// exist, because "what runs next" has no answer under linear execution.
	OutOfOrder []MigrateStatusFile `json:"OutOfOrder,omitempty"`
	Current    string              `json:"Current"`
	Next       string              `json:"Next,omitempty"`
	Status     string              `json:"Status,omitempty"`
	Count      int                 `json:"Count,omitempty"`
	Total      int                 `json:"Total,omitempty"`
	Error      string              `json:"Error,omitempty"`
	SQL        string              `json:"SQL,omitempty"`
}

type MigrateStatusFile struct {
	Name        string `json:"Name,omitempty"`
	Version     string `json:"Version"`
	Description string `json:"Description,omitempty"`
	Type        string `json:"Type,omitempty"`
}

type MigrateStatusRevision struct {
	Version         string        `json:"Version"`
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
	files, err := migrateStatusFiles(opts.FS, opts.RevisionVersions)
	if err != nil {
		return MigrateStatus{}, err
	}
	result := MigrateStatus{
		Env: atlasEnv{
			Driver: opts.Driver,
			URL:    atlasRedactedURL(opts.URL),
			Dir:    opts.Dir,
		},
		Available:  files,
		Applied:    migrateStatusAppliedRevisions(files, opts.AppliedRevisions),
		Pending:    selectedMigrateStatusFiles(files, opts.Status.PendingMigrations, opts.Status.PendingMigrationKeys, ""),
		OutOfOrder: selectedMigrateStatusFiles(files, opts.Status.OutOfOrderMigrations, opts.Status.OutOfOrderMigrationKeys, ""),
		Current: migrateStatusCurrent(
			opts.Status,
			opts.AppliedRevisions,
			opts.RevisionVersions,
		),
		Next:   migrateStatusNext(opts.Status.PendingMigrations, opts.Status.PendingMigrationKeys),
		Status: migrateStatusLabel(opts.Status),
	}
	applyMigrateStatusPartial(&result, opts.Status.DirtyRevision, opts.AppliedRevisions)
	return result, nil
}

// applyMigrateStatusPartial fills Count, Total, Error and SQL from the dirty
// revision row. The last-row fallback preserves the public report adapter for
// callers that provide raw revision data without the matching status pointer.
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
// A retired opaque source identity can have runtime order zero and therefore
// need not be the final query row. MigrationStatus.DirtyRevision is the
// authoritative selector for that case.
func applyMigrateStatusPartial(
	result *MigrateStatus,
	dirty *migrator.MigrationRevision,
	revisions []migrator.MigrationRevision,
) {
	if dirty == nil && len(revisions) == 0 {
		return
	}
	partial := dirty
	if partial == nil {
		partial = &revisions[len(revisions)-1]
	}
	if partial.Error == "" && partial.Applied >= partial.Total {
		return
	}
	result.Count = partial.Applied
	result.Total = partial.Total
	result.Error = migrateStatusOneLine(partial.Error)
	result.SQL = migrateStatusOneLine(partial.ErrorStatement)
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
		version := revision.RevisionVersion()
		description := descriptions[version]
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

func migrateStatusFileDescriptions(files []MigrateStatusFile) map[string]string {
	descriptions := make(map[string]string, len(files))
	for _, file := range files {
		descriptions[file.Version] = file.Description
	}
	return descriptions
}

func migrateStatusFiles(fsys fs.FS, revisionVersions map[int64]string) ([]MigrateStatusFile, error) {
	discovered, err := migrator.DiscoverMigrationFiles(fsys, migrator.MigrationDirFormatAtlas)
	if err != nil {
		return nil, fmt.Errorf("discover Atlas migration files: %w", err)
	}
	files := make([]MigrateStatusFile, 0, len(discovered))
	for _, file := range discovered {
		if file.Direction == "down" {
			continue
		}
		version := file.RevisionVersion()
		if mapped, ok := revisionVersions[file.Version]; ok {
			version = mapped
		}
		files = append(files, MigrateStatusFile{
			Name:        file.Path,
			Version:     version,
			Description: atlasMigrationFileDescription(file.Path),
		})
	}
	return files, nil
}

func selectedMigrateStatusFiles(
	files []MigrateStatusFile,
	versions []int64,
	keys []string,
	fileType string,
) []MigrateStatusFile {
	out := make([]MigrateStatusFile, 0, len(versions))
	selected := migrateStatusVersionKeySet(versions, keys)
	for _, file := range files {
		if _, ok := selected[file.Version]; !ok {
			continue
		}
		file.Type = fileType
		out = append(out, file)
	}
	return out
}

func migrateStatusVersionKeySet(versions []int64, keys []string) map[string]struct{} {
	set := make(map[string]struct{}, len(versions))
	for index, version := range versions {
		key := strconv.FormatInt(version, 10)
		if index < len(keys) {
			key = keys[index]
		}
		set[key] = struct{}{}
	}
	return set
}

func migrateStatusCurrent(
	status *migrator.MigrationStatus,
	revisions []migrator.MigrationRevision,
	revisionVersions map[int64]string,
) string {
	if status.DirtyRevision != nil {
		if len(status.AppliedMigrations) == 0 {
			return "No migration applied yet"
		}
		return status.DirtyRevision.RevisionVersion()
	}
	if revisionVersions != nil && len(revisions) > 0 {
		current := revisions[0].RevisionVersion()
		for _, revision := range revisions[1:] {
			current = max(current, revision.RevisionVersion())
		}
		return current
	}
	if status.CurrentVersionKeySet {
		return status.CurrentVersionKey
	}
	if status.CurrentVersion <= 0 {
		return "No migration applied yet"
	}
	return strconv.FormatInt(status.CurrentVersion, 10)
}

func migrateStatusNext(pending []int64, keys []string) string {
	if len(pending) == 0 {
		return "Already at latest version"
	}
	if len(keys) > 0 {
		return keys[0]
	}
	return strconv.FormatInt(pending[0], 10)
}

func migrateStatusLabel(status *migrator.MigrationStatus) string {
	if status.HasPendingChanges {
		return "PENDING"
	}
	return "OK"
}
