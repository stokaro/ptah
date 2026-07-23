package atlasreport

import (
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strconv"

	"github.com/stokaro/ptah/migration/migrator"
)

type MigrateStatusOptions struct {
	Driver string
	URL    string
	Dir    string
	FS     fs.FS
	Status *migrator.MigrationStatus
}

type MigrateStatus struct {
	Env       atlasEnv            `json:"Env"`
	Available []MigrateStatusFile `json:"Available,omitempty"`
	Applied   []MigrateStatusFile `json:"Applied,omitempty"`
	Pending   []MigrateStatusFile `json:"Pending,omitempty"`
	Current   string              `json:"Current,omitempty"`
	Next      string              `json:"Next,omitempty"`
	Status    string              `json:"Status,omitempty"`
}

type MigrateStatusFile struct {
	Name        string `json:"Name,omitempty"`
	Version     string `json:"Version,omitempty"`
	Description string `json:"Description,omitempty"`
	Type        string `json:"Type,omitempty"`
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
		Applied:   selectedMigrateStatusFiles(files, opts.Status.AppliedMigrations, "applied"),
		Pending:   selectedMigrateStatusFiles(files, opts.Status.PendingMigrations, ""),
		Current:   migrateStatusCurrent(opts.Status.CurrentVersion),
		Next:      migrateStatusNext(opts.Status.PendingMigrations),
		Status:    migrateStatusLabel(opts.Status),
	}
	return result, nil
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
			Description: file.Name,
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
	if status.DirtyRevision != nil {
		return "DIRTY"
	}
	if status.HasPendingChanges {
		return "PENDING"
	}
	return "OK"
}
