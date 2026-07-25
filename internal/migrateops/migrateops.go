// Package migrateops implements checksum-aware maintenance of a migration
// directory: removing, rebasing (re-timestamping to the end of history), and
// re-hashing after an in-place edit. Every mutating operation regenerates the
// integrity file (ptah.sum / atlas.sum) atomically so the directory stays
// consistent, and callers can layer an applied-migration guard on top with
// EnsureNotApplied so already-applied history is never rewritten. It backs the
// `ptah migrations rm | edit | rebase` commands (#662).
package migrateops

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

// Result reports the outcome of a maintenance operation for CLI display.
type Result struct {
	// Files are the migration file names the operation touched (deleted or
	// renamed), in sorted order.
	Files []string
	// SumFile is the integrity file name that was rewritten.
	SumFile string
}

// Remove deletes every file belonging to version (its up and down pair) from dir
// and rewrites the integrity file. It errors if the version is not present.
func Remove(dir string, version int64, format migrator.MigrationDirFormat) (*Result, error) {
	format, err := resolveFormat(dir, format)
	if err != nil {
		return nil, err
	}
	matched, _, err := versionedFiles(dir, version, format)
	if err != nil {
		return nil, err
	}
	if len(matched) == 0 {
		return nil, fmt.Errorf("migration version %d not found in %s", version, dir)
	}

	removed := make([]string, 0, len(matched))
	for _, f := range matched {
		name := filepath.Base(f.Path)
		if err := os.Remove(filepath.Join(dir, filepath.FromSlash(f.Path))); err != nil {
			return nil, fmt.Errorf("remove %s: %w", name, err)
		}
		removed = append(removed, name)
	}
	slices.Sort(removed)
	return rehash(dir, format, removed)
}

// Rebase moves the migration at version to the end of history by re-timestamping
// its up/down pair to a fresh version greater than every existing version, then
// rewrites the integrity file. It returns the new version. Because it changes the
// version, it is only valid for an unapplied migration (the caller enforces that
// with EnsureNotApplied).
func Rebase(dir string, version int64, format migrator.MigrationDirFormat) (newVersion int64, _ *Result, err error) {
	format, err = resolveFormat(dir, format)
	if err != nil {
		return 0, nil, err
	}
	matched, all, err := versionedFiles(dir, version, format)
	if err != nil {
		return 0, nil, err
	}
	if len(matched) == 0 {
		return 0, nil, fmt.Errorf("migration version %d not found in %s", version, dir)
	}

	maxVersion := int64(0)
	for _, f := range all {
		if f.Version > maxVersion {
			maxVersion = f.Version
		}
	}
	if version == maxVersion {
		return 0, nil, fmt.Errorf("migration version %d is already last; rebase would not move it", version)
	}
	newVersion = migrator.GetNextMigrationVersion()
	if newVersion <= maxVersion {
		newVersion = maxVersion + 1
	}

	// Plan every rename and validate the new names before touching any file, so a
	// name Ptah's reader would reject (or a collision) fails the whole operation
	// rather than leaving a half-renamed, unreadable directory.
	type rename struct{ oldRel, newRel string }
	plan := make([]rename, 0, len(matched))
	touched := make([]string, 0, len(matched))
	for _, f := range matched {
		oldRel := filepath.FromSlash(f.Path)
		newName, err := swapVersion(filepath.Base(oldRel), newVersion)
		if err != nil {
			return 0, nil, err
		}
		if err := validateName(newName, f.Format); err != nil {
			return 0, nil, fmt.Errorf("rebased name %q is not a valid %s migration file: %w", newName, f.Format, err)
		}
		// Keep the migration in its subdirectory (if any); only the version changes.
		newRel := filepath.Join(filepath.Dir(oldRel), newName)
		if _, statErr := os.Stat(filepath.Join(dir, newRel)); statErr == nil {
			return 0, nil, fmt.Errorf("rebased name %q already exists in %s", newRel, dir)
		}
		plan = append(plan, rename{oldRel: oldRel, newRel: newRel})
		touched = append(touched, filepath.ToSlash(newRel))
	}
	for _, r := range plan {
		if err := os.Rename(filepath.Join(dir, r.oldRel), filepath.Join(dir, r.newRel)); err != nil {
			return 0, nil, fmt.Errorf("rename %s: %w", r.oldRel, err)
		}
	}
	slices.Sort(touched)
	res, err := rehash(dir, format, touched)
	return newVersion, res, err
}

// Rehash rewrites the integrity file for dir without otherwise changing it. It is
// the step an interactive edit runs after the migration files are saved in place.
func Rehash(dir string, format migrator.MigrationDirFormat) (*Result, error) {
	format, err := resolveFormat(dir, format)
	if err != nil {
		return nil, err
	}
	return rehash(dir, format, nil)
}

// resolveFormat turns an "auto" (or empty) format into the concrete ptah/atlas
// format for dir, so the rewritten integrity file and its reported name match. It
// follows the same signals the rest of the integrity subsystem uses: an existing
// sum file wins (and both present is an ambiguity the operator must resolve, as
// verification requires), otherwise the format is detected from the migration
// files' content — keeping rm/edit/rebase consistent with hash and validate.
func resolveFormat(dir string, format migrator.MigrationDirFormat) (migrator.MigrationDirFormat, error) {
	normalized, err := migrator.ParseMigrationDirFormat(string(format))
	if err != nil {
		return "", err
	}
	if normalized != migrator.MigrationDirFormatAuto {
		return normalized, nil
	}
	hasPtah := fileExists(filepath.Join(dir, migratesum.FileName))
	hasAtlas := fileExists(filepath.Join(dir, migratesum.AtlasFileName))
	switch {
	case hasPtah && hasAtlas:
		return "", fmt.Errorf("both %s and %s exist in %s; choose --dir-format ptah or --dir-format atlas", migratesum.FileName, migratesum.AtlasFileName, dir)
	case hasAtlas:
		return migrator.MigrationDirFormatAtlas, nil
	case hasPtah:
		return migrator.MigrationDirFormatPtah, nil
	}
	// No sum file yet: detect from the files themselves, as discovery does.
	files, err := migrator.DiscoverMigrationFiles(os.DirFS(dir), migrator.MigrationDirFormatAuto)
	if err != nil {
		return "", err
	}
	for _, f := range files {
		if f.Format == migrator.MigrationDirFormatAtlas {
			return migrator.MigrationDirFormatAtlas, nil
		}
	}
	return migrator.MigrationDirFormatPtah, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// LocatePair returns the up and down file paths (under dir) for version. Either
// path is empty when that direction is absent. It errors if the version has no
// files. It is used by the edit command to find the pair to open or overwrite.
func LocatePair(dir string, version int64, format migrator.MigrationDirFormat) (upPath, downPath string, err error) {
	format, err = resolveFormat(dir, format)
	if err != nil {
		return "", "", err
	}
	matched, _, err := versionedFiles(dir, version, format)
	if err != nil {
		return "", "", err
	}
	if len(matched) == 0 {
		return "", "", fmt.Errorf("migration version %d not found in %s", version, dir)
	}
	for _, f := range matched {
		p := filepath.Join(dir, filepath.FromSlash(f.Path))
		if f.Direction == "down" {
			downPath = p
		} else {
			upPath = p
		}
	}
	return upPath, downPath, nil
}

// EnsureNotApplied returns an error when version is present in applied, so a
// maintenance operation never rewrites already-applied history. The caller skips
// this check when the operator passes --force.
func EnsureNotApplied(applied []int64, version int64) error {
	if slices.Contains(applied, version) {
		return fmt.Errorf("migration version %d is already applied; refusing to modify applied history (use --force to override)", version)
	}
	return nil
}

// versionedFiles returns the versioned files matching version and all versioned
// files in dir. Repeatable (non-versioned) migrations are ignored.
func versionedFiles(dir string, version int64, format migrator.MigrationDirFormat) (matched, all []migrator.MigrationFile, err error) {
	if info, statErr := os.Stat(dir); statErr != nil || !info.IsDir() {
		return nil, nil, fmt.Errorf("migrations directory %s is not accessible", dir)
	}
	files, err := migrator.DiscoverMigrationFiles(os.DirFS(dir), format)
	if err != nil {
		return nil, nil, err
	}
	for _, f := range files {
		if f.Repeatable {
			continue
		}
		all = append(all, f)
		if f.Version == version {
			matched = append(matched, f)
		}
	}
	return matched, all, nil
}

func rehash(dir string, format migrator.MigrationDirFormat, touched []string) (*Result, error) {
	if _, err := migratesum.WriteWithFormat(dir, format); err != nil {
		return nil, fmt.Errorf("write integrity file: %w", err)
	}
	sumName, err := migratesum.FileNameForFormat(format)
	if err != nil {
		return nil, err
	}
	return &Result{Files: touched, SumFile: sumName}, nil
}

// swapVersion replaces the leading numeric run of a migration file name with
// newVersion, preserving the original digit width so the file keeps its format's
// version layout (Ptah's fixed 10 digits, or an Atlas timestamp's width).
func swapVersion(base string, newVersion int64) (string, error) {
	i := 0
	for i < len(base) && base[i] >= '0' && base[i] <= '9' {
		i++
	}
	if i == 0 {
		return "", fmt.Errorf("migration file %q has no leading version number", base)
	}
	return fmt.Sprintf("%0*d%s", i, newVersion, base[i:]), nil
}

// validateName confirms a generated file name round-trips through the reader for
// the file's own format, so an out-of-range version (for example one too wide for
// Ptah's exactly-10-digit format) is rejected before any file is renamed.
func validateName(name string, format migrator.MigrationDirFormat) error {
	if format == migrator.MigrationDirFormatAtlas {
		_, err := migrator.ParseAtlasMigrationFileName(name)
		return err
	}
	_, err := migrator.ParseMigrationFileName(name)
	return err
}
