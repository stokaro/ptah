package importer

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// maxPtahVersion is the largest version Ptah's exactly-10-digit file-name format
// can represent (migration/migrator/util.go fileNameRe uses \d{10}).
const maxPtahVersion int64 = 9999999999

// emptyDownSQL is written for the down file of a source migration that has no
// rollback, so every imported migration is a complete Ptah up/down pair.
const emptyDownSQL = "-- No rollback was provided by the source migration.\n"

// fallbackMigrationName is used when a source name sanitizes to empty (Ptah
// rejects an empty description).
const fallbackMigrationName = "migration"

// descSanitizeRE mirrors the character class GenerateMigrationFileName strips, so
// the importer can tell in advance when a source name would sanitize to empty.
var descSanitizeRE = regexp.MustCompile(`[^a-z0-9_]`)

// EmitResult reports the files an import wrote (or, in dry-run, would write).
type EmitResult struct {
	// Files are the migration file names written into the output directory, in
	// apply order (each migration contributes an up and a down file).
	Files []string
	// SumFile is the integrity file name refreshed after writing (empty in
	// dry-run).
	SumFile string
	// Remapped reports whether source versions were reassigned to sequential
	// Ptah versions because at least one did not fit the 10-digit format.
	Remapped bool
}

type plannedFile struct {
	name    string
	content []byte
}

// Emit writes each migration as a Ptah NNNNNNNNNN_description.up.sql / .down.sql
// pair into outDir and refreshes ptah.sum. Migrations must already be Normalized.
//
// Source versions are preserved when every version fits Ptah's 10-digit format;
// if any is too wide (for example a 14-digit golang-migrate timestamp), all
// migrations are reassigned to sequential Ptah versions in their existing order,
// carrying the original version into the description so history stays traceable.
// A source name that sanitizes to empty falls back to "migration". A repeatable
// source migration (Ptah has no such concept) is imported as a one-time
// migration ordered after every versioned one, named "repeatable_<name>". A
// source whole-migration no-transaction mode is translated onto both Ptah
// directions so importing never loses its execution semantics.
//
// The whole plan is validated (every generated file name round-trips through
// Ptah's reader) BEFORE any file is written, and a mid-write failure removes the
// files already written, so a failed import never leaves an unreadable,
// retry-blocking half-result. With dryRun it returns the planned file names and
// writes nothing. It refuses to overwrite an existing target file.
func Emit(outDir string, migrations []SourceMigration, opts Options) (*EmitResult, error) {
	remap := needsVersionRemap(migrations)
	result := &EmitResult{Remapped: remap}

	planned := make([]plannedFile, 0, len(migrations)*2)
	var maxVersion int64 // highest output version assigned so far (versioned sort first)
	for index, migration := range migrations {
		// Preserve the source version when it fits Ptah's 10-digit format;
		// otherwise reassign sequentially (order is already normalized), folding
		// the original version into the description so history stays traceable.
		version := migration.Version
		description := migration.Name
		switch {
		case migration.Repeatable:
			// Ptah has no repeatable-migration concept, so a source repeatable is
			// imported as a one-time migration ordered after every versioned one
			// (Normalize sorts repeatables last). A later content change in the
			// source becomes a new Ptah migration rather than a re-run.
			maxVersion++
			version = maxVersion
			description = "repeatable_" + migration.Name
		case remap:
			version = int64(index + 1)
			description = fmt.Sprintf("v%d_%s", migration.Version, migration.Name)
			maxVersion = version
		default:
			maxVersion = max(maxVersion, version)
		}
		if sanitizedDescriptionIsEmpty(description) {
			description = fallbackMigrationName
		}
		upName := migrator.GenerateMigrationFileName(version, description, "up")
		downName := migrator.GenerateMigrationFileName(version, description, "down")
		// Ptah's reader is strict (exactly 10 digits, non-empty name); confirm the
		// generated names round-trip before committing to writing anything.
		for _, name := range []string{upName, downName} {
			if _, err := migrator.ParseMigrationFileName(name); err != nil {
				return nil, fmt.Errorf("cannot import migration %q as Ptah file %q: %w", migration.Name, name, err)
			}
		}
		downSQL := migration.DownSQL
		if downSQL == "" {
			downSQL = emptyDownSQL
		}
		upSQL := migration.UpSQL
		if migration.NoTransaction {
			upSQL = addImportedNoTransactionDirective(upSQL)
			downSQL = addImportedNoTransactionDirective(downSQL)
		}
		planned = append(planned,
			plannedFile{upName, []byte(upSQL)},
			plannedFile{downName, []byte(downSQL)},
		)
		result.Files = append(result.Files, upName, downName)
	}

	if opts.DryRun {
		return result, nil
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	for _, file := range planned {
		if _, err := os.Stat(filepath.Join(outDir, file.name)); err == nil {
			return nil, fmt.Errorf("refusing to overwrite existing migration file %q in %s", file.name, outDir)
		}
	}
	written := make([]string, 0, len(planned))
	for _, file := range planned {
		path := filepath.Join(outDir, file.name)
		if err := os.WriteFile(path, file.content, 0o644); err != nil { // #nosec G306 -- migration files are shared, 0644 like the rest
			cleanupFiles(outDir, written)
			return nil, fmt.Errorf("write %q: %w", file.name, err)
		}
		written = append(written, file.name)
	}

	if _, err := migratesum.WriteWithFormat(outDir, migrator.MigrationDirFormatPtah); err != nil {
		cleanupFiles(outDir, written)
		return nil, fmt.Errorf("write integrity file: %w", err)
	}
	sumName, err := migratesum.FileNameForFormat(migrator.MigrationDirFormatPtah)
	if err != nil {
		return nil, err
	}
	result.SumFile = sumName
	return result, nil
}

func addImportedNoTransactionDirective(sql string) string {
	return "-- +ptah " + migrator.DirectiveNoTransaction + "\n" + sql
}

// needsVersionRemap reports whether any non-repeatable version falls outside
// Ptah's representable 10-digit range, forcing a sequential reassignment.
func needsVersionRemap(migrations []SourceMigration) bool {
	for _, migration := range migrations {
		if migration.Repeatable {
			continue
		}
		if migration.Version < 1 || migration.Version > maxPtahVersion {
			return true
		}
	}
	return false
}

func sanitizedDescriptionIsEmpty(name string) bool {
	sanitized := descSanitizeRE.ReplaceAllString(strings.ReplaceAll(strings.ToLower(name), " ", "_"), "")
	return sanitized == ""
}

func cleanupFiles(dir string, names []string) {
	for _, name := range names {
		_ = os.Remove(filepath.Join(dir, name))
	}
}
