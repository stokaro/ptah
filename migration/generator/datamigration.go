package generator

import (
	"fmt"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// WriteDataMigrationFiles writes an ordinary migration pair
// (NNNNNNNNNN_description.up.sql and .down.sql) into outputDir at the given
// version and rewrites the directory's ptah.sum so the new files are
// integrity-protected. It refuses to overwrite existing files — the caller
// resolves a version above the existing history — and returns the two written
// paths.
//
// Unlike WriteCheckpointFiles it produces ordinary (non-checkpoint) migration
// files, so the generated pair is applied and rolled back like any hand-written
// migration and takes part in normal history. It is the writer for the
// declarative reference/seed data pipeline (ptah migrations data): the SQL
// bodies are the concatenated datadiff DML for every managed table.
func WriteDataMigrationFiles(outputDir string, version int64, description, upSQL, downSQL string) (upPath, downPath string, err error) {
	return writeMigrationPair(outputDir, version, description, upSQL, downSQL, "migration", migrator.GenerateMigrationFileName)
}

// writeMigrationPair writes an up/down migration file pair, named by nameFor,
// into outputDir at version and then rewrites ptah.sum so the pair is
// integrity-protected. It refuses to overwrite an existing pair. kind labels
// the pair in error messages (for example "migration" or "checkpoint") so
// callers surface a wording that matches the file kind they asked for. It is
// the shared core of WriteDataMigrationFiles and WriteCheckpointFiles, which
// differ only in the file-name scheme and that label.
func writeMigrationPair(
	outputDir string,
	version int64,
	description, upSQL, downSQL, kind string,
	nameFor func(version int64, description, direction string) string,
) (upPath, downPath string, err error) {
	if err := ensureMigrationOutputDir(outputDir); err != nil {
		return "", "", fmt.Errorf("failed to create output directory: %w", err)
	}

	upPath = filepath.Join(outputDir, nameFor(version, description, "up"))
	downPath = filepath.Join(outputDir, nameFor(version, description, "down"))
	if fileExists(upPath) || fileExists(downPath) {
		return "", "", fmt.Errorf("%s files for version %d already exist", kind, version)
	}

	if err := writeNewMigrationFile(upPath, upSQL); err != nil {
		return "", "", fmt.Errorf("failed to write %s up file: %w", kind, err)
	}
	if err := writeNewMigrationFile(downPath, downSQL); err != nil {
		_ = os.Remove(upPath)
		return "", "", fmt.Errorf("failed to write %s down file: %w", kind, err)
	}

	if _, err := migratesum.WriteWithFormat(outputDir, migrator.MigrationDirFormatPtah); err != nil {
		return "", "", fmt.Errorf("failed to rewrite ptah.sum: %w", err)
	}
	return upPath, downPath, nil
}
