package generator

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/capability"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// GenerateCheckpoint renders a full cumulative schema as a checkpoint migration
// body pair. The up body creates every object in dependency order; the down
// body drops them in reverse.
//
// It diffs the schema against an empty database, so it is deterministic and
// needs no live database connection. Callers obtain the schema either by
// introspecting a database that has the whole migration directory applied (and
// converting the result with internal/convert/dbschematogo) or from Go
// entities / schema files. An empty schema yields empty up and down bodies.
func GenerateCheckpoint(schema *goschema.Database, dialect string) (upSQL, downSQL string, err error) {
	if schema == nil {
		return "", "", fmt.Errorf("checkpoint schema is required")
	}

	empty := &dbschematypes.DBSchema{}
	diff := schemadiff.CompareWithDialect(schema, empty, dialect)
	spec, _, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
		Diff:         diff,
		Generated:    schema,
		DBSchema:     empty,
		Dialect:      dialect,
		Capabilities: capability.ForDialect(dialect),
	})
	if err != nil {
		return "", "", fmt.Errorf("generate checkpoint: %w", err)
	}
	return spec.UpSQL, spec.DownSQL, nil
}

// WriteCheckpointFiles writes a checkpoint migration pair
// (NNNNNNNNNN_description.checkpoint.up.sql and .checkpoint.down.sql) into
// outputDir at the given version and rewrites the directory's ptah.sum so the
// new files are integrity-protected. It refuses to overwrite existing files —
// the caller resolves a version above the existing history — and returns the
// two written paths.
func WriteCheckpointFiles(outputDir string, version int64, description, upSQL, downSQL string) (upPath, downPath string, err error) {
	if err := ensureMigrationOutputDir(outputDir); err != nil {
		return "", "", fmt.Errorf("failed to create output directory: %w", err)
	}

	upPath = filepath.Join(outputDir, migrator.GenerateCheckpointMigrationFileName(version, description, "up"))
	downPath = filepath.Join(outputDir, migrator.GenerateCheckpointMigrationFileName(version, description, "down"))
	if fileExists(upPath) || fileExists(downPath) {
		return "", "", fmt.Errorf("checkpoint files for version %d already exist", version)
	}

	if err := writeNewMigrationFile(upPath, upSQL); err != nil {
		return "", "", fmt.Errorf("failed to write checkpoint up file: %w", err)
	}
	if err := writeNewMigrationFile(downPath, downSQL); err != nil {
		_ = os.Remove(upPath)
		return "", "", fmt.Errorf("failed to write checkpoint down file: %w", err)
	}

	if _, err := migratesum.WriteWithFormat(outputDir, migrator.MigrationDirFormatPtah); err != nil {
		return "", "", fmt.Errorf("failed to rewrite ptah.sum: %w", err)
	}
	return upPath, downPath, nil
}
