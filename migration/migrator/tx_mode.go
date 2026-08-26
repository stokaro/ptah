package migrator

import (
	"fmt"
	"path"

	"go.5x5.cz/ptah/migration/migrationfile"
)

// MigrationTxMode controls how pending up migrations are wrapped in
// transactions.
type MigrationTxMode string

const (
	// MigrationTxModeFile wraps each pending migration file in its own
	// transaction unless the file explicitly selects none.
	MigrationTxModeFile MigrationTxMode = "file"
	// MigrationTxModeAll wraps all pending migration files in one transaction.
	MigrationTxModeAll MigrationTxMode = "all"
	// MigrationTxModeNone applies pending migration files without creating
	// migration transactions unless a file explicitly selects file.
	MigrationTxModeNone MigrationTxMode = "none"
)

func normalizeMigrationTxMode(mode MigrationTxMode) MigrationTxMode {
	if mode == "" {
		return MigrationTxModeFile
	}
	return mode
}

func resolveMigrationFileTxMode(global MigrationTxMode, file migrationfile.FileTxMode) (MigrationTxMode, error) {
	switch global {
	case MigrationTxModeFile, MigrationTxModeAll, MigrationTxModeNone:
	default:
		return "", fmt.Errorf("invalid tx-mode %q: expected file, all, or none", global)
	}

	switch file {
	case migrationfile.FileTxModeUnspecified:
		return global, nil
	case migrationfile.FileTxModeFile, migrationfile.FileTxModeNone:
		return MigrationTxMode(file), nil
	default:
		return "", fmt.Errorf("invalid migration file txmode %q: expected file, none, or empty", file)
	}
}

// ResolveAtlasDirectiveTxMode combines a global transaction mode with the
// mode an `-- atlas:txmode` directive selects, under the rule that family
// already answers to: the directive overrides the global mode, except under
// `all`, where the combination is refused rather than resolved.
//
// The directive is not only a migration file's: a declarative plan file
// carries the same header, and reading the same line to mean two different
// things depending on which artifact carried it is the defect this shared
// rule exists to prevent (stokaro/ptah#1700): the migrator resolves a
// migration file's directive and the CLI resolves a plan file's directive
// through this one function.
//
// source names the artifact for the refusal -- a migration file's path and
// description, or a plan file's path.
func ResolveAtlasDirectiveTxMode(
	global MigrationTxMode,
	file migrationfile.FileTxMode,
	source string,
) (MigrationTxMode, error) {
	if global == MigrationTxModeAll && file != migrationfile.FileTxModeUnspecified {
		return "", migrationfile.NewAtlasTxModeDirectiveError(fmt.Sprintf(
			"cannot set txmode directive to %q in %q when txmode %q is set globally",
			file,
			source,
			MigrationTxModeAll,
		))
	}
	return resolveMigrationFileTxMode(global, file)
}

func (m *Migrator) resolveUpMigrationTxMode(migration *Migration) (MigrationTxMode, error) {
	fileMode := migration.parsedUpTxModeForDialect(m.connectionDialect())
	if fileMode.Err != nil {
		return "", fileMode.Err
	}
	if m.txMode == MigrationTxModeAll && fileMode.Mode != migrationfile.FileTxModeUnspecified &&
		fileMode.Source != migrationfile.FileTxModeSourceAtlas {
		if fileMode.Source == migrationfile.FileTxModeSourcePtah && fileMode.Mode == migrationfile.FileTxModeNone {
			return "", fmt.Errorf("migration %d is marked no_transaction and cannot run with tx-mode all", migration.Version)
		}
		return "", fmt.Errorf(
			"migration %d selects txmode %q and cannot run with tx-mode all",
			migration.Version,
			fileMode.Mode,
		)
	}
	return ResolveAtlasDirectiveTxMode(
		m.txMode,
		fileMode.Mode,
		migrationTxModeSourceName(migration.upSourcePath, migration.Description),
	)
}

func (m *Migrator) resolveDownMigrationTxMode(migration *Migration) (MigrationTxMode, error) {
	fileMode := migration.parsedDownTxModeForDialect(m.connectionDialect())
	if fileMode.Err != nil {
		return "", fileMode.Err
	}
	return resolveMigrationFileTxMode(MigrationTxModeFile, fileMode.Mode)
}

func migrationTxModeSourceName(sourcePath, description string) string {
	if sourcePath != "" {
		return path.Base(sourcePath)
	}
	return description
}
