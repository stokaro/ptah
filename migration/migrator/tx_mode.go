package migrator

import (
	"fmt"
	"path"
	"strings"
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

// ParseMigrationTxMode parses the Atlas-compatible migration transaction mode.
func ParseMigrationTxMode(value string) (MigrationTxMode, error) {
	mode := normalizeMigrationTxMode(MigrationTxMode(strings.ToLower(strings.TrimSpace(value))))
	switch mode {
	case MigrationTxModeFile, MigrationTxModeAll, MigrationTxModeNone:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid tx-mode %q: expected file, all, or none", value)
	}
}

func normalizeMigrationTxMode(mode MigrationTxMode) MigrationTxMode {
	if mode == "" {
		return MigrationTxModeFile
	}
	return mode
}

func resolveMigrationFileTxMode(global MigrationTxMode, file MigrationFileTxMode) (MigrationTxMode, error) {
	switch global {
	case MigrationTxModeFile, MigrationTxModeAll, MigrationTxModeNone:
	default:
		return "", fmt.Errorf("invalid tx-mode %q: expected file, all, or none", global)
	}

	switch file {
	case MigrationFileTxModeUnspecified:
		return global, nil
	case MigrationFileTxModeFile, MigrationFileTxModeNone:
		return MigrationTxMode(file), nil
	default:
		return "", fmt.Errorf("invalid migration file txmode %q: expected file, none, or empty", file)
	}
}

func (m *Migrator) resolveUpMigrationTxMode(migration *Migration) (MigrationTxMode, error) {
	fileMode := migration.parsedUpTxModeForDialect(m.connectionDialect())
	if fileMode.err != nil {
		return "", fileMode.err
	}
	if m.txMode == MigrationTxModeAll && fileMode.mode != MigrationFileTxModeUnspecified {
		if fileMode.source == migrationFileTxModeSourcePtah && fileMode.mode == MigrationFileTxModeNone {
			return "", fmt.Errorf("migration %d is marked no_transaction and cannot run with tx-mode all", migration.Version)
		}
		if fileMode.source != migrationFileTxModeSourceAtlas {
			return "", fmt.Errorf(
				"migration %d selects txmode %q and cannot run with tx-mode all",
				migration.Version,
				fileMode.mode,
			)
		}
		return "", newAtlasTxModeDirectiveError(
			"cannot set txmode directive to %q in %q when txmode %q is set globally",
			fileMode.mode,
			migrationTxModeSourceName(migration.upSourcePath, migration.Description),
			MigrationTxModeAll,
		)
	}
	return resolveMigrationFileTxMode(m.txMode, fileMode.mode)
}

func (m *Migrator) resolveDownMigrationTxMode(migration *Migration) (MigrationTxMode, error) {
	fileMode := migration.parsedDownTxModeForDialect(m.connectionDialect())
	if fileMode.err != nil {
		return "", fileMode.err
	}
	return resolveMigrationFileTxMode(MigrationTxModeFile, fileMode.mode)
}

func migrationTxModeSourceName(sourcePath, description string) string {
	if sourcePath != "" {
		return path.Base(sourcePath)
	}
	return description
}
