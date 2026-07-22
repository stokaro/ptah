package migrator

import (
	"fmt"
	"strings"
)

// MigrationTxMode controls how pending up migrations are wrapped in
// transactions.
type MigrationTxMode string

const (
	// MigrationTxModeFile wraps each pending migration file in its own
	// transaction unless the file opts out with no_transaction.
	MigrationTxModeFile MigrationTxMode = "file"
	// MigrationTxModeAll wraps all pending migration files in one transaction.
	MigrationTxModeAll MigrationTxMode = "all"
	// MigrationTxModeNone applies pending migration files without creating
	// migration transactions.
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
