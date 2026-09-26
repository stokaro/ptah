package atlas

import (
	"fmt"
	"slices"

	"ptah.run/migration/migrator"
)

// The --tx-mode values each verb accepts, spelled exactly. The pinned
// community binary v1.3.0 accepts `none` and `file` on `schema apply` and adds
// `all` on `migrate apply`. It compares the value as written, so `ALL`, `File`
// and an explicit empty value are refused, each as `unknown tx-mode "<value>"`.
// Measured on 2026-09-26 on PostgreSQL 18 (stokaro/ptah#3689).
//
// Leaving out `all` on `schema apply` removes nothing: a schema apply runs one
// plan, and Ptah runs it in one transaction under `file` and `all` alike.
var (
	atlasSchemaApplyTxModes  = []string{string(migrator.MigrationTxModeNone), string(migrator.MigrationTxModeFile)}
	atlasMigrateApplyTxModes = []string{
		string(migrator.MigrationTxModeNone),
		string(migrator.MigrationTxModeFile),
		string(migrator.MigrationTxModeAll),
	}
)

// parseAtlasTxMode returns value as a transaction mode when accepted spells it
// exactly, and the pinned binary's refusal otherwise.
//
// Native `ptah` reads the same flag through
// [ptah.run/internal/cli/internal/migrateflags.ParseMigrationTxMode], which
// accepts any case and names the modes it expects. That is the clearer
// sentence where there is no community answer to match.
//
// The pinned binary checks the value on `migrate apply` only when it executes a
// file: its `--dry-run` accepts `--tx-mode bogus` at exit 0, and a real run
// prints its progress header and creates the revision table before refusing
// the value. A dry run that passes where the real run is refused is the defect
// the dry run exists to catch, so this surface refuses both before it connects.
func parseAtlasTxMode(value string, accepted []string) (migrator.MigrationTxMode, error) {
	if !slices.Contains(accepted, value) {
		return "", fmt.Errorf("unknown tx-mode %q", value)
	}
	return migrator.MigrationTxMode(value), nil
}
