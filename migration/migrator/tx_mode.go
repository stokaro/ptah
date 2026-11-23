package migrator

import (
	"fmt"
	"path"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/servercode"
	"ptah.run/migration/migrationfile"
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
			return "", fmt.Errorf("migration %d is marked no_transaction and cannot run with tx-mode all", m.versionText(migration.Version))
		}
		return "", fmt.Errorf(
			"migration %d selects txmode %q and cannot run with tx-mode all",
			m.versionText(migration.Version),
			fileMode.Mode,
		)
	}
	mode, err := ResolveAtlasDirectiveTxMode(
		m.txMode,
		fileMode.Mode,
		migrationTxModeSourceName(migration.upSourcePath, migration.Description),
	)
	if err != nil {
		return "", err
	}
	if m.replaysWithoutATransaction(migration, m.fileModeRefusesBeforeTheRun(migration)) {
		return MigrationTxModeNone, nil
	}
	return m.effectiveTxMode(mode), nil
}

// replaysWithoutATransaction is the rule a replay under
// [Migrator.WithAppliedHistoryReplay] chooses an up file's mode by. It reports
// whether migration replays without a transaction, given refused, whether
// tx-mode file refused the file. A file with no txmode directive that a run
// without a transaction accepts ran without one, since it ran at all, and
// replays without one. A file whose directive names a mode replays in it, a
// file both modes refuse keeps tx-mode file and its refusal, and a run that
// replays nothing keeps its mode for every file.
//
// Three refusals count:
//   - the checks a run asks of every file before the first of them runs
//     ([Migrator.fileModeRefusesBeforeTheRun]);
//   - on MySQL and MariaDB, the checks of the database's state the
//     transaction witness asks before the file's first statement, such as a
//     table that is not InnoDB ([Migrator.requireTransactionalTarget]);
//   - a server's refusal of a statement inside the file's one transaction on
//     a target whose rollback undoes schema changes
//     ([Migrator.refusedInItsTransaction]). The transaction rolls back, and
//     the replay runs the file again without one. Sequence values and session
//     state a rollback does not undo, such as a prepared statement or an
//     advisory lock, stay, so a file that depends on them may not replay.
func (m *Migrator) replaysWithoutATransaction(migration *Migration, refused bool) bool {
	return refused && m.replaysHistory &&
		migration.parsedUpTxModeForDialect(m.connectionDialect()).Mode == migrationfile.FileTxModeUnspecified &&
		m.refuseBodyForItsMode(migration, MigrationDirectionUp, ownTransactionRun{mode: MigrationTxModeNone}) == nil
}

// fileModeRefusesBeforeTheRun reports whether a run in tx-mode file refuses
// migration's up body for being in a transaction, by the checks a run asks of
// every file before the first of them runs: [Migrator.refuseBodyForItsMode]
// and [Migrator.refuseWhatATransactionRefuses].
//
// A file that runs without a transaction in tx-mode file anyway, because the
// target runs each query on its own or the file runs alone outside one (see
// [Migrator.ownTransactionMode]), is not refused.
func (m *Migrator) fileModeRefusesBeforeTheRun(migration *Migration) bool {
	if m.effectiveTxMode(MigrationTxModeFile) != MigrationTxModeFile {
		return false
	}
	own, err := m.ownTransactionMode(migration, MigrationTxModeFile, MigrationDirectionUp)
	if err != nil || own.mode != MigrationTxModeFile {
		return false
	}
	return m.refuseBodyForItsMode(migration, MigrationDirectionUp, own) != nil ||
		m.refuseWhatATransactionRefuses(migration, MigrationDirectionUp, own) != nil
}

// refusedInItsTransaction reports whether err is a server's refusal of a
// statement of migration's up body because it ran inside a transaction
// ([servercode.RefusedInATransaction]), in a run that sent the whole body in
// one transaction to a target that rolls schema changes back with it
// (capability.TransactionalDDL). The rollback undoes the file's schema
// changes and rows, so the file can run again without a transaction; what a
// rollback does not undo stays, as [Migrator.replaysWithoutATransaction]
// says. A target whose rollback leaves schema changes in place, such as
// CockroachDB, is never asked to run the file twice. A refusal the static
// checks cannot see is one of these: a COMMIT inside a procedure the file
// calls, or an enum value the file adds and then uses.
func (m *Migrator) refusedInItsTransaction(migration *Migration, txMode MigrationTxMode, err error) bool {
	if txMode != MigrationTxModeFile || !servercode.RefusedInATransaction(err) ||
		!m.conn.Info().Capabilities.Has(capability.TransactionalDDL) {
		return false
	}
	own, ownErr := m.ownTransactionMode(migration, txMode, MigrationDirectionUp)
	return ownErr == nil && own.mode == MigrationTxModeFile && own.blocks.Empty()
}

func (m *Migrator) resolveDownMigrationTxMode(migration *Migration) (MigrationTxMode, error) {
	fileMode := migration.parsedDownTxModeForDialect(m.connectionDialect())
	if fileMode.Err != nil {
		return "", fileMode.Err
	}
	mode, err := resolveMigrationFileTxMode(MigrationTxModeFile, fileMode.Mode)
	if err != nil {
		return "", err
	}
	return m.effectiveTxMode(mode), nil
}

func migrationTxModeSourceName(sourcePath, description string) string {
	if sourcePath != "" {
		return path.Base(sourcePath)
	}
	return description
}
