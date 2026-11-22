package shadowdb

import "ptah.run/migration/migrator"

// HistoryTxMode is the transaction mode a replay of an applied history runs
// in, for a target whose runs apply their pending migrations in run.
//
// The revision table records that a migration ran, not the mode it ran in.
// The mode the run applies its pending migrations in is the only statement of
// that mode the replay has, so the replay takes it. The replay then resolves
// each migration's mode by the rule a run uses, and a migration's own
// directive wins where it writes one. A file marked `-- atlas:txmode none`
// replays outside a transaction under `file`, and an unmarked file that a
// `none` run applied replays outside a transaction too, where `file` would
// refuse a statement the run accepted.
//
// `all` replays as `file`. A run under `all` applies its pending migrations in
// one transaction, but a history spans every run that applied it, and the
// revision table does not record which migrations ran together, so no replay
// can rebuild those transactions. `file` gives each migration a transaction,
// as `all` did. A migration that `all` refuses, because of its directive, its
// checks or a transaction it opens itself, was applied by a run in another
// mode, and `file` replays it as that run applied it.
//
// Every replay of an applied history takes its mode from here, so the drift
// check of `ptah migrations up` and the report of `ptah migrations drift`
// replay one history alike.
func HistoryTxMode(run migrator.MigrationTxMode) migrator.MigrationTxMode {
	if run == migrator.MigrationTxModeAll {
		return migrator.MigrationTxModeFile
	}
	return run
}
