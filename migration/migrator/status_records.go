package migrator

import (
	"slices"

	"ptah.run/migration/migrationfile"
)

// StatusContractVersion is the version of the machine-readable migration
// status document.
//
// It is emitted so a consumer can refuse a document it does not understand
// rather than reading the fields it recognizes and acting on a shape that
// means something else. It rises when a field changes meaning or leaves; a new
// field a reader may ignore is not a new version.
const StatusContractVersion = 1

// Migration states a [MigrationRecord] reports.
const (
	// MigrationStateApplied is a clean revision row whose migration file still
	// accounts for the checksum it recorded.
	MigrationStateApplied = "applied"
	// MigrationStateModified is a clean revision row whose migration file no
	// longer accounts for it. Nothing may apply anything while one exists.
	MigrationStateModified = "modified"
	// MigrationStateDirty is the version a failed or interrupted run left
	// behind. Whether its statements reached the database is what the row's
	// own progress and partial digest answer.
	MigrationStateDirty = "dirty"
	// MigrationStatePending is a migration the database has not applied.
	MigrationStatePending = "pending"
	// MigrationStateOutOfOrder is a pending migration below the current
	// version. Whether it runs is the migrator's execution order to decide.
	MigrationStateOutOfOrder = "out-of-order"
	// MigrationStateCheckpointCovered is a migration below the checkpoint that
	// covers it: it will never run here, and its absence from the history is
	// not a gap. A bootstrap does not change that -- it records the checkpoint
	// that covers them.
	MigrationStateCheckpointCovered = "checkpoint-covered"
)

// MigrationRecord is one migration of the directory, as the machine contract
// reports it.
//
// A caller planning work reads these rather than the version lists beside
// them: a version number says which file, and a plan that has to survive the
// gap between planning and execution needs what the file is, what the database
// recorded for it, and whether the two still agree.
type MigrationRecord struct {
	// Version is the migration's numeric version, and VersionKey its exact
	// revision identity, which is a decimal spelling for a native directory
	// and an opaque token for an Atlas repeatable migration.
	Version    int64  `json:"version"`
	VersionKey string `json:"version_key,omitempty"`
	// Description is the migration's own description, from the file name.
	Description string `json:"description,omitempty"`
	// Checksum is what the current file hashes to, under the same rule that
	// decides what a revision row records: a directory carrying an atlas.sum
	// hash keeps it, and everything else hashes its up SQL. Comparing this
	// across formats is not the applied-checksum rule; State is.
	Checksum string `json:"checksum,omitempty"`
	// AppliedChecksum is what the revision row recorded, present only for a row
	// that exists. A clean row whose file no longer accounts for it reports
	// state "modified"; comparing these two strings is not that rule, because
	// an Atlas history records a running hash over every preceding file.
	AppliedChecksum string `json:"applied_checksum,omitempty"`
	// Checkpoint marks a migration whose up body is the cumulative schema at
	// its version.
	Checkpoint bool `json:"checkpoint,omitempty"`
	// TransactionMode is the file's declared up-direction transaction mode:
	// "file", "none", or empty where the file declares none and the migrator's
	// own mode decides.
	TransactionMode string `json:"transaction_mode,omitempty"`
	// State is one of the MigrationState constants.
	State string `json:"state"`
}

// migrationRecords projects the directory and the revision rows into the
// per-migration contract.
//
// The verdict on an applied checksum comes from the same classifier
// VerifyAppliedChecksums reads, because a second interpreter of an Atlas
// revision hash would disagree with the first one the moment either learned
// something.
func (m *Migrator) migrationRecords(
	migrations []*Migration,
	revisions []MigrationRevision,
	pending []*Migration,
	outOfOrder []int64,
	floor int64,
	dirty *MigrationRevision,
	mismatched map[string]struct{},
) []MigrationRecord {
	dialect := m.connectionDialect()
	revisionsByKey := appliedRevisionsByKey(revisions)
	pendingKeys := make(map[string]struct{}, len(pending))
	for _, migration := range pending {
		pendingKeys[migration.RevisionVersion()] = struct{}{}
	}
	records := make([]MigrationRecord, 0, len(migrations))
	for _, migration := range migrations {
		key := migration.RevisionVersion()
		record := MigrationRecord{
			Version:         migration.Version,
			VersionKey:      key,
			Description:     migration.Description,
			Checksum:        migrationRevisionHash(migration),
			Checkpoint:      migration.IsCheckpoint,
			TransactionMode: transactionModeName(migration.parsedUpTxModeForDialect(dialect)),
		}
		if revision, applied := revisionsByKey[key]; applied {
			record.AppliedChecksum = revision.Checksum
		}
		record.State = migrationState(migration, key, migrationStateSets{
			revisionsByKey: revisionsByKey,
			mismatched:     mismatched,
			pending:        pendingKeys,
			outOfOrder:     outOfOrder,
			floor:          floor,
			dirty:          dirty,
		})
		records = append(records, record)
	}
	return records
}

// migrationStateSets is what one migration's state is decided against: the
// rows the database holds, the verdicts of the applied-checksum rule, and the
// selection this status already computed.
type migrationStateSets struct {
	revisionsByKey map[string]MigrationRevision
	mismatched     map[string]struct{}
	pending        map[string]struct{}
	outOfOrder     []int64
	floor          int64
	dirty          *MigrationRevision
}

func migrationState(migration *Migration, key string, sets migrationStateSets) string {
	if sets.dirty != nil && sets.dirty.RevisionVersion() == key {
		return MigrationStateDirty
	}
	if _, applied := sets.revisionsByKey[key]; applied {
		if _, modified := sets.mismatched[key]; modified {
			return MigrationStateModified
		}
		return MigrationStateApplied
	}
	if _, isPending := sets.pending[key]; isPending {
		if slices.Contains(sets.outOfOrder, migration.Version) {
			return MigrationStateOutOfOrder
		}
		return MigrationStatePending
	}
	if sets.floor > 0 && migration.Version < sets.floor {
		return MigrationStateCheckpointCovered
	}
	return MigrationStatePending
}

// transactionModeName spells a file's declared transaction mode for the
// contract, resolved for the connection's dialect because a directive only one
// dialect's string rules expose is not a directive on another.
//
// A file that declares nothing, and a file whose directive did not parse, are
// both left empty rather than named: the first decides nothing and the
// migrator's own mode does, and the second is a refusal the executor owes its
// caller rather than a mode this document may guess at.
func transactionModeName(parsed migrationfile.ParsedFileTxMode) string {
	if parsed.Err != nil {
		return ""
	}
	switch parsed.Mode {
	case migrationfile.FileTxModeFile:
		return "file"
	case migrationfile.FileTxModeNone:
		return "none"
	default:
		return ""
	}
}
