package migrator

import (
	"cmp"
	"context"
	"fmt"
	"slices"
)

// AppliedHistory is what a revision table says a database holds: the
// migrations whose effects the history recorded, in the order a replay applies
// them, and the rows it was read from.
//
// Replaying Migrations on an empty database with [Migrator.MigrateUp], and
// Repeatables after them, rebuilds the schema the history produced;
// [AppliedHistory.ReplayProvider] holds both. An Atlas baseline stands for every file at
// or below it, so those files are in Migrations although they have no row. A
// checkpoint the database was bootstrapped from is the only checkpoint a
// history records, and the files below it have no row either, so a replay of
// Migrations bootstraps from that checkpoint in turn.
type AppliedHistory struct {
	Migrations []*Migration
	Revisions  []MigrationRevision
	// Repeatables are the repeatable migrations whose content is in effect in
	// the database, each with that content, in the order an up run executes
	// them: the file where it hashes to the digest recorded as applied, and
	// the body the record keeps otherwise. A directory read without
	// [WithRepeatableMigrations] contributes none.
	Repeatables []AppliedRepeatable
}

// AppliedRepeatable is a repeatable migration and the content of it in effect
// in the database.
type AppliedRepeatable struct {
	// Identity is the name the record keys the migration by.
	Identity string
	// Path is the file's path in the migration directory, empty when the
	// directory has no file for the identity and the content is the body the
	// record keeps.
	Path string
	// Checksum is the digest recorded as applied, which the content hashes
	// to.
	Checksum string
	// Migration carries the body the way a versioned up migration carries it.
	// A history [Migrator.AppliedHistory] returns never holds a nil one.
	Migration *Migration
}

// Versions lists the versions of the history's migrations in replay order.
func (h AppliedHistory) Versions() []int64 {
	return migrationVersions(h.Migrations)
}

// VersionKeys lists the identities the revision table records the history's
// migrations under, in the order of [AppliedHistory.Versions].
func (h AppliedHistory) VersionKeys() []string {
	return migrationVersionKeys(h.Migrations)
}

// Reasons an [UnreplayableHistoryError] gives.
const (
	// UnreplayableDirty is a revision that is not cleanly applied: a failed or
	// partial run, or a rollback that stopped halfway. What it left in the
	// database is not what any file says. For a repeatable migration it is an
	// unfinished attempt outside a transaction, or a rolled-back one whose
	// record keeps no body for the content that applied before it.
	UnreplayableDirty = "dirty"
	// UnreplayableMissingFile is an applied revision whose migration the
	// directory no longer holds. For a repeatable migration the record keeps no
	// body that hashes to the digest it applied.
	UnreplayableMissingFile = "missing_file"
	// UnreplayableRepeatable is an applied repeatable migration. Its row
	// records the last run of a file that ran more than once, which a single
	// replay does not reproduce.
	UnreplayableRepeatable = "repeatable"
	// UnreplayableOutOfOrder is a history that applied a lower version after a
	// higher one. A replay applies versions in order, so it would not run the
	// statements in the order the database ran them.
	UnreplayableOutOfOrder = "out_of_order"
	// UnreplayableChanged is a migration whose file changed after it applied,
	// so nothing in the directory holds the content the database ran. For a
	// versioned migration the file no longer accounts for the checksum its
	// revision recorded, under the rule an apply verifies before it runs; for a
	// repeatable migration the record keeps no body that hashes to the digest
	// it applied.
	UnreplayableChanged = "changed"
)

// UnreplayableHistoryError says why a history cannot be replayed into the
// schema it produced. A caller that checks a database against its history
// reports it as a history it could not check, never as a clean one.
type UnreplayableHistoryError struct {
	// Reason is one of the Unreplayable constants.
	Reason string
	// Version is the revision the reason is about. It is zero when the reason
	// is about a repeatable migration.
	Version    int64
	VersionKey string
	// Repeatable is the identity of the repeatable migration the reason is
	// about, and empty when the reason is about a revision.
	Repeatable string
	// Detail says what was found, for a person.
	Detail string
	// Err is the finding beneath the reason when another error type carries
	// it: a [*ChecksumMismatchError] for a versioned migration whose file
	// changed. It is nil otherwise.
	Err error
}

func (e *UnreplayableHistoryError) Error() string {
	if e.Repeatable != "" {
		return fmt.Sprintf("the applied history cannot be replayed: repeatable migration %s: %s", e.Repeatable, e.Detail)
	}
	return fmt.Sprintf("the applied history cannot be replayed: version %s: %s", e.VersionKey, e.Detail)
}

// Unwrap returns the finding beneath the reason, or nil.
func (e *UnreplayableHistoryError) Unwrap() error { return e.Err }

// AppliedHistory reads the revision table and returns the migrations its
// applied revisions name, or an [UnreplayableHistoryError] when the history
// does not say what the database holds. It writes nothing.
//
// A versioned migration whose file no longer accounts for the checksum its
// revision recorded is an [UnreplayableHistoryError] with reason
// [UnreplayableChanged] wrapping the [*ChecksumMismatchError] an apply refuses
// with: a replay would run the file as it is now, not what the database ran.
//
// When the provider loads repeatable migrations, it reads their record too.
// Each one whose file hashes to the applied digest is in Repeatables. A
// record whose last attempt did not finish, whose file is gone, or whose file
// changed after it applied is an [UnreplayableHistoryError] naming the
// repeatable migration, because no file in the directory is the content the
// database ran.
//
// Call it while holding the migration lock, as a [MigrationPlanGuard] does: a
// cooperating runner cannot then change the history between this read and the
// run that follows.
func (m *Migrator) AppliedHistory(ctx context.Context) (AppliedHistory, error) {
	revisions, err := m.GetRevisions(ctx)
	if err != nil {
		return AppliedHistory{}, fmt.Errorf("read the applied history: %w", err)
	}
	history, err := m.appliedHistory(revisions)
	if err != nil {
		return AppliedHistory{}, err
	}
	if err := m.refuseChangedAppliedFiles(revisions); err != nil {
		return AppliedHistory{}, err
	}
	history.Repeatables, err = m.appliedRepeatables(ctx)
	if err != nil {
		return AppliedHistory{}, err
	}
	return history, nil
}

func (m *Migrator) appliedHistory(revisions []MigrationRevision) (AppliedHistory, error) {
	for _, revision := range revisions {
		if revision.Dirty || revision.State != migrationStateApplied || revision.Direction == MigrationDirectionDown {
			return AppliedHistory{}, &UnreplayableHistoryError{
				Reason: UnreplayableDirty, Version: revision.Version, VersionKey: revision.RevisionVersion(),
				Detail: fmt.Sprintf("the revision is %s, not cleanly applied", revisionStateWords(revision)),
			}
		}
	}
	if err := refuseOutOfOrderHistory(revisions); err != nil {
		return AppliedHistory{}, err
	}

	byVersion := make(map[int64]*Migration)
	for _, migration := range m.migrationProvider.Migrations() {
		byVersion[migration.Version] = migration
	}
	applied := m.effectiveAppliedVersionsFromRevisions(appliedRevisionVersions(revisions), revisions)
	keys := make(map[int64]string, len(revisions))
	for _, revision := range revisions {
		keys[revision.Version] = revision.RevisionVersion()
	}
	history := AppliedHistory{Revisions: revisions, Migrations: make([]*Migration, 0, len(applied))}
	for _, version := range applied {
		key := keys[version]
		if key == "" {
			key = fmt.Sprintf("%d", m.versionText(version))
		}
		migration, ok := byVersion[version]
		if !ok {
			return AppliedHistory{}, &UnreplayableHistoryError{
				Reason: UnreplayableMissingFile, Version: version, VersionKey: key,
				Detail: "the database records it as applied and the migration directory has no file for it",
			}
		}
		if migration.isAtlasRepeatable() {
			return AppliedHistory{}, &UnreplayableHistoryError{
				Reason: UnreplayableRepeatable, Version: version, VersionKey: key,
				Detail: "it is a repeatable migration, and its row records only its last run",
			}
		}
		history.Migrations = append(history.Migrations, migration)
	}
	slices.SortFunc(history.Migrations, func(a, b *Migration) int { return cmp.Compare(a.Version, b.Version) })
	return history, nil
}

// refuseChangedAppliedFiles refuses a history in which an applied file no
// longer accounts for the checksum its revision recorded. It asks the rule an
// apply asks before it runs, so the history a drift check replays is one an
// apply would accept, and a mismatch an apply tolerates as a coherent
// projection of the history is tolerated here too.
func (m *Migrator) refuseChangedAppliedFiles(revisions []MigrationRevision) error {
	classified, err := m.classifyAppliedChecksums(m.migrationProvider.Migrations(), revisions)
	if err != nil {
		return fmt.Errorf("verify the applied checksums: %w", err)
	}
	if len(classified.mismatches) == 0 || classified.coherentProjection {
		return nil
	}
	mismatch := classified.mismatches[0]
	return &UnreplayableHistoryError{
		Reason: UnreplayableChanged, Version: mismatch.Version, VersionKey: mismatch.RevisionKey,
		Detail: fmt.Sprintf("its file changed after it applied (checksum stored %s, current %s), "+
			"so the directory no longer holds what the database ran", mismatch.Stored, mismatch.Computed),
		Err: mismatch,
	}
}

// refuseOutOfOrderHistory refuses a history in which a version was applied
// after a higher one. A row with no time, as a baseline can leave, says
// nothing about order and is left out.
func refuseOutOfOrderHistory(revisions []MigrationRevision) error {
	timed := make([]MigrationRevision, 0, len(revisions))
	for _, revision := range revisions {
		if !revision.AppliedAt.IsZero() {
			timed = append(timed, revision)
		}
	}
	slices.SortFunc(timed, func(a, b MigrationRevision) int { return cmp.Compare(a.Version, b.Version) })
	for position := 1; position < len(timed); position++ {
		lower, higher := timed[position-1], timed[position]
		if lower.AppliedAt.After(higher.AppliedAt) {
			return &UnreplayableHistoryError{
				Reason: UnreplayableOutOfOrder, Version: lower.Version, VersionKey: lower.RevisionVersion(),
				Detail: fmt.Sprintf("it was applied at %s, after version %s at %s",
					lower.AppliedAt.UTC().Format("2006-01-02T15:04:05.000Z"), higher.RevisionVersion(),
					higher.AppliedAt.UTC().Format("2006-01-02T15:04:05.000Z")),
			}
		}
	}
	return nil
}

func revisionStateWords(revision MigrationRevision) string {
	switch {
	case revision.Direction == MigrationDirectionDown:
		return "a rollback that did not finish"
	case revision.Dirty:
		return "dirty"
	default:
		return revision.State
	}
}

// DriftOutcome is what a drift check concluded.
type DriftOutcome string

const (
	// DriftClean is a target whose schema is the one its applied history
	// produces, over the scope the check compared.
	DriftClean DriftOutcome = "clean"
	// DriftDrifted is a target whose schema differs from the one its applied
	// history produces.
	DriftDrifted DriftOutcome = "drifted"
	// DriftUnable is a check that could not decide: the history could not be
	// replayed, the shadow database could not be used, or the comparison could
	// not read part of the target. It is never a clean result.
	DriftUnable DriftOutcome = "unable"
)

// DriftCheck is the part of a run document that reports a drift check.
type DriftCheck struct {
	// Outcome is clean, drifted or unable.
	Outcome DriftOutcome `json:"outcome"`
	// Replayed and ReplayedKeys are the versions the check replayed on the
	// shadow database, in order. They are absent when it replayed nothing.
	Replayed     []int64  `json:"replayed,omitempty"`
	ReplayedKeys []string `json:"replayed_keys,omitempty"`
	// ReplayedRepeatables are the identities of the repeatable migrations the
	// check replayed after the versions, in order. They are absent when it
	// replayed none.
	ReplayedRepeatables []string `json:"replayed_repeatables,omitempty"`
	// Stage and Reason say where and why a check came back unable; both are
	// absent otherwise. Reason is a stable code, such as an
	// [UnreplayableHistoryError] reason or a shadow-verification kind.
	Stage  string `json:"stage,omitempty"`
	Reason string `json:"reason,omitempty"`
	// Message says the same for a person.
	Message string `json:"message,omitempty"`
	// Findings are the differences a drifted check found, or the objects an
	// unable check could not compare. Each names an object, never a row.
	Findings []DriftFinding `json:"findings,omitempty"`
}

// DriftFinding is one difference between the target and its replayed history.
type DriftFinding struct {
	Kind       string `json:"kind"`
	Object     string `json:"object,omitempty"`
	Table      string `json:"table,omitempty"`
	Column     string `json:"column,omitempty"`
	Constraint string `json:"constraint,omitempty"`
	Message    string `json:"message"`
}
