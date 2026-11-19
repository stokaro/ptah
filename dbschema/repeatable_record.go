package dbschema

import (
	"context"
	"fmt"
	"time"

	"ptah.run/core/ptaherr"
)

// The states a row of the record of repeatable migrations holds.
const (
	// RepeatableRunning is a row whose last attempt is claimed and has no
	// outcome yet.
	RepeatableRunning = "running"
	// RepeatableApplied is a row whose last attempt applied.
	RepeatableApplied = "applied"
	// RepeatableFailed is a row whose last attempt failed.
	RepeatableFailed = "failed"
)

// RepeatableRow is what the record of repeatable migrations keeps of one
// repeatable migration: the content that last applied and the last attempt.
// The migrator owns what a row means: which file runs, retry and repair.
type RepeatableRow struct {
	// Identity is the part of the file name between R__ and .sql.
	Identity string
	// State is RepeatableRunning, RepeatableApplied or RepeatableFailed.
	State string
	// Checksum is the digest of the content that last applied, empty until
	// one has.
	Checksum string
	// AttemptChecksum, AttemptOrder, Transactional and RunID describe the
	// last attempt: its digest, its 1-based position in its run, whether its
	// body ran in a transaction, and the run that made it.
	AttemptChecksum string
	AttemptOrder    int
	Transactional   bool
	RunID           string
	// AttemptedAt is when the last attempt was claimed. AppliedAt is when a
	// content last applied, the zero time until one has.
	AttemptedAt time.Time
	AppliedAt   time.Time
	// Error is the last failed attempt's error, empty after one that applied.
	Error string
	// BodyKept reports that the record keeps the bytes of the content that
	// last applied. An empty body is kept; [RepeatableStore.ReadBody] reads it.
	BodyKept bool
}

// RepeatableSnapshot is a complete read of the record, without bodies. A
// missing record differs from an empty one. A read that cannot be complete
// returns an error, never a usable prefix. Rows belong to the caller.
type RepeatableSnapshot struct {
	Exists bool
	Rows   []RepeatableRow
}

// RepeatableBody is the content the record keeps for one identity. Found is
// false when the record holds no row for it; Kept is false when the row keeps
// no body, and Body is then empty.
type RepeatableBody struct {
	Found bool
	Kept  bool
	Body  []byte
}

// RepeatableClaim records that a run is about to attempt an identity, before
// any statement of its body runs. It sets the row's state to running, its
// attempt fields and RunID, and clears its error; the applied digest, the
// applied time and the body stay as they were.
//
// With Insert the claim succeeds only when the record holds no row for the
// identity. Without it the claim succeeds only while the row still names
// ExpectedRunID, the run the caller read. Any other row is a
// [*RepeatableConflictError].
type RepeatableClaim struct {
	Identity        string
	Insert          bool
	ExpectedRunID   string
	RunID           string
	AttemptChecksum string
	AttemptOrder    int
	Transactional   bool
	AttemptedAt     time.Time
}

// RepeatableCompletion records an attempt's content as applied: the state,
// the digest, the bytes and the time, and no error. It applies only to the row
// RunID claimed, and Checksum must be the digest that claim wrote.
type RepeatableCompletion struct {
	Identity  string
	RunID     string
	Checksum  string
	Body      []byte
	AppliedAt time.Time
}

// RepeatableFailure records that an attempt's body failed: the state and the
// error. It applies only to the row RunID claimed.
type RepeatableFailure struct {
	Identity string
	RunID    string
	Error    string
}

// RepeatableConflictError reports a write to the record that met a row other
// than the one it expected: another run inserted, claimed or removed it after
// the caller read the record. The write changed nothing.
type RepeatableConflictError struct {
	// Holder is the run the row names, empty when the record holds no row for
	// the identity.
	Holder string
}

// Error names the run that holds the row, when one does.
func (e *RepeatableConflictError) Error() string {
	if e.Holder == "" {
		return "the record of repeatable migrations holds no row for the identity"
	}
	return fmt.Sprintf("run %q holds the row of the record of repeatable migrations", e.Holder)
}

// RepeatableStore keeps the record of repeatable migrations for a session
// whose engine has no SQL connection the migrator can reach, such as one
// served by an executable provider. It stores rows; the migrator decides what
// they mean.
//
// Read, Prepare, ReadBody, Claim, Fail and Remove run on the session outside
// any transaction. Complete runs on the handle the store is bound to: a store
// a transaction returns writes the completion in that transaction, so the
// record and the migration body commit together. Prepare creates a missing
// record and checks the ownership and layout of an existing one; it never
// upgrades another layout. Reads create nothing.
//
// Each write changes one row atomically. A write that met another row returns
// a [*RepeatableConflictError] and changed nothing. A write whose effect is
// unknown matches ptaherr.ErrOutcomeUnknown and must not be replayed: the
// caller settles it by reading the row back. Remove reports whether it deleted
// a row.
type RepeatableStore interface {
	Read(context.Context, MigrationMetadataTable) (RepeatableSnapshot, error)
	Prepare(context.Context, MigrationMetadataTable) error
	ReadBody(context.Context, MigrationMetadataTable, string) (RepeatableBody, error)
	Claim(context.Context, MigrationMetadataTable, RepeatableClaim) error
	Complete(context.Context, MigrationMetadataTable, RepeatableCompletion) error
	Fail(context.Context, MigrationMetadataTable, RepeatableFailure) error
	Remove(context.Context, MigrationMetadataTable, string) (bool, error)
	// MaxBodyBytes is the longest content Complete stores. The migrator
	// refuses a longer file before anything runs.
	MaxBodyBytes() int
}

// RepeatableStoreProvider binds the record to the implementing session or
// transaction. A transaction must not return its parent session's unbound
// store. Lack of support returns an error that says what is missing.
type RepeatableStoreProvider interface {
	RepeatableStore() (RepeatableStore, error)
}

// RepeatableStore returns the record of repeatable migrations on the active
// execution handle. SQL-backed connections return nil without error and keep
// their SQL path. A native connection without the service refuses it, and the
// error says why; it never falls back to SQL. A transaction-bound copy returns
// a store bound to that transaction.
func (dc *DatabaseConnection) RepeatableStore() (RepeatableStore, error) {
	if dc == nil {
		return nil, fmt.Errorf("the record of repeatable migrations requires a database connection")
	}
	if dc.native == nil {
		return nil, nil
	}
	var owner any = dc.native
	if dc.executor != nil {
		owner = dc.executor
	}
	service, ok := owner.(RepeatableStoreProvider)
	if !ok || nilNativeService(service) {
		return nil, fmt.Errorf("%w: the active provider handle keeps no record of repeatable migrations", ptaherr.ErrUnsupportedFeature)
	}
	store, err := service.RepeatableStore()
	if err != nil {
		return nil, err
	}
	if nilNativeService(store) {
		return nil, fmt.Errorf("native provider returned a nil record of repeatable migrations")
	}
	return store, nil
}
