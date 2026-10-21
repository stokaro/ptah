package dbschema

import (
	"context"
	"fmt"
	"time"

	"ptah.run/core/ptaherr"
)

// MigrationMetadataTable names provider-owned storage without SQL quoting.
// The provider validates its namespace, ownership, and layout before use.
type MigrationMetadataTable struct {
	Schema string
	Name   string
}

// MigrationLogRecord contains stored event facts. The common migrator owns
// event meaning, actor provenance, attempt pairing, and recovery decisions.
type MigrationLogRecord struct {
	RunID       string
	Seq         int64
	Operation   string
	Version     int64
	State       string
	Actor       string
	ActorSource string
	Checksum    string
	At          time.Time
	Error       string
}

// MigrationTagRecord is a movable directory-state label, not a revision row.
type MigrationTagRecord struct {
	Name       string
	Version    int64
	RecordedAt time.Time
}

// MigrationMetadataPage is one bounded part of an immutable read snapshot.
// Exists is fixed for the snapshot. An absent store has one empty complete page.
// A nonterminal page must contain records. Complete means no records remain.
type MigrationMetadataPage[T any] struct {
	Exists   bool
	Complete bool
	Records  []T
}

// MigrationMetadataReader owns a read snapshot on the selected target.
// Next returns at most maxRecords entries and advances exactly once. It never
// repeats an entry or skips a stored record. Concurrent writes do not change
// this snapshot. Close is idempotent, honors cancellation, and retains errors.
// The session closes abandoned readers and invalidates them on restart.
type MigrationMetadataReader[T any] interface {
	Next(context.Context, uint32) (MigrationMetadataPage[T], error)
	Close(context.Context) error
}

// MigrationLogStore persists events outside schema and revision transactions.
// Append stores a unique (RunID, Seq) once; a duplicate is an error, not a replay.
// Lost acknowledgments match ptaherr.ErrOutcomeUnknown and must not be retried.
// Preparation and reads validate ownership and layout; they never adopt a foreign
// table or an incompatible schema. Opening an absent store creates nothing.
type MigrationLogStore interface {
	Prepare(context.Context, MigrationMetadataTable) error
	Append(context.Context, MigrationMetadataTable, MigrationLogRecord) error
	OpenRead(context.Context, MigrationMetadataTable) (MigrationMetadataReader[MigrationLogRecord], error)
}

// MigrationLogStoreProvider exposes durable event storage owned by the session.
// Its writes survive rollback of the session's execution transaction.
type MigrationLogStoreProvider interface {
	MigrationLogStore() (MigrationLogStore, error)
}

// MigrationTagsStore persists exact directory labels on its execution handle.
// Put atomically inserts or moves a label. Delete reports whether that statement
// removed an existing label; an earlier existence check is not sufficient proof.
// Mutations preserve ptaherr.ErrOutcomeUnknown without automatic replay.
// Preparation and reads verify ownership and layout. Reads create no storage.
type MigrationTagsStore interface {
	Prepare(context.Context, MigrationMetadataTable) error
	Put(context.Context, MigrationMetadataTable, MigrationTagRecord) error
	Lookup(context.Context, MigrationMetadataTable, string) (MigrationTagRecord, bool, error)
	Delete(context.Context, MigrationMetadataTable, string) (bool, error)
	OpenRead(context.Context, MigrationMetadataTable) (MigrationMetadataReader[MigrationTagRecord], error)
}

// MigrationTagsStoreProvider binds tags to the implementing session or transaction.
// A transaction may not return its parent's unwrapped store.
type MigrationTagsStoreProvider interface {
	MigrationTagsStore() (MigrationTagsStore, error)
}

// MigrationLogStore selects the native session's durable event store. It uses
// the parent session even on a transaction-bound connection: log events must
// survive execution rollback. SQL connections return nil and retain their SQL
// path. Missing native support refuses instead of selecting that path.
func (dc *DatabaseConnection) MigrationLogStore() (MigrationLogStore, error) {
	if dc == nil {
		return nil, fmt.Errorf("migration logging requires a database connection")
	}
	if dc.native == nil {
		return nil, nil
	}
	owner, ok := dc.native.(MigrationLogStoreProvider)
	if !ok || nilNativeService(owner) {
		return nil, fmt.Errorf("%w: the provider session has no migration log service", ptaherr.ErrUnsupportedFeature)
	}
	store, err := owner.MigrationLogStore()
	if err != nil {
		return nil, err
	}
	if nilNativeService(store) {
		return nil, fmt.Errorf("native provider returned a nil migration log service")
	}
	return store, nil
}

// MigrationTagsStore selects tags on the active native execution handle.
// SQL connections return nil and retain their SQL path. Missing native support
// refuses; transactions cannot fall back to their parent's unwrapped store.
func (dc *DatabaseConnection) MigrationTagsStore() (MigrationTagsStore, error) {
	if dc == nil {
		return nil, fmt.Errorf("migration tags require a database connection")
	}
	if dc.native == nil {
		return nil, nil
	}
	var active any = dc.native
	if dc.executor != nil {
		active = dc.executor
	}
	owner, ok := active.(MigrationTagsStoreProvider)
	if !ok || nilNativeService(owner) {
		return nil, fmt.Errorf("%w: the active provider handle has no migration tag service", ptaherr.ErrUnsupportedFeature)
	}
	store, err := owner.MigrationTagsStore()
	if err != nil {
		return nil, err
	}
	if nilNativeService(store) {
		return nil, fmt.Errorf("native provider returned a nil migration tag service")
	}
	return store, nil
}
