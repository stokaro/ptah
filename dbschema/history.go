package dbschema

import (
	"context"
	"fmt"
	"time"

	"ptah.run/core/ptaherr"
)

// HistoryFormat identifies the representation of a revision store. Providers
// refuse unsupported representations rather than reinterpret stored records.
type HistoryFormat string

const (
	// NativeHistory selects Ptah revision records with numeric identities.
	NativeHistory HistoryFormat = "native"
	// AtlasHistory selects exact Atlas revision identities and metadata.
	AtlasHistory HistoryFormat = "atlas"
)

// HistoryTable names a revision store using separate identifier components.
// Names are not SQL fragments. An empty schema uses the connected default.
type HistoryTable struct {
	Schema string
	Name   string
	Format HistoryFormat
}

// HistoryRecord contains stored revision facts without migration policy. Version
// preserves exact identity, including an empty Atlas identity. Native versions
// use canonical signed decimal spelling. State and Direction are explicit;
// providers must not derive recovery decisions from them.
type HistoryRecord struct {
	Version         string
	Description     string
	State           string
	Direction       string
	Applied         int
	Total           int
	Error           string
	ErrorStatement  string
	ExecutionTime   time.Duration
	Checksum        string
	AppliedAt       time.Time
	OperatorVersion string
	PartialHashes   []string
	AtlasType       uint32
}

// HistorySnapshot is a complete consistent read. A missing store differs from
// an empty store. An incomplete or oversized read returns an error, never a
// usable prefix. Records and their slices belong exclusively to the caller.
type HistorySnapshot struct {
	Exists  bool
	Records []HistoryRecord
}

// HistoryStore accesses revision storage on one session or transaction.
// Preparation runs outside transactions and validates ownership and layout;
// incompatible layouts are refused, never upgraded. CompareAndSwap atomically
// compares the entire expected record before a replacement or deletion. A nil
// expected record inserts only if absent; a nil replacement deletes the exact
// expected record. A conflict changes nothing and returns an error.
//
// Operations never retry an uncertain write. A successful mutation within a
// transaction confirms execution only; committing that transaction establishes
// durability. The migrator owns ordering, checksums, retry, and repair policy.
type HistoryStore interface {
	Read(context.Context, HistoryTable) (HistorySnapshot, error)
	Prepare(context.Context, HistoryTable) error
	CompareAndSwap(context.Context, HistoryTable, *HistoryRecord, *HistoryRecord) error
}

// HistoryStoreProvider binds history access to the implementing session or
// transaction. A transaction must not return its parent session's unwrapped
// store. Lack of support returns an error rather than another connection.
type HistoryStoreProvider interface {
	HistoryStore() (HistoryStore, error)
}

// HistoryStore returns native revision storage on the active execution handle.
// SQL-backed connections return nil without error and keep their SQL storage
// path. Native connections without this service refuse it; they never fall back
// to SQL or open an unrelated session. Transaction copies require a store bound
// to their active executor, even when their parent session exposes history.
func (dc *DatabaseConnection) HistoryStore() (HistoryStore, error) {
	if dc.native == nil {
		return nil, nil
	}
	var owner any = dc.native
	if dc.executor != nil {
		owner = dc.executor
	}
	if nilNativeService(owner) {
		return nil, fmt.Errorf("%w: the active provider handle is nil", ptaherr.ErrUnsupportedFeature)
	}
	service, ok := owner.(HistoryStoreProvider)
	if !ok {
		return nil, fmt.Errorf("%w: the active provider handle has no history service", ptaherr.ErrUnsupportedFeature)
	}
	store, err := service.HistoryStore()
	if err != nil {
		return nil, err
	}
	if nilNativeService(store) {
		return nil, fmt.Errorf("native provider returned a nil history service")
	}
	return store, nil
}
