// Package embedydb is the YDB half of an inference migration: the row tables
// the run state lives in, the keyset scan that reads a source table, the
// serializable transaction that writes vectors and their checkpoint together,
// and the vector_kmeans_tree index queries search through.
//
// It implements [embedbackend.Backend] with YDB's own vector support rather
// than through a compatibility layer: a vector is stored as the bytes
// Knn::ToBinaryStringFloat produces, and distances are the Knn functions. The
// run-state rules are [embedstore]'s: each lifecycle operation reads the rows
// it decides on inside one serializable transaction, decides on an
// [embedstore.Memory] built from them, and writes back what changed. YDB
// aborts a transaction whose reads another transaction changed, and the
// operation then runs again on what is there now, which is the serialization
// the PostgreSQL store gets from its locks (stokaro/ptah#4181).
//
// What YDB cannot do is refused by the capability that says so and the server
// line that lacks it: an index needs vector_indexes, a write into an indexed
// generation needs vector_index_maintained_on_write, and the outbox
// consistency mode needs row triggers, which YDB does not have.
package embedydb

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedgen"
)

// Dialect is what this package speaks to.
const Dialect = platform.YDB

// A signature that drifts from the contract is a compile error here rather
// than at the verb that reaches it.
var _ embedbackend.Backend = (*Backend)(nil)

// Backend is [embedbackend.Backend] over YDB.
type Backend struct {
	db *dbschema.DatabaseConnection
	// caps are the connected server's capabilities, refined by its feature
	// flags when the URL names its monitoring endpoint.
	caps capability.Capabilities
	// version is the server's version, which a refusal names.
	version string
	store   *Store
}

// Open connects to the YDB database a ydb:// or ydbs:// URL names.
//
// It connects the way every other YDB command does, so the capabilities a
// refusal is decided by are the ones `ptah db read` would report for the same
// URL, feature flags included.
func Open(ctx context.Context, dbURL string) (*Backend, error) {
	connection, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", dburldisplay.Format(dbURL), err)
	}
	info := connection.Info()
	if platform.NormalizeDialect(info.Dialect) != Dialect {
		_ = connection.Close()
		return nil, fmt.Errorf("%s is a %s database, not YDB", dburldisplay.Format(dbURL), info.Dialect)
	}
	return newBackend(connection, info.Capabilities, info.Version), nil
}

// newBackend assembles a backend over an open connection.
func newBackend(db *dbschema.DatabaseConnection, caps capability.Capabilities, version string) *Backend {
	backend := &Backend{db: db, caps: caps, version: version}
	backend.store = &Store{backend: backend}
	return backend
}

// Dialect names the engine.
func (b *Backend) Dialect() string { return Dialect }

// Close releases the connection.
func (b *Backend) Close() error { return b.db.Close() }

// Store is the run state.
func (b *Backend) Store() embedbackend.Store { return b.store }

// line names the connected server for a refusal: YDB and its version.
func (b *Backend) line() string {
	if strings.TrimSpace(b.version) == "" {
		return "this YDB server"
	}
	return "YDB " + b.version
}

// require refuses an operation the server's capabilities do not cover, naming
// the capability, the server line and what the operation needed it for.
func (b *Backend) require(key capability.Capability, purpose string) error {
	if b.caps.Has(key) {
		return nil
	}
	return fmt.Errorf("%w: %s has no %s, and %s", ErrUnsupported, b.line(), key, purpose)
}

// ResolveRelation answers a schema and table as written. A YDB path has no
// search path to resolve through: a directory relative to the database root
// and a name are the relation.
func (b *Backend) ResolveRelation(_ context.Context, schema, table string) (embedbackend.Relation, error) {
	return embedbackend.Relation{Schema: directory(schema), Table: strings.TrimSpace(table)}, nil
}

// WithResolvedRelations answers spec with its relations named as YDB names
// them.
func (b *Backend) WithResolvedRelations(ctx context.Context, spec embedgen.Spec) (embedgen.Spec, error) {
	source, err := b.ResolveRelation(ctx, spec.Source.Schema, spec.Source.Table)
	if err != nil {
		return spec, err
	}
	target, err := b.ResolveRelation(ctx, spec.Target.Schema, spec.Target.Table)
	if err != nil {
		return spec, err
	}
	resolved := spec
	resolved.Source.Schema, resolved.Source.Table = source.Schema, source.Table
	resolved.Target.Schema, resolved.Target.Table = target.Schema, target.Table
	return resolved, nil
}

// TargetObjects is the YDB column and index a generation needs.
func (b *Backend) TargetObjects(spec embedgen.Spec) (embedgen.TargetObjects, error) {
	return TargetObjects(spec)
}
