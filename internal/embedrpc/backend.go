// Package embedrpc is [embedbackend.Backend] over the embedding store an
// executable provider serves (ADR 0032).
//
// The host keeps the lifecycle and the provider keeps the storage. Every
// lifecycle decision is [embedstore]'s: an operation reads the records it
// decides on with the provider's token, decides on an [embedstore.Memory]
// built from them, and sends back only the records the decision changed with
// that token. A provider that finds another token answers CONFLICT and writes
// nothing, and the operation reads and decides again, up to a bound. A batch
// commit is the same exchange over the run and the batch's keys. Nothing here
// holds a transaction across calls, and nothing here composes SQL: the
// provider owns the layout, the statements, the vector encoding and the
// searches, and refuses what its engine cannot hold.
//
// The names of a generation's objects come from [embedgen], so the name that
// is created and the name that is dropped come from one derivation on every
// engine.
package embedrpc

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedgen"
)

// ErrUnsupported is what a provider's engine cannot do for a generation.
var ErrUnsupported = errors.New("not supported by the provider's engine")

// Session is one provider session's embedding store.
type Session struct {
	// Store is the provider's client, which validates every call against
	// the contract both sides negotiated.
	Store provider.EmbeddingStore
	// Engine is the engine's ID.
	Engine string
	// ConsistencyModes are the modes the engine declares a generation can be
	// built with.
	ConsistencyModes []string
	// ID is the session every call acts on.
	ID string
}

// A signature that drifts from the contract is a compile error here rather
// than at the verb that reaches it.
var _ embedbackend.Backend = (*Backend)(nil)

// Backend is [embedbackend.Backend] over a provider session.
type Backend struct {
	session Session
	release func() error
	store   *Store

	closeOnce sync.Once
	closeErr  error
}

// New returns the backend over a session. release closes the connection the
// session belongs to; Close calls it once.
func New(session Session, release func() error) *Backend {
	backend := &Backend{session: session, release: release}
	backend.store = &Store{backend: backend}
	return backend
}

// Dialect names the engine by its ID.
func (b *Backend) Dialect() string { return b.session.Engine }

// Close releases the connection. Closing twice is a no-op.
func (b *Backend) Close() error {
	b.closeOnce.Do(func() {
		if b.release != nil {
			b.closeErr = b.release()
		}
	})
	return b.closeErr
}

// Store is the run state.
func (b *Backend) Store() embedbackend.Store { return b.store }

// requestID names one write, which its result echoes.
func requestID() string { return rand.Text() }

// engineName names the engine for a diagnostic.
func (b *Backend) engineName() string { return "engine " + strconv.Quote(b.session.Engine) }

// EnsureSchema prepares the run-state store. The provider owns its layout, so
// the renderer is not used: a provider renders its own store.
func (b *Backend) EnsureSchema(ctx context.Context, _ renderer.Service) error {
	result, err := b.session.Store.PrepareEmbeddingStore(ctx, &protocol.PrepareEmbeddingStoreRequest{SessionId: b.session.ID, RequestId: requestID()})
	return writeError("prepare the embedding store on "+b.engineName(), result, err)
}

// writeError describes a write that did not apply, with the line the
// provider wrote about it, or returns nil for one that did.
//
// A refusal and a write that was not applied confirm that nothing was
// written. An unknown outcome does not, and the operator is told to read the
// state again before anything else decides on it.
func writeError(what string, result *protocol.EmbeddingWriteResult, err error) error {
	if err == nil {
		return nil
	}
	written, confirmed := errors.AsType[*provider.EmbeddingWriteError](err)
	switch {
	case !confirmed:
		return fmt.Errorf("%s: %w", what, err)
	case written.Outcome == protocol.EmbeddingWriteResult_REFUSED:
		return fmt.Errorf("%s: %s: %w", what, result.GetRefusal().GetMessage(), err)
	case written.Outcome == protocol.EmbeddingWriteResult_NOT_APPLIED:
		return fmt.Errorf("%s: %s: %w", what, result.GetMessage(), err)
	case written.Outcome == protocol.EmbeddingWriteResult_UNKNOWN:
		return fmt.Errorf("%s: the provider cannot say whether it committed, so read the state again before anything decides on it: %w", what, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// ResolveRelation answers the relation a schema and table spell on this
// server. A spelling that names no relation comes back as written.
func (b *Backend) ResolveRelation(ctx context.Context, schema, name string) (embedbackend.Relation, error) {
	resolved, err := b.resolve(ctx, table(schema, name))
	if err != nil {
		return embedbackend.Relation{}, err
	}
	return resolved[0], nil
}

// resolve answers each relation in one call.
func (b *Backend) resolve(ctx context.Context, relations ...*protocol.Reference) ([]embedbackend.Relation, error) {
	answer, err := b.session.Store.ResolveEmbeddingRelations(ctx, &protocol.ResolveEmbeddingRelationsRequest{SessionId: b.session.ID, Relations: relations})
	if err != nil {
		return nil, fmt.Errorf("resolve relations on %s: %w", b.engineName(), err)
	}
	resolved := make([]embedbackend.Relation, len(answer.GetRelations()))
	for i, relation := range answer.GetRelations() {
		resolved[i] = embedbackend.Relation{Schema: relation.GetSchema(), Table: relation.GetName()}
	}
	return resolved, nil
}

// WithResolvedRelations answers spec with its source and target named as the
// server names them.
func (b *Backend) WithResolvedRelations(ctx context.Context, spec embedgen.Spec) (embedgen.Spec, error) {
	resolved, err := b.resolve(ctx, table(spec.Source.Schema, spec.Source.Table), table(spec.Target.Schema, spec.Target.Table))
	if err != nil {
		return spec, err
	}
	physical := spec
	physical.Source.Schema, physical.Source.Table = resolved[0].Schema, resolved[0].Table
	physical.Target.Schema, physical.Target.Table = resolved[1].Schema, resolved[1].Table
	return physical, nil
}

// TargetObjects asks the provider what a generation needs on its engine. It
// is offline, and the contract has no context to give it, so it runs under
// the SDK's own call bound.
func (b *Backend) TargetObjects(spec embedgen.Spec) (embedgen.TargetObjects, error) {
	return TargetObjects(context.Background(), b.session.Store, b.session.Engine, spec)
}

// TargetObjects asks a provider offline what a generation needs on engine,
// and answers its refusal as an error that names it.
func TargetObjects(ctx context.Context, store provider.EmbeddingStore, engine string, spec embedgen.Spec) (embedgen.TargetObjects, error) {
	record, err := specRecord(spec)
	if err != nil {
		return embedgen.TargetObjects{}, err
	}
	described, err := store.DescribeEmbeddingTarget(ctx, &protocol.DescribeEmbeddingTargetRequest{Engine: engine, Spec: record})
	if err != nil {
		return embedgen.TargetObjects{}, fmt.Errorf("describe the target objects on engine %q: %w", engine, err)
	}
	if refusal := described.GetRefusal(); refusal != nil {
		return embedgen.TargetObjects{}, fmt.Errorf("%w: target objects: engine %q cannot hold this specification (%s): %s",
			ErrUnsupported, engine, refusal.GetReason(), refusal.GetMessage())
	}
	objects := embedgen.TargetObjects{
		Column: embedgenColumn(spec, described.GetColumnType()), HasIndex: described.GetHasIndex(),
		RequiredExtensions: slices.Clone(described.GetRequiredExtensions()), OwnsTable: described.GetOwnsTable(),
	}
	if objects.OwnsTable {
		objects.TableComment = record.GetNames().GetOwnershipMark()
	}
	if described.GetReferencesSource() {
		objects.ForeignKeyName = embedgen.ForeignKeyName(spec.Target.Table, spec.Target.Column, record.GetGeneration())
	}
	if objects.HasIndex {
		objects.Index.StructName = spec.Target.Table
		objects.Index.Name = record.GetNames().GetIndex()
		objects.Index.Fields = []string{spec.Target.Column}
		objects.Index.Type = described.GetIndexMethod()
		objects.Index.Operator = described.GetIndexOperator()
	}
	return objects, nil
}

// embedgenColumn is the vector column a generation writes, of the type the
// engine named. It is nullable, because a row without its vector is the
// normal state during a backfill.
func embedgenColumn(spec embedgen.Spec, columnType string) schemamodel.Field {
	return schemamodel.Field{StructName: spec.Target.Table, Name: spec.Target.Column, Type: columnType, Nullable: true, Comment: spec.ColumnComment()}
}

// CheckMode refuses a consistency mode the engine does not declare, naming
// the ones it does. No mode at all installs nothing and needs nothing of the
// engine, so it is never refused here.
func (b *Backend) CheckMode(mode embedcatchup.Mode) error {
	if mode == embedcatchup.ModeNone || slices.Contains(b.session.ConsistencyModes, string(mode)) {
		return nil
	}
	return fmt.Errorf("%w: %s builds generations in consistency mode %s, and the specification selects %q; "+
		"use a mode the engine declares", ErrUnsupported, b.engineName(), strings.Join(b.session.ConsistencyModes, ", "), string(mode))
}

// Outbox refuses: the embedding store has no change capture, which a later
// contract minor adds as its own feature.
func (b *Backend) Outbox(context.Context, embedgen.Spec) (embedbackend.Outbox, error) {
	if err := b.CheckMode(embedcatchup.ModeOutbox); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%w: %s captures no source changes through the embedding store", ErrUnsupported, b.engineName())
}
