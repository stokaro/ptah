// Package embedconnect opens the inference backend a database URL names.
//
// The URL's scheme decides the engine, the way it decides the dialect for
// every other command. A scheme an executable provider the invocation selected
// serves opens that provider's embedding store (ADR 0032). Otherwise a
// PostgreSQL URL, or a keyword/value DSN with no scheme, opens the pgvector
// backend, and a ydb:// or ydbs:// URL opens the YDB backend. `ptah inference`
// and the inference tools of `ptah mcp` both open their database here, so the
// two surfaces accept and refuse the same URLs in the same words.
package embedconnect

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedrpc"
	"ptah.run/internal/embedydb"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerselect"
)

// RefuseAnotherEngine says what an inference migration speaks to, before a
// driver says it cannot parse a URL.
//
// Handed another engine's URL, the PostgreSQL driver fails with `cannot parse
// ...: invalid keyword/value`, which reads as a malformed connection string and
// sends an operator to check one that is correct (stokaro/ptah#2386). Both
// surfaces ask this before they dial, so they give one answer.
//
// It refuses only a scheme Ptah RECOGNIZES as another dialect and no selected
// provider serves. An unrecognized scheme, and a keyword/value DSN with no
// scheme at all, still reach the PostgreSQL driver: `host=localhost user=ptah`
// is a form pgx accepts, and this must not start rejecting it for having no
// scheme to read. The PostgreSQL family is refused by name: CockroachDB,
// YugabyteDB and Spanner speak the wire protocol and have no pgvector.
func RefuseAnotherEngine(ctx context.Context, dbURL string) error {
	if providerServes(ctx, dbURL) {
		return nil
	}
	dialect, scheme := dialectOf(dbURL)
	switch dialect {
	case "", platform.Postgres, platform.YDB:
		return nil
	}
	// The dialect is named once. Saying it mid-sentence as well would put the
	// same answer in two places, and neither could then be measured.
	return fmt.Errorf(
		"ptah inference works against PostgreSQL with pgvector, against YDB, and against a provider engine "+
			"with an embedding store, and %q names another engine: a generation's run state and its vectors "+
			"need one of those, so there is nothing here to run against %s",
		scheme+"://", dialect)
}

// Open connects to the database a URL names and returns its backend.
func Open(ctx context.Context, dbURL string) (embedbackend.Backend, error) {
	if err := RefuseAnotherEngine(ctx, dbURL); err != nil {
		return nil, err
	}
	// Each branch returns its own nil on failure: a nil *Backend converted to
	// the interface is not a nil interface.
	if providerServes(ctx, dbURL) {
		backend, err := openProvider(ctx, dbURL)
		if err != nil {
			return nil, err
		}
		return backend, nil
	}
	if dialect, _ := dialectOf(dbURL); dialect == platform.YDB {
		backend, err := embedydb.Open(ctx, dbURL)
		if err != nil {
			return nil, err
		}
		return backend, nil
	}
	backend, err := embedpg.Open(ctx, dbURL)
	if err != nil {
		return nil, err
	}
	return backend, nil
}

// openProvider connects through the provider that serves the URL and returns
// the backend over its embedding store. The connection is the one every other
// command opens for the URL, so it is checked and gated the same way.
func openProvider(ctx context.Context, dbURL string) (*embedrpc.Backend, error) {
	connection, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", dburldisplay.Format(dbURL), err)
	}
	store, err := providerhost.EmbeddingSession(connection)
	if err != nil {
		return nil, errors.Join(err, connection.Close())
	}
	return embedrpc.New(session(store), connection.Close), nil
}

// session is a provider's embedding store as the backend takes it.
func session(store providerhost.EmbeddingStore) embedrpc.Session {
	return embedrpc.Session{Store: store.Client, Engine: store.Engine, ConsistencyModes: store.ConsistencyModes, ID: store.SessionID}
}

// providerServes reports whether a provider the invocation selected serves the
// URL's scheme.
func providerServes(ctx context.Context, dbURL string) bool {
	scheme, _, found := strings.Cut(strings.TrimSpace(dbURL), ":")
	if !found || ctx == nil {
		return false
	}
	_, served := dbschema.NativeSources(ctx)[strings.ToLower(scheme)]
	return served
}

// TargetObjects derives a generation's column and index without a database,
// for `ptah inference describe`.
//
// A provider the invocation selected answers for its engine, and is the only
// way to describe an engine that only a provider serves. Without one, the
// representation decides the engine, because each built-in engine names a type
// only it has: `vector`, `halfvec` and `sparsevec` are pgvector's, and `float`
// is YDB's element type. A specification naming neither is refused by the
// PostgreSQL derivation, which names the types it knows.
func TargetObjects(ctx context.Context, spec embedgen.Spec) (embedgen.TargetObjects, error) {
	if started := providerselect.StartedFrom(ctx); started != nil {
		store, err := started.EmbeddingStore()
		if err != nil {
			return embedgen.TargetObjects{}, err
		}
		return embedrpc.TargetObjects(ctx, store.Client, store.Engine, spec)
	}
	if strings.EqualFold(strings.TrimSpace(spec.Target.Representation), embedydb.FloatRepresentation) {
		return embedydb.TargetObjects(spec)
	}
	return spec.TargetObjects()
}

// dialectOf reads the dialect a URL's scheme names, and the scheme as written.
// A URL with no scheme names no dialect.
func dialectOf(dbURL string) (dialect, scheme string) {
	scheme, _, found := strings.Cut(dbURL, "://")
	if !found {
		return "", ""
	}
	return platform.NormalizeDialect(strings.ToLower(scheme)), scheme
}
