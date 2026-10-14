// Package embedconnect opens the inference backend a database URL names.
//
// The URL's scheme decides the engine, the way it decides the dialect for
// every other command: a PostgreSQL URL, or a keyword/value DSN with no
// scheme, opens the pgvector backend, and a ydb:// or ydbs:// URL opens the
// YDB backend. `ptah inference` and the inference tools of `ptah mcp` both
// open their database here, so the two surfaces accept and refuse the same
// URLs in the same words.
package embedconnect

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedydb"
)

// RefuseAnotherEngine says what an inference migration speaks to, before a
// driver says it cannot parse a URL.
//
// It refuses only a scheme Ptah RECOGNIZES as another dialect. An
// unrecognized scheme, and a keyword/value DSN with no scheme at all, still
// reach the PostgreSQL driver: `host=localhost user=ptah` is a form pgx
// accepts, and this must not start rejecting it for having no scheme to read.
// The PostgreSQL family is refused by name: CockroachDB, YugabyteDB and
// Spanner speak the wire protocol and have no pgvector.
func RefuseAnotherEngine(dbURL string) error {
	dialect, scheme := dialectOf(dbURL)
	switch dialect {
	case "", platform.Postgres, platform.YDB:
		return nil
	}
	// The dialect is named once. Saying it mid-sentence as well would put the
	// same answer in two places, and neither could then be measured.
	return fmt.Errorf(
		"ptah inference works against PostgreSQL with pgvector and against YDB, and %q names another "+
			"engine: a generation's run state and its vectors need one of those two, so there is nothing "+
			"here to run against %s",
		scheme+"://", dialect)
}

// Open connects to the database a URL names and returns its backend.
func Open(ctx context.Context, dbURL string) (embedbackend.Backend, error) {
	if err := RefuseAnotherEngine(dbURL); err != nil {
		return nil, err
	}
	// Each branch returns its own nil on failure: a nil *Backend converted to
	// the interface is not a nil interface.
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

// TargetObjects derives a generation's column and index without a database,
// for `ptah inference describe`.
//
// The representation decides the engine, because each one names a type only
// one engine has: `vector`, `halfvec` and `sparsevec` are pgvector's, and
// `float` is YDB's element type. A specification naming neither is refused by
// the PostgreSQL derivation, which names the types it knows.
func TargetObjects(spec embedgen.Spec) (embedgen.TargetObjects, error) {
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
