package dbschema_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/connectgate"
)

type nativeFixture struct {
	opens    atomic.Int64
	next     atomic.Int64
	mu       sync.Mutex
	sessions []*nativeHandle
	requests []dbschema.NativeRequest
	failure  error
	metadata *catalog.ServerInfo
}

func (f *nativeFixture) Open(_ context.Context, request dbschema.NativeRequest) (dbschema.NativeSession, error) {
	f.opens.Add(1)
	if f.failure != nil {
		return nil, f.failure
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, request)
	handle := &nativeHandle{fixture: f, id: f.next.Add(1)}
	f.sessions = append(f.sessions, handle)
	return handle, nil
}

type nativeHandle struct {
	fixture *nativeFixture
	id      int64
	closed  atomic.Bool
	closes  atomic.Int64
	dry     atomic.Bool
	writes  atomic.Int64
}

func (h *nativeHandle) Info() catalog.ServerInfo {
	if h.fixture.metadata != nil {
		return *h.fixture.metadata
	}
	return catalog.ServerInfo{Dialect: "example", Schema: "main", IdentifierSemantics: identifier.ForDialect("sqlite")}
}
func (h *nativeHandle) Reader(schemas []string) catalog.SchemaReader {
	return nativeReader{h, slices.Clone(schemas)}
}
func (h *nativeHandle) Writer() catalog.SchemaWriter { return h }
func (h *nativeHandle) OpenSession(ctx context.Context) (dbschema.NativeSession, error) {
	return h.fixture.Open(ctx, dbschema.NativeRequest{})
}
func (*nativeHandle) RestrictUntrustedSQL(context.Context) error {
	return ptaherr.ErrUnsupportedFeature
}
func (h *nativeHandle) Close() error {
	if h.closed.CompareAndSwap(false, true) {
		h.closes.Add(1)
	}
	return nil
}
func (h *nativeHandle) ExecuteSQL(context.Context, string, ...any) error {
	if !h.dry.Load() {
		h.writes.Add(1)
	}
	return nil
}
func (h *nativeHandle) IsDryRun() bool                    { return h.dry.Load() }
func (h *nativeHandle) SetDryRun(value bool)              { h.dry.Store(value) }
func (*nativeHandle) DropAllTables(context.Context) error { return ptaherr.ErrUnsupportedFeature }
func (*nativeHandle) BeginTransaction(context.Context) (catalog.SchemaTransaction, error) {
	return nil, ptaherr.ErrUnsupportedFeature
}

type nativeReader struct {
	handle  *nativeHandle
	schemas []string
}

func (r nativeReader) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}
func (r nativeReader) ReadSchemaContext(ctx context.Context) (*catalog.Database, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &catalog.Database{Tables: []catalog.Table{{Name: fmt.Sprintf("handle_%d", r.handle.id), Schema: fmt.Sprint(r.schemas)}}}, nil
}

func nativeContext(c *qt.C, fixture *nativeFixture) context.Context {
	c.Helper()
	registrations := map[string]dbschema.NativeRegistration{"example": {Target: "example", Connector: fixture}}
	ctx, err := dbschema.WithNativeConnections(c.Context(), registrations)
	c.Assert(err, qt.IsNil)
	delete(registrations, "example")
	return ctx
}

func TestNativeConnectionKeepsReadsAndWritesOnItsScopedHandle(t *testing.T) {
	c := qt.New(t)
	fixture := &nativeFixture{}
	conn, err := dbschema.ConnectToDatabase(nativeContext(c, fixture), "example://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	callbackErr := errors.New("callback failed")
	err = conn.WithSession(c.Context(), func(scoped *dbschema.DatabaseConnection) error {
		snapshot, readErr := dbschema.ReadSchemaWithSchemasContext(c.Context(), scoped, []string{"selected"})
		c.Assert(readErr, qt.IsNil)
		c.Assert(snapshot.Tables, qt.DeepEquals, []catalog.Table{{Name: "handle_2", Schema: "[selected]"}})
		c.Assert(scoped.Writer().ExecuteSQL(c.Context(), "write"), qt.IsNil)
		c.Assert(scoped.Close(), qt.IsNil)
		c.Assert(scoped.WithSessionOrCurrent(c.Context(), func(current *dbschema.DatabaseConnection) error {
			c.Assert(current, qt.Equals, scoped)
			return nil
		}), qt.IsNil)
		return callbackErr
	})
	c.Assert(err, qt.ErrorIs, callbackErr)
	c.Assert(fixture.opens.Load(), qt.Equals, int64(2))
	c.Assert(fixture.sessions[1].writes.Load(), qt.Equals, int64(1))
	c.Assert(fixture.sessions[1].closes.Load(), qt.Equals, int64(1))
	c.Assert(fixture.sessions[0].closes.Load(), qt.Equals, int64(0))
	c.Assert(conn.Close(), qt.IsNil)
	c.Assert(conn.Close(), qt.IsNil)
	c.Assert(fixture.sessions[0].closes.Load(), qt.Equals, int64(1))
}

func TestNativeConnectionRefusesUnavailableRawSQL(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(nativeContext(c, &nativeFixture{}), "example://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	//nolint:rowserrcheck // This refusal must return nil rows, asserted below.
	rows, err := conn.QueryContext(c.Context(), "query")
	c.Assert(rows, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(conn.QueryRowContext(c.Context(), "query").Scan(new(int)), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(conn.QueryRow("query").Err(), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	result, err := conn.ExecContext(c.Context(), "write")
	c.Assert(result, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	tx, err := conn.BeginTx(c.Context(), nil)
	c.Assert(tx, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	raw, err := conn.Conn(c.Context())
	c.Assert(raw, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	called := false
	err = conn.WithUntrustedSQLSession(c.Context(), func(*dbschema.DatabaseConnection) error { called = true; return nil })
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(called, qt.IsFalse)
}

func TestNativeConnectionSelectionRefusesBeforeOpening(t *testing.T) {
	c := qt.New(t)
	fixture := &nativeFixture{}
	selected := nativeContext(c, fixture)
	refusal := errors.New("target forbidden")
	guarded := connectgate.With(selected, func(target string) error { c.Assert(target, qt.Equals, "example"); return refusal })
	conn, err := dbschema.ConnectToDatabase(guarded, "example://database")
	c.Assert(conn, qt.IsNil)
	c.Assert(err, qt.ErrorIs, refusal)
	canceled, cancel := context.WithCancel(selected)
	cancel()
	conn, err = dbschema.ConnectToDatabase(canceled, "example://database")
	c.Assert(conn, qt.IsNil)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(fixture.opens.Load(), qt.Equals, int64(0))
	conn, err = dbschema.ConnectToDatabase(c.Context(), "example://database")
	c.Assert(conn, qt.IsNil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(fixture.opens.Load(), qt.Equals, int64(0))
}

func TestSelectedNativeFailureNeverOpensBundledDatabase(t *testing.T) {
	c := qt.New(t)
	refusal := errors.New("provider failed")
	fixture := &nativeFixture{failure: refusal}
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{"sqlite": {Target: "sqlite", Connector: fixture}})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(conn, qt.IsNil)
	c.Assert(err, qt.ErrorIs, refusal)
	c.Assert(fixture.opens.Load(), qt.Equals, int64(1))
}

func TestNativeConnectionRefusesInvalidSessionMetadata(t *testing.T) {
	tests := []struct {
		name string
		info catalog.ServerInfo
	}{
		{name: "another target", info: catalog.ServerInfo{Dialect: "another", IdentifierSemantics: identifier.ForDialect("sqlite")}},
		{name: "missing identifier semantics", info: catalog.ServerInfo{Dialect: "example"}},
		{name: "whole server instead of a database", info: catalog.ServerInfo{Dialect: "example", WholeServer: true, IdentifierSemantics: identifier.ForDialect("sqlite")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := &nativeFixture{metadata: &test.info}
			conn, err := dbschema.ConnectToDatabase(nativeContext(c, fixture), "example://database")
			c.Assert(err, qt.IsNotNil)
			c.Assert(conn, qt.IsNil)
			c.Assert(fixture.sessions[0].closes.Load(), qt.Equals, int64(1))
		})
	}
}

func TestNativeSessionPreservesScopeAndDryRun(t *testing.T) {
	c := qt.New(t)
	fixture := &nativeFixture{}
	conn, err := dbschema.ConnectToDatabase(nativeContext(c, fixture), "example://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	conn.SchemaWriter().SetDryRun(true)
	c.Assert(conn.WithSession(c.Context(), func(scoped *dbschema.DatabaseConnection) error {
		c.Assert(scoped.Writer().IsDryRun(), qt.IsTrue)
		return scoped.Writer().ExecuteSQL(c.Context(), "write")
	}), qt.IsNil)
	c.Assert(fixture.sessions[1].writes.Load(), qt.Equals, int64(0))
	fixture.metadata = &catalog.ServerInfo{Dialect: "example", Schema: "another", IdentifierSemantics: identifier.ForDialect("sqlite")}
	called := false
	err = conn.WithSession(c.Context(), func(*dbschema.DatabaseConnection) error { called = true; return nil })
	c.Assert(err, qt.ErrorMatches, "native session changed the connected scope")
	c.Assert(called, qt.IsFalse)
	c.Assert(fixture.sessions[2].closes.Load(), qt.Equals, int64(1))
}

func TestNativeConnectionHidesTheProviderAddress(t *testing.T) {
	c := qt.New(t)
	fixture := &nativeFixture{}
	// #nosec G101 -- Made-up credentials verify that every address component is hidden.
	address := "example://user:password@host/private-key?custom-secret=token"
	conn, err := dbschema.ConnectToDatabase(nativeContext(c, fixture), address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	c.Assert(conn.Info().URL, qt.Equals, address)
	c.Assert(conn.Info().RedactedURL, qt.Equals, "example://redacted")
	c.Assert(fixture.requests[0].Address, qt.Equals, address)
}

func TestNativeConnectionAcceptsAnExplicitServerScope(t *testing.T) {
	c := qt.New(t)
	fixture := &nativeFixture{metadata: &catalog.ServerInfo{Dialect: "example", WholeServer: true, IdentifierSemantics: identifier.ForDialect("sqlite")}}
	conn, err := dbschema.ConnectToServer(nativeContext(c, fixture), "example://server")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	c.Assert(conn.Info().WholeServer, qt.IsTrue)
	c.Assert(fixture.requests[0].Scope, qt.Equals, dbschema.ServerScope)
}
