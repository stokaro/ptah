package dbschema

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/connectgate"
	"ptah.run/internal/connector"
)

// ConnectionScope identifies whether opening a whole server is permitted.
type ConnectionScope uint8

const (
	// DatabaseScope requires a named database rather than a whole server.
	DatabaseScope ConnectionScope = iota
	// ServerScope permits a whole-server connection when the provider supports it.
	ServerScope
)

// NativeRequest carries a connection address and the scope the caller accepts.
// Address can contain credentials and must not be included in diagnostics.
type NativeRequest struct {
	Address string
	Scope   ConnectionScope
}

// NativeConnector opens provider-owned sessions without a database/sql driver.
// A failed open returns no usable session and must release partial resources.
type NativeConnector interface {
	Open(context.Context, NativeRequest) (NativeSession, error)
}

// NativeSession owns one provider handle. Readers and writers use that handle;
// OpenSession creates an independent handle on the same target. Close is
// idempotent. Implementations must be safe for concurrent calls.
type NativeSession interface {
	Info() catalog.ServerInfo
	Reader(schemas []string) catalog.SchemaReader
	Writer() catalog.SchemaWriter
	OpenSession(context.Context) (NativeSession, error)
	RestrictUntrustedSQL(context.Context) error
	Close() error
}

// NativeRegistration binds a URL scheme to an explicitly selected target and
// connector. Failure of that connector never selects a bundled implementation.
type NativeRegistration struct {
	Target    string
	Connector NativeConnector
}

type nativeConnectionsKey struct{}

// WithNativeConnections freezes the selected scheme bindings for this request.
// The context owns selection, not process lifetime; the application closes its
// providers after its commands finish. Registrations are copied before use.
func WithNativeConnections(ctx context.Context, registrations map[string]NativeRegistration) (context.Context, error) {
	selected := make(map[string]NativeRegistration, len(registrations))
	for scheme, registration := range registrations {
		if scheme == "" || scheme != strings.ToLower(strings.TrimSpace(scheme)) || strings.TrimSpace(registration.Target) == "" || nilNativeService(registration.Connector) {
			return nil, fmt.Errorf("invalid native connection registration")
		}
		selected[scheme] = registration
	}
	return context.WithValue(ctx, nativeConnectionsKey{}, selected), nil
}

func nilNativeService(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func nativeConnection(ctx context.Context, scheme, address string, scope ConnectionScope) (*DatabaseConnection, bool, error) {
	selected, _ := ctx.Value(nativeConnectionsKey{}).(map[string]NativeRegistration)
	registration, found := selected[scheme]
	if !found {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	if err := connectgate.Check(ctx, registration.Target); err != nil {
		return nil, true, err
	}
	session, err := registration.Connector.Open(ctx, NativeRequest{Address: address, Scope: scope})
	if err != nil {
		return nil, true, err
	}
	result, err := newNativeConnection(session, registration.Target, address, scope)
	if err != nil && !nilNativeService(session) {
		err = errors.Join(err, session.Close())
	}
	return result, true, err
}

func newNativeConnection(session NativeSession, target, address string, scope ConnectionScope) (*DatabaseConnection, error) {
	if nilNativeService(session) {
		return nil, fmt.Errorf("native connector returned a nil session")
	}
	info := session.Info()
	if info.Dialect != target || info.IdentifierSemantics.IsZero() {
		return nil, fmt.Errorf("native session returned invalid target metadata")
	}
	if scope == DatabaseScope && info.WholeServer {
		return nil, fmt.Errorf("native session opened a whole server where a database is required")
	}
	if err := info.Provider.Validate(); err != nil {
		return nil, fmt.Errorf("native session returned invalid provider identity: %w", err)
	}
	info.URL = address
	// A provider URL can carry secrets in fields the host does not recognize.
	// Keep only the target in the display form rather than guessing its grammar.
	info.RedactedURL = target + "://redacted"
	info.IdentifierSemantics = info.IdentifierSemantics.Clone()
	reader, writer := session.Reader(nil), session.Writer()
	if nilNativeService(reader) || nilNativeService(writer) {
		return nil, fmt.Errorf("native session returned a nil schema service")
	}
	return &DatabaseConnection{native: session, info: info, reader: reader, writer: writer, service: connector.Base{}}, nil
}

func (dc *DatabaseConnection) withNativeSession(ctx context.Context, use func(*DatabaseConnection) error) (resultErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	session, err := dc.native.OpenSession(ctx)
	if err != nil {
		return err
	}
	if nilNativeService(session) {
		return fmt.Errorf("native connector returned a nil session")
	}
	defer func() { resultErr = errors.Join(resultErr, session.Close()) }()
	scope := DatabaseScope
	if dc.info.WholeServer {
		scope = ServerScope
	}
	scoped, err := newNativeConnection(session, dc.info.Dialect, dc.info.URL, scope)
	if err != nil {
		return err
	}
	if scoped.info.Schema != dc.info.Schema || scoped.info.WholeServer != dc.info.WholeServer {
		return fmt.Errorf("native session changed the connected scope")
	}
	if scoped.info.Provider != dc.info.Provider {
		return fmt.Errorf("native session changed the selected provider identity")
	}
	scoped.pinned = true
	if dc.writer.IsDryRun() {
		scoped.writer.SetDryRun(true)
	}
	return use(scoped)
}

type refusedRow struct{ err error }

func (r refusedRow) Scan(...any) error { return r.err }
func (r refusedRow) Err() error        { return r.err }

func rawSQLUnavailable() error {
	return fmt.Errorf("%w: this connection does not expose database/sql operations", ptaherr.ErrUnsupportedFeature)
}
