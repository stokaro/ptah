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

// NativeLookup resolves a URL scheme to a native registration when a connection
// needs it, for a caller that cannot name its providers before the command runs.
// A lookup is additive: a scheme it does not serve keeps the bundled driver, and
// it is consulted only when no exclusive selection (see
// [WithNativeConnections]) is on the context.
type NativeLookup interface {
	// NativeRegistration returns the registration serving scheme, which is
	// lower-case. It reports false when no provider serves the scheme. An error
	// refuses the connection; it never selects a bundled driver instead.
	NativeRegistration(ctx context.Context, scheme string) (NativeRegistration, bool, error)
}

// NativeSchemeLister is implemented by a [NativeLookup] that can name the URL
// schemes it serves without starting anything, so a caller can tell a database
// URL from another source before any connection opens.
type NativeSchemeLister interface {
	// NativeSchemes returns each served lower-case scheme and the canonical
	// target it binds to.
	NativeSchemes() map[string]string
}

type nativeConnectionsKey struct{}

type nativeLookupKey struct{}

// WithNativeLookup returns a context under which connections consult lookup for
// a scheme no exclusive selection registered. A later call replaces the lookup;
// the context owns selection, not process lifetime.
func WithNativeLookup(ctx context.Context, lookup NativeLookup) context.Context {
	return context.WithValue(ctx, nativeLookupKey{}, lookup)
}

// WithNativeConnections freezes the selected scheme bindings for this request.
// The context owns selection, not process lifetime; the application closes its
// providers after its commands finish. Registrations are copied before use.
// Selection is exclusive: an unregistered URL scheme is refused, including
// schemes for bundled drivers. An empty selection permits no connections.
func WithNativeConnections(ctx context.Context, registrations map[string]NativeRegistration) (context.Context, error) {
	selected := make(map[string]NativeRegistration, len(registrations))
	for scheme, registration := range registrations {
		if !validRegistration(scheme, registration) {
			return nil, fmt.Errorf("invalid native connection registration")
		}
		selected[scheme] = registration
	}
	return context.WithValue(ctx, nativeConnectionsKey{}, selected), nil
}

// NativeTargets returns the selected URL schemes and their canonical targets.
// Source classification and connection dispatch must use the same frozen
// registrations. The returned copy exposes no connector or mutable selection.
func NativeTargets(ctx context.Context) map[string]string {
	selected, _ := ctx.Value(nativeConnectionsKey{}).(map[string]NativeRegistration)
	targets := make(map[string]string, len(selected))
	for scheme, registration := range selected {
		targets[scheme] = registration.Target
	}
	return targets
}

func validRegistration(scheme string, registration NativeRegistration) bool {
	return scheme != "" && scheme == strings.ToLower(strings.TrimSpace(scheme)) && strings.TrimSpace(registration.Target) != "" && !nilNativeService(registration.Connector)
}

// NativeSources returns the URL schemes a source classifier should treat as
// database URLs: the exclusive selection of [NativeTargets], or, when there is
// none, the schemes the context's [NativeLookup] serves. Unlike NativeTargets it
// is not a statement that built-in schemes are refused.
func NativeSources(ctx context.Context) map[string]string {
	if selected := NativeTargets(ctx); len(selected) != 0 {
		return selected
	}
	lister, installed := ctx.Value(nativeLookupKey{}).(NativeSchemeLister)
	if !installed || nilNativeService(lister) {
		return nil
	}
	return lister.NativeSchemes()
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
	selected, configured := ctx.Value(nativeConnectionsKey{}).(map[string]NativeRegistration)
	registration, found := selected[scheme]
	if !found && configured {
		return nil, true, fmt.Errorf("the selected provider does not own the database URL scheme")
	}
	if !found {
		var err error
		registration, found, err = lookedUp(ctx, scheme)
		if err != nil {
			return nil, true, err
		}
	}
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

// lookedUp asks the context's [NativeLookup], if any, who serves scheme.
func lookedUp(ctx context.Context, scheme string) (NativeRegistration, bool, error) {
	lookup, installed := ctx.Value(nativeLookupKey{}).(NativeLookup)
	if !installed || nilNativeService(lookup) {
		return NativeRegistration{}, false, nil
	}
	registration, found, err := lookup.NativeRegistration(ctx, scheme)
	if err != nil || !found {
		return NativeRegistration{}, false, err
	}
	if !validRegistration(scheme, registration) {
		return NativeRegistration{}, false, fmt.Errorf("invalid native connection registration")
	}
	return registration, true, nil
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
