package providerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	providerruntime "github.com/stokaro/ptah/provider-sdk/runtime"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
)

type session struct {
	target    *target
	remote    *protocol.Session
	address   string
	info      catalog.ServerInfo
	dry       atomic.Bool
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func (t *target) Open(ctx context.Context, request dbschema.NativeRequest) (dbschema.NativeSession, error) {
	remote, err := t.host.client.Open(ctx, &protocol.OpenRequest{Engine: t.engine.GetId(), Address: request.Address})
	if err != nil {
		return nil, err
	}
	info, err := t.sessionInfo(remote)
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), providerruntime.CallTimeout)
		defer cancel()
		return nil, errors.Join(err, t.host.client.CloseSession(cleanup, remote.GetId()))
	}
	return &session{target: t, remote: remote, address: request.Address, info: info}, nil
}

func (t *target) sessionInfo(remote *protocol.Session) (catalog.ServerInfo, error) {
	rules, err := identifierRules(remote.GetIdentifiers(), remote.GetDefaultSchema())
	if err != nil {
		return catalog.ServerInfo{}, err
	}
	flags := make(map[capability.Capability]bool, len(remote.GetCapabilities()))
	for name, value := range remote.GetCapabilities() {
		flags[capability.Capability(name)] = value
	}
	caps := capability.Of(flags)
	if err := caps.Validate(); err != nil {
		return catalog.ServerInfo{}, fmt.Errorf("provider session capabilities: %w", err)
	}
	identity, err := t.identity(remote)
	if err != nil {
		return catalog.ServerInfo{}, err
	}
	return catalog.ServerInfo{Dialect: remote.GetEngine(), Schema: remote.GetDefaultSchema(), Version: remote.GetServerVersion(), IdentifierSemantics: rules, Provider: identity, Capabilities: caps}, nil
}

func (s *session) Info() catalog.ServerInfo { return s.info }
func (s *session) Reader(schemas []string) catalog.SchemaReader {
	return reader{session: s, schemas: slices.Clone(schemas)}
}
func (s *session) Writer() catalog.SchemaWriter { return s }
func (s *session) OpenSession(ctx context.Context) (dbschema.NativeSession, error) {
	if s.closed.Load() {
		return nil, fmt.Errorf("provider session is closed")
	}
	return s.target.Open(ctx, dbschema.NativeRequest{Address: s.address})
}
func (*session) RestrictUntrustedSQL(context.Context) error {
	return unsupported("untrusted SQL isolation")
}
func (s *session) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), providerruntime.CallTimeout)
		defer cancel()
		s.closeErr = s.target.host.client.CloseSession(ctx, s.remote.GetId())
	})
	return s.closeErr
}
func (s *session) IsDryRun() bool       { return s.dry.Load() }
func (s *session) SetDryRun(value bool) { s.dry.Store(value) }
func (s *session) DropAllTables(context.Context) error {
	if s.IsDryRun() {
		return nil
	}
	return unsupported("database cleanup")
}
func (s *session) ExecuteSQL(ctx context.Context, sql string, args ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return fmt.Errorf("provider session is closed")
	}
	if len(args) != 0 {
		return unsupported("bound SQL parameters")
	}
	if s.IsDryRun() {
		return nil
	}
	return s.execute(ctx, "", sql)
}

func (s *session) execute(ctx context.Context, transactionID, sql string) error {
	_, err := s.target.host.client.Execute(ctx, &protocol.ExecuteRequest{
		SessionId: s.remote.GetId(), RequestId: rand.Text(), TransactionId: transactionID,
		Units: []*protocol.Unit{{ChangeId: "statement", Body: &protocol.Unit_Sql{Sql: sql}}},
	})
	if err != nil {
		return fmt.Errorf("provider execution failed; inspect the database before retrying: %w", nativeOutcomeError(err))
	}
	return nil
}

type dryTransaction struct{}

func (dryTransaction) ExecuteSQL(context.Context, string, ...any) error { return nil }
func (dryTransaction) IsDryRun() bool                                   { return true }
func (dryTransaction) Commit() error                                    { return nil }
func (dryTransaction) Rollback() error                                  { return nil }

func unsupported(operation string) error {
	return fmt.Errorf("%w: the experimental provider contract does not support %s", ptaherr.ErrUnsupportedFeature, operation)
}
