package atlasschema

// White-box testing required: rehearsalSeed.target chooses how the rehearsal
// reads a provider's target, and what it sends is observable only as the
// selectors of each provider read. Through the CLI a fixture session always
// names the dev session's own default schema, so the session's schema is read
// whether or not the target sends it, and a second read of a whole database
// returns what the first did; neither difference reaches a command's result.

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
)

// seedProvider is a provider session whose engine carries namespaces where
// carries says so, and which records the schema selectors of each read.
type seedProvider struct {
	carries bool
	reads   [][]string
}

func (p *seedProvider) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return seedSession{p}, nil
}

type seedSession struct{ provider *seedProvider }

func (seedSession) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: "seed", Schema: "ledger", IdentifierSemantics: identifier.ForDialect("sqlite"), Provider: catalog.ProviderIdentity{
		ID: "example.provider", Version: "test", SemanticIdentity: "seed-1", ProtocolRevision: 2,
		ArtifactSHA256: strings.Repeat("a", 64), ContractSHA256: strings.Repeat("b", 64),
	}}
}
func (s seedSession) Reader(schemas []string) catalog.SchemaReader {
	return seedReader{s.provider, schemas}
}
func (s seedSession) Writer() catalog.SchemaWriter { return seedWriter{carries: s.provider.carries} }
func (s seedSession) OpenSession(ctx context.Context) (dbschema.NativeSession, error) {
	return s.provider.Open(ctx, dbschema.NativeRequest{})
}
func (seedSession) RestrictUntrustedSQL(context.Context) error { return ptaherr.ErrUnsupportedFeature }
func (seedSession) Close() error                               { return nil }

type seedReader struct {
	provider *seedProvider
	schemas  []string
}

func (r seedReader) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}
func (r seedReader) ReadSchemaContext(context.Context) (*catalog.Database, error) {
	r.provider.reads = append(r.provider.reads, r.schemas)
	return &catalog.Database{}, nil
}

// seedWriter reports whether the session's engine carries namespaces. A
// rehearsal target calls nothing else on it.
type seedWriter struct {
	catalog.SchemaWriter
	carries bool
}

func (w seedWriter) CarriesNamespaces() bool { return w.carries }

func connectSeedProvider(c *qt.C, provider *seedProvider) *dbschema.DatabaseConnection {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{"seed": {Target: "seed", Connector: provider}})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "seed://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return conn
}

// A provider whose engine carries namespaces is asked for the schemas the
// statements name and the schema the target's session names, ledger, which
// no statement names.
func TestRehearsalTargetOfANamespaceProvider_HappyPath(t *testing.T) {
	c := qt.New(t)
	provider := &seedProvider{carries: true}
	target := rehearsalSeed{narrowed: true}.target(c.Context(), connectSeedProvider(c, provider))

	_, err := target([]string{"app"})

	c.Assert(err, qt.IsNil)
	c.Assert(provider.reads, qt.DeepEquals, [][]string{{"app", "ledger"}})
}

// A provider whose engine carries no namespaces is read once, for the whole
// database, however many times the rehearsal asks.
func TestRehearsalTargetOfAProviderWithoutNamespaces_HappyPath(t *testing.T) {
	c := qt.New(t)
	provider := &seedProvider{}
	target := rehearsalSeed{narrowed: true}.target(c.Context(), connectSeedProvider(c, provider))

	first, firstErr := target([]string{"app"})
	second, secondErr := target([]string{"app", "identity"})

	c.Assert(firstErr, qt.IsNil)
	c.Assert(secondErr, qt.IsNil)
	c.Assert(second, qt.Equals, first)
	c.Assert(provider.reads, qt.DeepEquals, [][]string{nil})
}
