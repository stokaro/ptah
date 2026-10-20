package atlasschema_test

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
)

type boundProvider struct {
	base    context.Context
	info    catalog.ServerInfo
	address string
	reads   atomic.Int64
}

func (p *boundProvider) Open(ctx context.Context, _ dbschema.NativeRequest) (dbschema.NativeSession, error) {
	// This fixture implements its own storage through the bundled SQLite
	// adapter. The host's exclusive URL selection belongs to the outer call.
	storage, cancel := context.WithCancel(p.base)
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := dbschema.ConnectToDatabase(storage, p.address)
	if err != nil {
		return nil, err
	}
	return &boundProviderSession{DatabaseConnection: conn, provider: p}, nil
}

type boundProviderSession struct {
	*dbschema.DatabaseConnection
	provider *boundProvider
}

func (s *boundProviderSession) Info() catalog.ServerInfo { return s.provider.info }
func (s *boundProviderSession) Writer() catalog.SchemaWriter {
	return s.DatabaseConnection.SchemaWriter()
}
func (s *boundProviderSession) Reader(schemas []string) catalog.SchemaReader {
	return boundProviderReader{s, schemas}
}
func (s *boundProviderSession) OpenSession(ctx context.Context) (dbschema.NativeSession, error) {
	return s.provider.Open(ctx, dbschema.NativeRequest{})
}
func (*boundProviderSession) RestrictUntrustedSQL(context.Context) error {
	return ptaherr.ErrUnsupportedFeature
}

type boundProviderReader struct {
	session *boundProviderSession
	schemas []string
}

func (r boundProviderReader) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}
func (r boundProviderReader) ReadSchemaContext(ctx context.Context) (*catalog.Database, error) {
	r.session.provider.reads.Add(1)
	return dbschema.ReadSchemaWithSchemasContext(ctx, r.session.DatabaseConnection, r.schemas)
}

func connectBoundProvider(c *qt.C, provider *boundProvider) *dbschema.DatabaseConnection {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{
		"external": {Target: provider.info.Dialect, Connector: provider},
	})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToServer(ctx, "external://opaque-address")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return conn
}

func prepareBoundPlan(c *qt.C) (*boundProvider, *dbschema.DatabaseConnection, atlasschema.PlanFile) {
	c.Helper()
	path := filepath.Join(c.TempDir(), "provider.db")
	original := connectPlanSQLite(c, path)
	info := original.Info()
	info.Provider = catalog.ProviderIdentity{
		ID: "example.provider", Version: "experiment", SemanticIdentity: "tables-1",
		ProtocolRevision: 2, ArtifactSHA256: strings.Repeat("a", 64), ContractSHA256: strings.Repeat("b", 64),
	}
	provider := &boundProvider{base: c.Context(), info: info, address: "sqlite://" + path}
	conn := connectBoundProvider(c, provider)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	plan, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{
		Runtime: runtime,
		Desired: &schemamodel.Database{
			Tables: []schemamodel.Table{{Name: "items", StructName: "Item"}},
			Fields: []schemamodel.Field{{Name: "id", StructName: "Item", Type: "INTEGER", Primary: true}},
		},
	})
	c.Assert(err, qt.IsNil)
	return provider, conn, plan
}

func TestSavedPlanPreservesAndVerifiesProviderBinding(t *testing.T) {
	c := qt.New(t)
	provider, conn, plan := prepareBoundPlan(c)
	c.Assert(plan.Provider, qt.IsNotNil)
	c.Assert(plan.Provider.Identity, qt.Equals, provider.info.Provider)
	c.Assert(plan.Provider.TargetSHA256, qt.Matches, "[0-9a-f]{64}")
	encoded, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	decoded, _, err := atlasschema.DecodePlanFile(encoded, "provider.plan.json")
	c.Assert(err, qt.IsNil)
	c.Assert(decoded.Provider, qt.DeepEquals, plan.Provider)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	c.Assert(atlasschema.VerifyPlanTarget(c.Context(), conn, decoded, runtime), qt.IsNil)
	_, err = atlasschema.MarshalPlanFileHCL(decoded)
	c.Assert(err, qt.ErrorMatches, ".*cannot preserve provider identity.*")
	c.Assert(string(encoded), qt.Not(qt.Contains), "opaque-address")
	c.Assert(string(encoded), qt.Not(qt.Contains), provider.address)
}

func TestSavedPlanRefusesChangedProviderBeforeReadingTheTarget(t *testing.T) {
	cases := []struct {
		name   string
		change func(*catalog.ServerInfo)
	}{
		{"provider", func(info *catalog.ServerInfo) { info.Provider.ID = "another.provider" }},
		{"artifact", func(info *catalog.ServerInfo) { info.Provider.ArtifactSHA256 = strings.Repeat("c", 64) }},
		{"version", func(info *catalog.ServerInfo) { info.Provider.Version = "another" }},
		{"semantics", func(info *catalog.ServerInfo) { info.Provider.SemanticIdentity = "tables-2" }},
		{"protocol", func(info *catalog.ServerInfo) { info.Provider.ProtocolRevision++ }},
		{"negotiation", func(info *catalog.ServerInfo) { info.Provider.ContractSHA256 = strings.Repeat("d", 64) }},
		{"capabilities", func(info *catalog.ServerInfo) {
			info.Capabilities = info.Capabilities.With(capability.ObjectExistenceGuards, false)
		}},
		{"identifiers", func(info *catalog.ServerInfo) { info.IdentifierSemantics.DefaultSchema = "another" }},
		{"scope", func(info *catalog.ServerInfo) { info.Schema = "another" }},
		{"server", func(info *catalog.ServerInfo) { info.Version = "another" }},
		{"whole server", func(info *catalog.ServerInfo) { info.WholeServer = true }},
		{"integer size", func(info *catalog.ServerInfo) { info.DefaultIntSize = 8 }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider, _, plan := prepareBoundPlan(c)
			changed := &boundProvider{base: c.Context(), info: provider.info, address: provider.address}
			test.change(&changed.info)
			conn := connectBoundProvider(c, changed)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			c.Assert(atlasschema.VerifyPlanTarget(c.Context(), conn, plan, runtime), qt.ErrorMatches, "plan provider binding changed; create and approve a new plan")
			c.Assert(changed.reads.Load(), qt.Equals, int64(0))
		})
	}
}

func TestSavedPlanRefusesMissingProviderAndEmbeddedSubstitution(t *testing.T) {
	c := qt.New(t)
	provider, conn, plan := prepareBoundPlan(c)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	missing := plan
	missing.Provider = nil
	reads := provider.reads.Load()
	c.Assert(atlasschema.VerifyPlanTarget(c.Context(), conn, missing, runtime), qt.ErrorMatches, "plan provider binding changed; create and approve a new plan")
	c.Assert(provider.reads.Load(), qt.Equals, reads)
	embedded, err := dbschema.ConnectToDatabase(c.Context(), provider.address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(embedded.Close(), qt.IsNil) })
	c.Assert(atlasschema.VerifyPlanTarget(c.Context(), embedded, plan, runtime), qt.ErrorMatches, "plan provider binding changed; create and approve a new plan")
}

func TestProviderTargetProjectionIgnoresIdentifierFactOrder(t *testing.T) {
	c := qt.New(t)
	provider, _, _ := prepareBoundPlan(c)
	provider.info.IdentifierSemantics.ResolvedNames = []identifier.ResolvedName{{Name: "b", Key: "b"}, {Name: "a", Key: "a"}}
	conn := connectBoundProvider(c, provider)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	plan, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{Runtime: runtime, Desired: &schemamodel.Database{}})
	c.Assert(err, qt.IsNil)
	changed := &boundProvider{base: c.Context(), info: provider.info, address: provider.address}
	changed.info.IdentifierSemantics.ResolvedNames = []identifier.ResolvedName{{Name: "a", Key: "a"}, {Name: "b", Key: "b"}}
	c.Assert(atlasschema.VerifyPlanTarget(c.Context(), connectBoundProvider(c, changed), plan, runtime), qt.IsNil)
}

func TestProviderTargetProjectionHasVersionedGolden(t *testing.T) {
	c := qt.New(t)
	provider, _, _ := prepareBoundPlan(c)
	provider.info.Version = "fixture-server"
	provider.info.Capabilities = capability.Capabilities{}
	provider.info.IdentifierSemantics = identifier.Semantics{
		DefaultSchema: "main", IndexNamespace: identifier.IndexNamespaceSchema,
		IndexNames: identifier.ComparisonExact, TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact,
	}
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	plan, err := atlasschema.PreparePlanFile(c.Context(), connectBoundProvider(c, provider), atlasschema.PlanFileOptions{Runtime: runtime, Desired: &schemamodel.Database{}})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Provider.TargetSHA256, qt.Equals, "489d3d5511aefc8cff3a76284ed9f63fda4819c5a0f6079cbf46974d3c0d04a4")
}

func TestSavedPlanRefusesMalformedProviderBinding(t *testing.T) {
	c := qt.New(t)
	_, _, plan := prepareBoundPlan(c)
	plan.Provider.TargetSHA256 = "not-a-digest"
	_, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.ErrorMatches, ".*plan provider target requires a lowercase SHA-256 digest.*")
	plan.Provider.TargetSHA256 = strings.Repeat("a", 64)
	plan.Provider.Identity.ProtocolRevision = 0
	_, err = atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.ErrorMatches, ".*provider identity requires a protocol revision.*")
}
