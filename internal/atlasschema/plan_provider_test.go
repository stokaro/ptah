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
	"ptah.run/core/schemaext"
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

// newPlanAdvice ends each refusal that names a selected provider.
const newPlanAdvice = "create and approve a new plan with the selected provider"

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
	c.Assert(plan.Provider.ID, qt.Equals, "example.provider")
	c.Assert(plan.Provider.Version, qt.Equals, "experiment")
	c.Assert(plan.Provider.ContractMajor, qt.Equals, uint32(2))
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
	c.Assert(string(encoded), qt.Not(qt.Contains), provider.info.Provider.ArtifactSHA256)
}

// The same release installed again, or built for another platform, has
// another executable digest; a host that negotiates another contract minor
// records another contract digest. Neither is the release the plan names, so
// both apply the plan.
func TestSavedPlanAcceptsAnotherBuildOfTheSameRelease(t *testing.T) {
	cases := []struct {
		name   string
		change func(*catalog.ProviderIdentity)
	}{
		{"executable digest", func(identity *catalog.ProviderIdentity) { identity.ArtifactSHA256 = strings.Repeat("c", 64) }},
		{"negotiated contract digest", func(identity *catalog.ProviderIdentity) { identity.ContractSHA256 = strings.Repeat("d", 64) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider, _, plan := prepareBoundPlan(c)
			rebuilt := &boundProvider{base: c.Context(), info: provider.info, address: provider.address}
			test.change(&rebuilt.info.Provider)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			c.Assert(atlasschema.VerifyPlanTarget(c.Context(), connectBoundProvider(c, rebuilt), plan, runtime), qt.IsNil)
		})
	}
}

func TestSavedPlanRefusesChangedProviderBeforeReadingTheTarget(t *testing.T) {
	const targetChanged = `plan provider target facts changed: the database context, server version, schema scope, capabilities or identifier rules differ from planning; ` + newPlanAdvice
	cases := []struct {
		name   string
		change func(*catalog.ServerInfo)
		want   string
	}{
		{"provider", func(info *catalog.ServerInfo) { info.Provider.ID = "another.provider" },
			`plan was computed with provider "example\.provider", but provider "another\.provider" serves the connection; ` + newPlanAdvice},
		{"release version", func(info *catalog.ServerInfo) { info.Provider.Version = "another" },
			`plan was computed with provider "example\.provider" version "experiment", but the selected provider is version "another"; ` + newPlanAdvice},
		{"contract major", func(info *catalog.ServerInfo) { info.Provider.ProtocolRevision++ },
			`plan was computed with provider "example\.provider" version "experiment" under provider contract major version 2, but the selected provider uses major version 3; ` + newPlanAdvice},
		{"capabilities", func(info *catalog.ServerInfo) {
			info.Capabilities = info.Capabilities.With(capability.ObjectExistenceGuards, false)
		}, targetChanged},
		{"identifiers", func(info *catalog.ServerInfo) { info.IdentifierSemantics.DefaultSchema = "another" }, targetChanged},
		{"scope", func(info *catalog.ServerInfo) { info.Schema = "another" }, targetChanged},
		{"server", func(info *catalog.ServerInfo) { info.Version = "another" }, targetChanged},
		{"whole server", func(info *catalog.ServerInfo) { info.WholeServer = true }, targetChanged},
		{"integer size", func(info *catalog.ServerInfo) { info.DefaultIntSize = 8 }, targetChanged},
		{"database context", func(info *catalog.ServerInfo) {
			info.DatabaseContext = schemaext.DatabaseContext{Database: "/other", SourceDatabase: "/source"}
		}, targetChanged},
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
			c.Assert(atlasschema.VerifyPlanTarget(c.Context(), conn, plan, runtime), qt.ErrorMatches, test.want)
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
	c.Assert(atlasschema.VerifyPlanTarget(c.Context(), conn, missing, runtime), qt.ErrorMatches,
		`plan was computed without an executable provider, but provider "example\.provider" version "experiment" serves the connection; `+newPlanAdvice)
	c.Assert(provider.reads.Load(), qt.Equals, reads)
	embedded, err := dbschema.ConnectToDatabase(c.Context(), provider.address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(embedded.Close(), qt.IsNil) })
	c.Assert(atlasschema.VerifyPlanTarget(c.Context(), embedded, plan, runtime), qt.ErrorMatches,
		`plan was computed with provider "example\.provider" version "experiment", but no executable provider serves the connection; create and approve a new plan`)
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
	c.Assert(plan.Provider.TargetSHA256, qt.Equals, "dffeecec025fab260fed741b6735304deb0641da4aa59e96f203636b9f4372b3")
}

func TestSavedPlanRefusesMalformedProviderBinding(t *testing.T) {
	c := qt.New(t)
	_, _, plan := prepareBoundPlan(c)
	plan.Provider.TargetSHA256 = "not-a-digest"
	_, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.ErrorMatches, ".*plan provider target requires a lowercase SHA-256 digest.*")
	plan.Provider.TargetSHA256 = strings.Repeat("a", 64)
	plan.Provider.ContractMajor = 0
	_, err = atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.ErrorMatches, "plan provider binding requires a contract major version")
	plan.Provider.ContractMajor = 2
	plan.Provider.Version = " experiment"
	_, err = atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.ErrorMatches, "plan provider binding requires a provider ID and a release version")
}

func TestSavedPlanBindsBothDatabaseInterpretationRoots(t *testing.T) {
	for _, field := range []string{"database", "source database", "incomplete"} {
		t.Run(field, func(t *testing.T) {
			c := qt.New(t)
			selected, _, _ := prepareBoundPlan(c)
			selected.info.DatabaseContext = schemaext.DatabaseContext{Database: "/selected/realm", SourceDatabase: "/authored"}
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			conn := connectBoundProvider(c, selected)
			plan, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{Runtime: runtime, Desired: &schemamodel.Database{}})
			c.Assert(err, qt.IsNil)
			c.Assert(atlasschema.VerifyPlanTarget(c.Context(), conn, plan, runtime), qt.IsNil)
			changed := &boundProvider{base: c.Context(), info: selected.info, address: selected.address}
			switch field {
			case "database":
				changed.info.DatabaseContext.Database = "/selected/other"
			case "source database":
				changed.info.DatabaseContext.SourceDatabase = "/other"
			case "incomplete":
				changed.info.DatabaseContext.SourceDatabase = ""
			}
			err = atlasschema.VerifyPlanTarget(c.Context(), connectBoundProvider(c, changed), plan, runtime)
			c.Assert(err, qt.IsNotNil)
			c.Assert(changed.reads.Load(), qt.Equals, int64(0))
		})
	}
}
