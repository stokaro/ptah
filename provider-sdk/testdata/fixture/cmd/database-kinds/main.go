// Package main exercises database kinds across the subprocess boundary: the
// engine names its retention model a database kind, and every read reports
// one retention object, quota, named at the database rather than in a
// schema of it, as the YDB provider reports a resource pool. A read filtered
// to the schema app, which holds no table, reports it beside its scope when
// the host lists schema.database-kinds. With FIXTURE_DATABASE_KINDS=silent the
// provider does not list the feature, and a filtered read leaves the object
// uninspected, as a provider of an earlier contract does.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

// appSchema is the schema a filtered read may select besides main. It holds
// no table, so a read of it reports nothing but the database's objects.
const appSchema = "app"

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-database-kinds"
	listed := os.Getenv("FIXTURE_DATABASE_KINDS") != "silent"
	if listed {
		engine := description.Engines[0]
		description.Features = append(description.Features, provider.DatabaseKindsFeature)
		engine.Features = append(engine.Features, provider.DatabaseKindsFeature)
		engine.DatabaseKinds = []string{retentionModel(description).GetKind()}
	}
	owner := &databaseProvider{Provider: &backend.Provider{}, listed: listed}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// retentionModel is the engine's observed retention model.
func retentionModel(description *protocol.Description) *protocol.ValueModel {
	for _, model := range description.GetEngines()[0].GetValueModels() {
		if model.GetRepresentation() == protocol.ValueInput_OBSERVED {
			return model
		}
	}
	return nil
}

type databaseProvider struct {
	*backend.Provider
	listed bool
}

// negotiated reports whether this call may use the feature: the provider
// lists it and the host listed it.
func (p *databaseProvider) negotiated(ctx context.Context) bool {
	return p.listed && slices.Contains(provider.HostFeatures(ctx), provider.DatabaseKindsFeature)
}

func (p *databaseProvider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	if p.negotiated(ctx) {
		session.Features = append(session.Features, provider.DatabaseKindsFeature)
	}
	return session, nil
}

// Inspect reads the base fixture's tables and adds the object quota at the
// database. A read of app binds that schema and lists no table, since the
// base fixture's tables are main's.
func (p *databaseProvider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	filtered := slices.Equal(request.GetSchemas(), []string{appSchema})
	base := proto.CloneOf(request)
	if filtered {
		base.Schemas = nil
	}
	snapshot, err := p.Provider.Inspect(ctx, base)
	if err != nil {
		return nil, err
	}
	// The fixture names a table's database by its file; a database with no
	// table names none, and the read then reports no quota.
	database := ""
	if len(snapshot.GetTables()) != 0 {
		database = snapshot.GetTables()[0].GetIdentity().GetDatabase()
	}
	negotiated := p.negotiated(ctx)
	if negotiated {
		snapshot.TargetFeatures = append(snapshot.TargetFeatures, provider.DatabaseKindsFeature)
	}
	if filtered {
		snapshot.Tables = nil
		snapshot.ScopeBindings = []*protocol.InspectionScopeBinding{{Selector: appSchema, Namespaces: []*protocol.ModelNamespace{{
			Database: &protocol.NamespaceComponent{Source: database, Normalized: database}, Catalog: &protocol.NamespaceComponent{},
			Schema: &protocol.NamespaceComponent{Source: appSchema, Normalized: appSchema},
		}}}}
	}
	if database == "" || filtered && !negotiated {
		return snapshot, nil
	}
	description := backend.Description()
	model := retentionModel(description)
	owner := &protocol.Reference{Engine: "fixture", Database: database, Kind: model.GetKind(), Name: "quota"}
	snapshot.NativeObjects = append(snapshot.NativeObjects, &protocol.Envelope{ProviderId: description.GetProviderId(), Kind: model.GetKind(),
		SchemaVersion: model.GetSchemaVersion(), Encoding: model.GetEncoding(), DefinitionSha256: model.GetDefinitionSha256(), Owner: owner,
		Payload: []byte(`{"seconds":60}`)})
	snapshot.Coverage.Subjects = append(snapshot.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: model.GetKind(), Subject: proto.CloneOf(owner),
		NormalizedSubject: proto.CloneOf(owner), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	snapshot.Coverage.Kinds = append(snapshot.Coverage.Kinds, &protocol.ModelKindCoverage{Model: proto.CloneOf(model),
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "the read knows retention objects in its database alone"},
		Regions: []*protocol.ModelNamespaceKnowledge{{Scope: &protocol.ModelNamespace{Database: &protocol.NamespaceComponent{Source: database, Normalized: database},
			Catalog: &protocol.NamespaceComponent{}, Schema: &protocol.NamespaceComponent{}}, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}})
	return snapshot, nil
}
