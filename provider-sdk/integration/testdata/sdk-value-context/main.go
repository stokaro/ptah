// Package main exercises context, view facets, and column observations over RPC.
package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-sdk-value-context"
	features := []string{provider.DatabaseContextFeature, provider.ValueContextFeature, provider.CommonObjectsFeature, provider.CommonViewFacetsFeature, provider.ObservedColumnsFeature, provider.PlanningObjectContextFeature}
	description.Features = append(description.Features, provider.PlanFeaturesFeature)
	description.Features = append(description.Features, features...)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, features...)
	engine.Planning = []*protocol.ModelPlanning{{Id: "objects", ContextKinds: []string{engine.ValueModels[0].Kind}}}
	engine.Aliases = append(engine.Aliases, "fixture-context")
	engine.CommonObjects = &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_VIEW}}
	owner := &contextOwner{Provider: &backend.Provider{}, description: description}
	if err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, DatabaseContexts: owner, CommonObjects: owner, FeaturePlanner: owner}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type contextOwner struct {
	*backend.Provider
	description *protocol.Description
	contexts    sync.Map
}

func (*contextOwner) ResolveDatabaseContext(ctx context.Context, request *protocol.DatabaseContextRequest) (*protocol.DatabaseContext, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	u, err := url.Parse(request.Address)
	if err != nil || u.Scheme != "fixture" || u.Host != "" || u.Path == "" {
		return nil, fmt.Errorf("invalid fixture context address")
	}
	path := filepath.FromSlash(u.Path)
	if len(path) > 3 && path[0] == '\\' && path[2] == ':' {
		path = path[1:]
	}
	source := u.Query().Get("source")
	if source == "" {
		source = path
	}
	return &protocol.DatabaseContext{Database: path, SourceDatabase: source}, nil
}

func (p *contextOwner) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	bound, err := p.ResolveDatabaseContext(ctx, &protocol.DatabaseContextRequest{Engine: request.Engine, Address: request.Address})
	if err != nil {
		return nil, err
	}
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Context = bound
	p.contexts.Store(session.Id, proto.CloneOf(bound))
	return session, nil
}

func (p *contextOwner) Close(ctx context.Context, request *protocol.CloseRequest) error {
	p.contexts.Delete(request.SessionId)
	return p.Provider.Close(ctx, request)
}

func (p *contextOwner) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	result, err := p.Provider.NormalizeValues(ctx, request)
	if err != nil {
		return nil, err
	}
	for _, value := range result.Values {
		if value.Value.Context == nil {
			value.RequiresDatabaseContext = true
			continue
		}
		bound := value.Value.Context
		// The fixture's opaque payload stays unchanged. Its owner resolves one
		// absolute prerequisite against the authored root into the selected realm.
		if !strings.HasPrefix(bound.SourceDatabase, "/") {
			return nil, fmt.Errorf("fixture source root is not absolute")
		}
		value.RequiredObjects = []*protocol.ComparisonIdentity{{Kind: "table", Database: &protocol.ComparisonPart{Source: bound.Database, Normalized: bound.Database}, Name: &protocol.ComparisonPart{Source: "dependency", Normalized: "dependency"}}}
	}
	return result, nil
}

func (p *contextOwner) ConvertValues(ctx context.Context, request *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	return backend.ConvertValues(ctx, request, p)
}

func (*contextOwner) PrepareCommonObjects(ctx context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &protocol.PreparedCommonObjects{Complete: true, Prepared: proto.CloneOf(request.Declared)}, nil
}

func (*contextOwner) PlanFeatures(ctx context.Context, request *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(request.ChangedSubjects) != 1 || len(request.CurrentObjects.Records) != 1 || len(request.DesiredObjects.Records) != 1 {
		return nil, fmt.Errorf("fixture requires complete native planning context")
	}
	result := &protocol.PlannedFeatures{Complete: true}
	for _, subject := range provider.PlanningObjectSubjects(request) {
		result.Objects = append(result.Objects, &protocol.PlanningReceipt{Subject: subject, Kind: subject.Kind, Strategy: "No separate work for fixture mutation"})
	}
	return result, nil
}
