// Package main exercises host database interpretation and durable value binding.
package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	description := backend.SQLDocumentDescription()
	description.SemanticIdentity += "-value-context"
	features := []string{provider.DatabaseContextFeature, provider.ValueContextFeature}
	description.Features = append(description.Features, features...)
	description.Engines[0].Features = append(description.Engines[0].Features, features...)
	description.Engines[0].Aliases = append(description.Engines[0].Aliases, "fixture-context")
	owner := &contextOwner{Provider: &backend.Provider{}}
	if err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, DatabaseContexts: owner, SQLSchemaReader: owner}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type contextOwner struct{ *backend.Provider }

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
	return session, nil
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
		if !strings.HasPrefix(bound.SourceDatabase, "/") {
			return nil, fmt.Errorf("fixture source root is not absolute")
		}
		value.RequiredObjects = []*protocol.ComparisonIdentity{{Kind: "table", Database: &protocol.ComparisonPart{Source: bound.Database, Normalized: bound.Database}, Name: &protocol.ComparisonPart{Source: "dependency", Normalized: "dependency"}}}
	}
	return result, nil
}

func (p *contextOwner) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	result, err := p.Provider.DecodeSource(ctx, request)
	if err != nil {
		return nil, err
	}
	for _, document := range result.Documents {
		for _, fragment := range document.Fragments {
			for _, value := range fragment.Values {
				value.Value.RequiresDatabaseContext = true
			}
		}
	}
	return result, nil
}

func (p *contextOwner) ConvertValues(ctx context.Context, request *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	return backend.ConvertValues(ctx, request, p)
}

func (p *contextOwner) ReadSQLSchema(ctx context.Context, request *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	if request.GetDatabasePath() != "/authored" {
		return nil, fmt.Errorf("fixture SQL source did not receive its authored root")
	}
	return p.Provider.ReadSQLSchema(ctx, request)
}
