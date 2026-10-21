// Package main exercises observed-model transport independently of the host.
// Synthetic retention objects do not claim a SQLite retention capability.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	description := backend.Description()
	description.SemanticIdentity += "-observed-models"
	owner := &observedProvider{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner,
		Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type observedProvider struct{ *backend.Provider }

func (p *observedProvider) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Features = []string{"tables"}
	return session, nil
}

func (p *observedProvider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.Provider.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot.TargetFeatures = []string{"tables"}
	if len(snapshot.Tables) == 0 {
		return snapshot, nil
	}
	table := snapshot.Tables[0]
	description := backend.Description()
	model := description.Engines[0].ValueModels[1]
	snapshot.Coverage.Kinds = []*protocol.ModelKindCoverage{{Model: model, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}
	for _, overload := range [][]string{nil, {"INTEGER, TEXT"}, {"INTEGER", "TEXT"}} {
		ref := &protocol.Reference{Engine: "fixture", Database: table.Identity.Database, Catalog: "catalog", Schema: "private", Path: []string{"a/b", "c"},
			Kind: model.Kind, Name: "Job", Overload: overload}
		snapshot.NativeObjects = append(snapshot.NativeObjects, observedEnvelope(description, model, ref))
	}
	table.Facets = []*protocol.Envelope{observedEnvelope(description, model, table.Identity)}
	column := table.Columns[0]
	column.Facets = []*protocol.Envelope{observedEnvelope(description, model, provider.ColumnReference(table.Identity, column.Name))}
	values := slices.Clone(snapshot.NativeObjects)
	values = append(values, table.Facets...)
	values = append(values, column.Facets...)
	for _, value := range values {
		snapshot.Coverage.Subjects = append(snapshot.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: model.Kind,
			Subject: proto.CloneOf(value.Owner), NormalizedSubject: normalizedOwner(value.Owner), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	}
	absent := proto.CloneOf(snapshot.Coverage.Subjects[0])
	absent.Subject.Name, absent.NormalizedSubject.Name = "Absent", "absent"
	absent.Knowledge.State = protocol.ModelKnowledge_ABSENT
	snapshot.Coverage.Subjects = append(snapshot.Coverage.Subjects, absent)
	if strings.HasPrefix(table.Identity.Name, "scoped-") {
		complete := &protocol.ModelNamespaceKnowledge{Scope: &protocol.ModelNamespace{
			Database:   &protocol.NamespaceComponent{Source: table.Identity.Database, Normalized: table.Identity.Database},
			Catalog:    &protocol.NamespaceComponent{Source: "catalog", Normalized: "catalog"},
			Schema:     &protocol.NamespaceComponent{Source: "Private", Normalized: "private"},
			PathPrefix: []*protocol.NamespaceComponent{{Source: "a/b", Normalized: "a/b"}, {Source: "C", Normalized: "c"}},
		}, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}
		unknown := proto.CloneOf(complete)
		unknown.Scope.PathPrefix = append(unknown.Scope.PathPrefix, &protocol.NamespaceComponent{Source: "Hidden", Normalized: "hidden"})
		unknown.Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "nested namespace was not inspected"}
		snapshot.Coverage.Kinds[0].Regions = []*protocol.ModelNamespaceKnowledge{complete, unknown}
	}
	switch table.Identity.Name {
	case "scoped-foreign-database":
		snapshot.Coverage.Kinds[0].Regions[0].Scope.Database = &protocol.NamespaceComponent{Source: "foreign", Normalized: "foreign"}
	case "scoped-unbound-database":
		snapshot.Coverage.Kinds[0].Regions[0].Scope.Database = nil
	case "scoped-normalized-database":
		snapshot.Coverage.Kinds[0].Regions[0].Scope.Database.Normalized = "foreign"

	case "coverage-mismatch":
		snapshot.Coverage.Subjects[0].NormalizedSubject.Name = "another"
	case "bad-feature":
		snapshot.TargetFeatures = []string{"columns"}
	case "missing-coverage":
		snapshot.Coverage = nil
	case "bad-payload":
		column.Facets[0].Payload = []byte(`{}`)
	case "bad-database":
		column.Facets[0].Owner.Database = "foreign"
	}
	return snapshot, nil
}

func observedEnvelope(description *protocol.Description, model *protocol.ValueModel, owner *protocol.Reference) *protocol.Envelope {
	return &protocol.Envelope{ProviderId: description.ProviderId, Kind: model.Kind, SchemaVersion: model.SchemaVersion,
		Encoding: model.Encoding, DefinitionSha256: model.DefinitionSha256, Owner: proto.CloneOf(owner), Payload: []byte(`{"seconds":9007199254740993}`)}
}

func (p *observedProvider) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	for _, value := range request.GetValues() {
		if value.GetValue().GetOwner().GetName() == "slow-normalize" {
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	result, err := p.Provider.NormalizeValues(ctx, request)
	if err != nil {
		return nil, err
	}
	for _, value := range result.GetValues() {
		value.NormalizedOwner = normalizedOwner(value.Value.Owner)
	}
	return result, nil
}

// Inspection coverage and offline codec decoding share one identity rule.
func normalizedOwner(ref *protocol.Reference) *protocol.Reference {
	result := proto.CloneOf(ref)
	result.Name = strings.ToLower(result.Name)
	if result.Kind == "table" || result.Kind == "column" {
		result.Schema = strings.ToLower(result.Schema)
		for i, part := range result.Path {
			result.Path[i] = strings.ToLower(part)
		}
	}
	for i, part := range result.Overload {
		result.Overload[i] = strings.ToLower(part)
	}
	if result.Kind == "column" && len(result.Path) == 1 && result.Path[0] == "bad-parent" {
		result.Path[0] = "another"
	}
	return result
}

func (p *observedProvider) ConvertValues(ctx context.Context, request *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	return backend.ConvertValues(ctx, request, p)
}
