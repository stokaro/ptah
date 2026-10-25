// Package main declares the scope selections of contract 25.4 for two native
// kinds on the fixture's SQLite: a queue, named in a schema, which a TOPIC
// grant names, and a pool, named across the database. A queue whose name
// ends in "-<pool>" runs on that pool and requires it, and a scope that keeps
// the queue keeps the pool. The kinds carry retention values and claim no
// SQLite capability. A table named "misfit" makes the read report a queue
// qualified by its database, which the queue's naming cannot read.
//
// FIXTURE_SECOND_ENGINE adds an engine beside the first: "same" selects the
// two kinds alike, "selector" names the queue by another selector type,
// "rules" compares names exactly, and "grant" names a third kind, a stream,
// by the queue's grant object type.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

const (
	retentionKind = "example.org/fixture/retention"
	queueKind     = "example.org/fixture/queue"
	poolKind      = "example.org/fixture/pool"
	streamKind    = "example.org/fixture/stream"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	description := selectionDescription()
	owner := &selector{Provider: &backend.Provider{}, description: description}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner,
		Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type selector struct {
	*backend.Provider
	description *protocol.Description
}

func selectionDescription() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity += "-scope-selections"
	description.Features = append(description.Features, provider.ScopeSelectionsFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.ScopeSelectionsFeature)
	retention := engine.GetValueModels()
	for _, kind := range []string{queueKind, poolKind} {
		for _, model := range retention {
			native := proto.CloneOf(model)
			native.Kind, native.SourceFormats, native.DisplayName = kind, nil, strings.TrimPrefix(kind, "example.org/fixture/")+"s"
			engine.ValueModels = append(engine.ValueModels, native)
		}
	}
	engine.ScopeSelections = []*protocol.ScopeSelection{
		{Kind: queueKind, SelectorType: "queue", Naming: protocol.ScopeSelection_IN_SCHEMA, GrantObjectType: "TOPIC", KeepsRequired: []string{poolKind}},
		{Kind: poolKind, SelectorType: "pool", Naming: protocol.ScopeSelection_DATABASE_WIDE},
	}
	variant := os.Getenv("FIXTURE_SECOND_ENGINE")
	if variant == "" {
		return description
	}
	other := proto.CloneOf(engine)
	other.Id = "other-fixture"
	other.YamlSourceKeys, other.GoSourceDirectives = nil, nil // a key and a directive have one owner
	for _, model := range other.GetValueModels() {
		model.SourceFormats = nil
	}
	switch variant {
	case "same":
	case "selector":
		other.ScopeSelections[0].SelectorType = "line"
	case "rules":
		other.Identifiers.TableNames = protocol.IdentifierRules_EXACT
	case "grant":
		for _, model := range retention {
			stream := proto.CloneOf(model)
			stream.Kind, stream.SourceFormats = streamKind, nil
			other.ValueModels = append(other.ValueModels, stream)
		}
		other.ScopeSelections = append(other.ScopeSelections[1:], &protocol.ScopeSelection{Kind: streamKind, SelectorType: "stream",
			Naming: protocol.ScopeSelection_IN_SCHEMA, GrantObjectType: "TOPIC"})
	default:
		panic("unknown second engine " + variant)
	}
	description.Engines = append(description.Engines, other)
	return description
}

// Inspect reads the tables and reports two queues and two pools beside them.
func (s *selector) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := s.Provider.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot.Coverage = &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED}
	natives := []*protocol.Reference{
		{Engine: "fixture", Kind: queueKind, Schema: "main", Name: "jobs-fast"},
		{Engine: "fixture", Kind: queueKind, Schema: "main", Name: "mail"},
		{Engine: "fixture", Kind: poolKind, Name: "fast"},
		{Engine: "fixture", Kind: poolKind, Name: "slow"},
	}
	for _, table := range snapshot.GetTables() {
		if table.GetIdentity().GetName() == "misfit" {
			natives[0].Database = table.GetIdentity().GetDatabase()
		}
	}
	for _, model := range s.description.Engines[0].GetValueModels() {
		if model.GetRepresentation() == protocol.ValueInput_OBSERVED && model.GetKind() != retentionKind {
			snapshot.Coverage.Kinds = append(snapshot.Coverage.Kinds, &protocol.ModelKindCoverage{Model: model, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
		}
	}
	for _, ref := range natives {
		model := observedModel(s.description, ref.GetKind())
		snapshot.NativeObjects = append(snapshot.NativeObjects, &protocol.Envelope{ProviderId: s.description.GetProviderId(), Kind: model.GetKind(),
			SchemaVersion: model.GetSchemaVersion(), Encoding: model.GetEncoding(), DefinitionSha256: model.GetDefinitionSha256(), Owner: ref, Payload: []byte(`{"seconds":60}`)})
		snapshot.Coverage.Subjects = append(snapshot.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: ref.GetKind(), Subject: proto.CloneOf(ref),
			NormalizedSubject: normalizedOwner(ref), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	}
	return snapshot, nil
}

func observedModel(description *protocol.Description, kind string) *protocol.ValueModel {
	for _, model := range description.Engines[0].GetValueModels() {
		if model.GetKind() == kind && model.GetRepresentation() == protocol.ValueInput_OBSERVED {
			return model
		}
	}
	panic("the fixture declares no observed model of " + kind)
}

// NormalizeValues normalizes a queue or a pool as the retention value it
// carries, and lists the pool a queue runs on as an object it requires.
func (s *selector) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	result := &protocol.NormalizedValues{Complete: true}
	for _, input := range request.GetValues() {
		kind := input.GetValue().GetKind()
		captured := proto.CloneOf(input)
		if kind == queueKind || kind == poolKind {
			captured.Value.Kind = retentionKind
		}
		values, err := s.Provider.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(),
			Values: []*protocol.ValueInput{captured}})
		if err != nil {
			return nil, err
		}
		value := values.Values[0]
		value.Value.Kind = kind
		if kind == queueKind || kind == poolKind {
			value.NormalizedOwner = normalizedOwner(value.GetValue().GetOwner())
		}
		if pool, runs := routedPool(value.GetValue().GetOwner()); runs {
			value.RequiredObjects = []*protocol.ComparisonIdentity{{Kind: poolKind, Name: &protocol.ComparisonPart{Source: pool, Normalized: strings.ToLower(pool)}}}
		}
		result.Values = append(result.Values, value)
	}
	return result, nil
}

// ConvertValues converts the retention values the fixture declares.
func (s *selector) ConvertValues(ctx context.Context, request *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	return backend.ConvertValues(ctx, request, s)
}

// routedPool is the pool a queue's name ends in, after its last dash.
func routedPool(owner *protocol.Reference) (string, bool) {
	if owner.GetKind() != queueKind {
		return "", false
	}
	dash := strings.LastIndex(owner.GetName(), "-")
	if dash < 0 {
		return "", false
	}
	pool := owner.GetName()[dash+1:]
	return pool, pool != ""
}

// normalizedOwner compares a queue's schema and name and a pool's name
// without regard to ASCII case, as the fixture's table names compare.
func normalizedOwner(ref *protocol.Reference) *protocol.Reference {
	result := proto.CloneOf(ref)
	result.Schema, result.Name = strings.ToLower(ref.GetSchema()), strings.ToLower(ref.GetName())
	return result
}
