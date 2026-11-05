// Package main counts the objects a comparison compares, for the host's drift
// inventory. A SQL schema document declares retention objects, and the database
// holds the ones FIXTURE_HELD_RETENTIONS lists as name=seconds pairs. A second
// service compares the retention facet of a table, whose records name the tables
// it stands for. The retention objects are synthetic and no statement changes
// them.
//
// FIXTURE_UNCOUNTED makes the provider one that predates the counts: it offers
// no models.compare-counts and compares the same objects.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

const (
	retentionKind   = "example.org/fixture/retention"
	changeKind      = "example.org/fixture/retention-change"
	facetChangeKind = "example.org/fixture/retention-facet-change"
)

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, fmt.Appendf(nil, "%d", os.Getpid()), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	description := countsDescription()
	owner := &counter{Provider: &backend.Provider{}, description: description}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, SQLSchemaReader: owner, SchemaValidator: owner, TablePreparer: owner, ModelComparer: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type counter struct {
	*backend.Provider
	description *protocol.Description
}

func countsDescription() *protocol.Description {
	description := backend.SQLDocumentDescription()
	description.SemanticIdentity += "-comparison-counts"
	// A schema that declares native objects needs a validator of its own, since
	// the host cannot render one to validate it, and that takes the table
	// preparation the host's common table identity would otherwise give it.
	description.Features = append(description.Features, provider.CompareModelsFeature, provider.ValidateSchemaFeature, provider.PrepareTablesFeature)
	engine := description.Engines[0]
	engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == provider.CommonTableIdentityFeature })
	engine.Features = append(engine.Features, provider.ValidateSchemaFeature, provider.PrepareTablesFeature)
	if os.Getenv("FIXTURE_UNCOUNTED") == "" {
		description.Features = append(description.Features, provider.ComparisonCountsFeature)
		engine.Features = append(engine.Features, provider.ComparisonCountsFeature)
	}
	definition, err := modeljson.Canonical([]byte(`{"type":"object"}`))
	if err != nil {
		panic(err)
	} // Invalid static JSON is a fixture programming error.
	digest := sha256.Sum256(definition)
	for _, kind := range []string{changeKind, facetChangeKind} {
		engine.ValueModels = append(engine.ValueModels, &protocol.ValueModel{Kind: kind, Representation: protocol.ValueInput_CHANGE, SchemaVersion: 1,
			Encoding: "json", DefinitionJson: definition, DefinitionSha256: hex.EncodeToString(digest[:]), CanonicalFormat: "retention-change-v1"})
	}
	engine.Comparisons = []*protocol.ModelComparison{
		{Id: "objects", Placement: protocol.ModelComparison_OBJECTS, Kinds: []string{backend.RetentionObjectKind}, ChangeKinds: []string{changeKind}},
		{Id: "facets", Placement: protocol.ModelComparison_FACETS, Kinds: []string{retentionKind}, ChangeKinds: []string{facetChangeKind}, OwnerKinds: []string{"table"}},
	}
	return description
}

// ValidateSchema accepts every schema: nothing this fixture declares is refused.
func (*counter) ValidateSchema(ctx context.Context, _ *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	return &protocol.SchemaValidation{Complete: true}, ctx.Err()
}

// targetFeatures is the engine's feature list as the host holds it, the
// profile a session and a read report so that observed and declared values
// compare.
func (p *counter) targetFeatures(ctx context.Context) []string {
	return provider.NegotiatedDescription(p.description, provider.HostFeatures(ctx)).GetEngines()[0].GetFeatures()
}

// Open opens the fixture's session under the engine's profile.
func (p *counter) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Features = p.targetFeatures(ctx)
	return session, nil
}

// PrepareTables keeps the tables as declared.
func (*counter) PrepareTables(ctx context.Context, request *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error) {
	result := &protocol.PreparedTables{Complete: true}
	for _, input := range request.GetTables() {
		result.Tables = append(result.Tables, &protocol.PreparedTable{Subject: proto.CloneOf(input.GetSubject()), ColumnPrimaryKeysPrepared: true,
			ColumnPrimaryKeys: slices.Clone(input.GetDesired().GetPrimaryKey())})
	}
	return result, ctx.Err()
}

// Inspect reads the tables and reports the retention objects the environment
// names. A read of the session's schema knows the kind there and nowhere else,
// and an object it reports belongs to the session's database.
func (p *counter) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	snapshot, err := p.Provider.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	model := p.observedModel()
	kind := &protocol.ModelKindCoverage{Model: model, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}
	database := ""
	for _, binding := range snapshot.GetScopeBindings() {
		kind.Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "outside the requested schemas"}
		for _, namespace := range binding.GetNamespaces() {
			kind.Regions = append(kind.Regions, &protocol.ModelNamespaceKnowledge{Scope: proto.CloneOf(namespace), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
			database = namespace.GetDatabase().GetNormalized()
		}
	}
	snapshot.Coverage = &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED, Kinds: []*protocol.ModelKindCoverage{kind}}
	snapshot.TargetFeatures = p.targetFeatures(ctx)
	for held := range strings.SplitSeq(os.Getenv("FIXTURE_HELD_RETENTIONS"), ",") {
		name, seconds, found := strings.Cut(held, "=")
		if !found {
			continue
		}
		owner := &protocol.Reference{Engine: "fixture", Kind: backend.RetentionObjectKind, Database: database, Name: name}
		snapshot.NativeObjects = append(snapshot.NativeObjects, &protocol.Envelope{ProviderId: p.description.GetProviderId(), Kind: model.GetKind(), SchemaVersion: model.GetSchemaVersion(),
			Encoding: model.GetEncoding(), DefinitionSha256: model.GetDefinitionSha256(), Owner: owner, Payload: fmt.Appendf(nil, `{"seconds":%s}`, seconds)})
		snapshot.Coverage.Subjects = append(snapshot.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: owner.GetKind(), Subject: proto.CloneOf(owner),
			NormalizedSubject: proto.CloneOf(owner), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	}
	return snapshot, nil
}

func (p *counter) observedModel() *protocol.ValueModel {
	for _, model := range p.description.GetEngines()[0].GetValueModels() {
		if model.GetKind() == backend.RetentionObjectKind && model.GetRepresentation() == protocol.ValueInput_OBSERVED {
			return model
		}
	}
	panic("the fixture declares no observed retention object")
}

// CompareModels pairs the records of both sides by name. It counts an object
// both sides hold as matched, whether or not it differs, one only the desired
// side holds as added and one only the database holds as removed. An excluded
// object is compared and counted nowhere, as is one whose desired record carries
// no value, which the run bound to another target. It counts only for a request
// whose target features list the feature, which the SDK requires of a reply.
func (p *counter) CompareModels(ctx context.Context, request *protocol.CompareModelsRequest) (*protocol.ComparedModels, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kind := changeKind
	if request.GetComparisonId() == "facets" {
		kind = facetChangeKind
	}
	result := &protocol.ComparedModels{Complete: true, Desired: proto.CloneOf(request.GetDesired())}
	count := &protocol.ComparisonCount{Kind: kind}
	for _, desired := range request.GetDesired().GetRecords() {
		if excluded(request, desired) || desired.GetValue() == nil {
			continue
		}
		current := findRecord(request.GetCurrent(), desired)
		if current == nil {
			count.Added++
			if err := p.addChange(result, kind, desired, nil, desired); err != nil {
				return nil, err
			}
			continue
		}
		count.Matched++
		if !bytes.Equal(desired.GetValue().GetCanonicalProjection(), current.GetValue().GetCanonicalProjection()) {
			if err := p.addChange(result, kind, desired, current, desired); err != nil {
				return nil, err
			}
		}
	}
	for _, current := range request.GetCurrent().GetRecords() {
		if excluded(request, current) || findRecord(request.GetDesired(), current) != nil {
			continue
		}
		count.Removed++
		if err := p.addChange(result, kind, current, current, nil); err != nil {
			return nil, err
		}
	}
	if slices.Contains(request.GetTargetFeatures(), provider.ComparisonCountsFeature) {
		result.Counts = []*protocol.ComparisonCount{count}
	}
	return result, nil
}

func excluded(request *protocol.CompareModelsRequest, record *protocol.ComparisonRecord) bool {
	return slices.ContainsFunc(request.GetExclusions(), func(exclusion *protocol.ComparisonExclusion) bool {
		return exclusion.GetKind() == record.GetKind() && exclusion.GetName() == record.GetSubject().GetName().GetSource()
	})
}

// findRecord pairs two records by name: a read names the session's database in
// every owner, and a declaration names none.
func findRecord(state *protocol.ComparisonState, subject *protocol.ComparisonRecord) *protocol.ComparisonRecord {
	for _, record := range state.GetRecords() {
		if record.GetKind() == subject.GetKind() && record.GetSubject().GetName().GetNormalized() == subject.GetSubject().GetName().GetNormalized() {
			return record
		}
	}
	return nil
}

// retentionChange names the seconds of each side, nil on a side that holds no
// such object.
type retentionChange struct {
	Before *uint64 `json:"before"`
	After  *uint64 `json:"after"`
}

func (p *counter) addChange(result *protocol.ComparedModels, kind string, subject, before, after *protocol.ComparisonRecord) error {
	first, err := seconds(before)
	if err != nil {
		return err
	}
	second, err := seconds(after)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(retentionChange{Before: first, After: second})
	if err != nil {
		return err
	}
	value := proto.CloneOf(subject.GetValue().GetValue())
	value.Kind, value.Payload, value.DefinitionSha256 = kind, payload, p.changeDigest()
	normalized, err := normalizeChange(&protocol.ValueInput{Id: "change-" + strconv.Itoa(len(result.GetChanges())), Representation: protocol.ValueInput_CHANGE, Value: value})
	if err != nil {
		return err
	}
	result.Changes = append(result.Changes, &protocol.ComparedChange{Kind: subject.GetKind(), Subject: proto.CloneOf(subject.GetSubject()), Value: normalized})
	return nil
}

func (p *counter) changeDigest() string {
	for _, model := range p.description.GetEngines()[0].GetValueModels() {
		if model.GetKind() == changeKind {
			return model.GetDefinitionSha256()
		}
	}
	panic("the fixture declares no change model")
}

func seconds(record *protocol.ComparisonRecord) (*uint64, error) {
	if record == nil {
		return nil, nil
	}
	var value struct {
		Seconds uint64 `json:"seconds"`
	}
	if err := json.Unmarshal(record.GetValue().GetValue().GetPayload(), &value); err != nil {
		return nil, err
	}
	return &value.Seconds, nil
}

func normalizeChange(input *protocol.ValueInput) (*protocol.NormalizedValue, error) {
	canonical, err := modeljson.Canonical(input.GetValue().GetPayload())
	if err != nil {
		return nil, err
	}
	var change retentionChange
	if err := json.Unmarshal(canonical, &change); err != nil {
		return nil, err
	}
	value := proto.CloneOf(input.GetValue())
	value.Payload = canonical
	return &protocol.NormalizedValue{Id: input.GetId(), Value: value, NormalizedOwner: proto.CloneOf(value.GetOwner()), CanonicalFormat: "retention-change-v1",
		CanonicalProjection: bytes.Clone(canonical),
		ChangeSemantics: &protocol.ChangeSemantics{ReplacesOwner: new(false), RemovesState: new(change.After == nil), Impact: protocol.ChangeSemantics_BEHAVIORAL,
			Reason: "Retention affects stored data", Access: protocol.ChangeSemantics_UNCHANGED, AccessReason: "Retention does not change grants"}}, nil
}

// NormalizeValues reads a retention object as the retention value it holds and
// keeps the object's own model kind, and takes a change as the comparison
// wrote it.
func (p *counter) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	result := &protocol.NormalizedValues{Complete: true}
	for _, input := range request.GetValues() {
		if input.GetRepresentation() == protocol.ValueInput_CHANGE {
			value, err := normalizeChange(input)
			if err != nil {
				return nil, err
			}
			result.Values = append(result.Values, value)
			continue
		}
		captured := proto.CloneOf(input)
		captured.Value.Kind = retentionKind
		values, err := p.Provider.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(), Values: []*protocol.ValueInput{captured}})
		if err != nil {
			return nil, err
		}
		values.GetValues()[0].Value.Kind = input.GetValue().GetKind()
		result.Values = append(result.Values, values.GetValues()[0])
	}
	return result, nil
}

// ConvertValues converts the retention values the fixture declares.
func (p *counter) ConvertValues(ctx context.Context, request *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	return backend.ConvertValues(ctx, request, p)
}
