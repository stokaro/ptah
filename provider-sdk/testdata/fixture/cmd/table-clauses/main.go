// Package main runs the fixture with a SQL table clause of contract 25.29:
// the fixture engine reads its SQL documents with the shared grammar and
// claims the clause of CREATE TABLE that opens with TTL, after the column list,
// which it reads as the table's retention: `TTL <n> SECONDS` or
// `TTL <n> MINUTES`. Any other clause that opens with TTL is refused, with a
// reason that quotes nothing of the clause, which errors.reasons carries to
// the host.
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
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// retentionKind is the fixture's retention model, which a table holds as a
// facet.
const retentionKind = "example.org/fixture/retention"

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, fmt.Appendf(nil, "%d", os.Getpid()), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	owner := &clauses{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description(), Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// claim is the program's claim of the TTL clause.
func claim() *protocol.SQLTableClause {
	return &protocol.SQLTableClause{Words: []string{"TTL"}, Selector: "retention", Models: []string{retentionKind}}
}

// description is the base fixture with SQL among the desired retention
// model's source formats, the claim of the TTL clause and refusal reasons.
func description() *protocol.Description {
	description := backend.Description()
	description.SemanticIdentity += "-table-clauses"
	description.Features = append(description.Features, provider.SQLTableClausesFeature, provider.ErrorReasonsFeature)
	engine := description.GetEngines()[0]
	engine.Features = append(engine.Features, provider.SQLTableClausesFeature)
	for _, model := range engine.GetValueModels() {
		if model.GetKind() == retentionKind && model.GetRepresentation() == protocol.ValueInput_DESIRED {
			model.SourceFormats = append(model.SourceFormats, protocol.SourceDocument_SQL)
		}
	}
	engine.SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_FRAGMENTS, TableClauses: []*protocol.SQLTableClause{claim()}}
	return description
}

type clauses struct{ *backend.Provider }

// DecodeSource reads each TTL clause into its table's retention, and leaves
// every other fragment to the base fixture.
func (p *clauses) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	forwarded := proto.CloneOf(request)
	for _, document := range forwarded.GetDocuments() {
		document.Fragments = slices.DeleteFunc(document.Fragments, func(fragment *protocol.SourceFragment) bool { return fragment.GetSqlTableClause() != nil })
	}
	decoded, err := p.Provider.DecodeSource(ctx, forwarded)
	if err != nil {
		return nil, err
	}
	for i, document := range request.GetDocuments() {
		answered, err := p.answer(ctx, document, decoded.GetDocuments()[i])
		if err != nil {
			return nil, err
		}
		decoded.GetDocuments()[i] = answered
	}
	return decoded, nil
}

// answer joins the base fixture's answer of a document's other fragments with
// the retention each TTL clause declares.
func (p *clauses) answer(ctx context.Context, document *protocol.SourceDocument, base *protocol.DecodedSourceDocument) (*protocol.DecodedSourceDocument, error) {
	answered := &protocol.DecodedSourceDocument{Id: base.GetId(), Coverage: base.GetCoverage()}
	results := base.GetFragments()
	for _, fragment := range document.GetFragments() {
		if fragment.GetSqlTableClause() == nil {
			answered.Fragments, results = append(answered.Fragments, results[0]), results[1:]
			continue
		}
		value, err := p.retention(ctx, document, fragment)
		if err != nil {
			return nil, err
		}
		answered.Fragments = append(answered.Fragments, &protocol.DecodedSourceFragment{Id: fragment.GetId(), Values: []*protocol.SourceModelValue{value}})
		answered.Coverage.Subjects = append(answered.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: retentionKind,
			Subject: proto.CloneOf(value.GetValue().GetValue().GetOwner()), NormalizedSubject: proto.CloneOf(value.GetValue().GetNormalizedOwner()),
			Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	}
	return answered, nil
}

// retention reads `TTL <n> SECONDS` or `TTL <n> MINUTES` into the retention
// of the clause's table, in seconds.
func (p *clauses) retention(ctx context.Context, document *protocol.SourceDocument, fragment *protocol.SourceFragment) (*protocol.SourceModelValue, error) {
	words := strings.Fields(fragment.GetSql())
	units := map[string]uint64{"SECONDS": 1, "MINUTES": 60}
	refusal := runtime.ErrorWithReason(codes.InvalidArgument, "a TTL clause is TTL <n> SECONDS or TTL <n> MINUTES")
	if len(words) != 3 {
		return nil, refusal
	}
	count, err := strconv.ParseUint(words[1], 10, 32)
	unit, known := units[strings.ToUpper(words[2])]
	if err != nil || !known || count == 0 {
		return nil, refusal
	}
	var owner *protocol.Reference
	for _, binding := range document.GetBindings() {
		if binding.GetId() == fragment.GetBindingId() {
			owner = proto.CloneOf(binding.GetObject())
		}
	}
	model := retentionModel()
	input := &protocol.ValueInput{Id: "ttl." + fragment.GetId(), Representation: protocol.ValueInput_DESIRED, Value: &protocol.Envelope{
		ProviderId: description().GetProviderId(), Kind: retentionKind, SchemaVersion: model.GetSchemaVersion(), Encoding: model.GetEncoding(),
		DefinitionSha256: model.GetDefinitionSha256(), Owner: owner, Payload: []byte(`{"seconds":` + strconv.FormatUint(count*unit, 10) + `}`)}}
	normalized, err := p.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: "fixture", Values: []*protocol.ValueInput{input}})
	if err != nil {
		return nil, err
	}
	return &protocol.SourceModelValue{Placement: protocol.SourceModelValue_FACET, Value: normalized.GetValues()[0],
		Source: &protocol.SourceTrace{DocumentId: document.GetId(), FragmentId: fragment.GetId(), Root: protocol.SourceTrace_CONTENT}}, nil
}

// retentionModel is the desired retention model as this program declares it.
func retentionModel() *protocol.ValueModel {
	for _, model := range description().GetEngines()[0].GetValueModels() {
		if model.GetKind() == retentionKind && model.GetRepresentation() == protocol.ValueInput_DESIRED {
			return model
		}
	}
	panic("the fixture declares no desired retention model") // A static description; a missing model is a fixture programming error.
}
