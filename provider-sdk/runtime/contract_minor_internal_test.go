package runtime

// White-box testing required: the loopback sends each client call through the
// server adapter without a process, the negotiated state of the adapter is set
// by Describe, and a malformed reply from a peer that is not the SDK server
// must reach the client's own checks.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	minorModel = "example.org/minor/retention"
	// minorDefinition is the SHA-256 of the canonical definition {"type":"object"}.
	minorDefinition = "a2c799262a3ce3c19ef5cdd983bf3d12b43ab3c426227091b909dcb7054738c0"
)

// minorFeatures are the provider features of contract 25.3 and the services
// they extend.
func minorFeatures() []string {
	return []string{provider.RenderFeature, provider.NormalizeValuesFeature, provider.DecodeSourceFeature, provider.CompareModelsFeature,
		provider.CommonObjectsFeature, provider.TextUnitsFeature, provider.PlanningPolicyFeature, provider.EntryKeysFeature,
		provider.HCLSourceFeature, provider.EncodeSourceFeature}
}

// minorDescription offers every addition of contract 25.3 on one engine: views
// and comments on the common wire, text units, the planning policy, a YAML
// index key that inherits the index's type, an HCL block and encoding.
func minorDescription() *protocol.Description {
	model := &protocol.ValueModel{Kind: minorModel, Representation: protocol.ValueInput_DESIRED, SchemaVersion: 1, Encoding: "json",
		DefinitionJson: []byte(`{"type":"object"}`), DefinitionSha256: minorDefinition, CanonicalFormat: "json-v1",
		SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_YAML, protocol.SourceDocument_HCL}}
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/minor", Version: "1", SemanticIdentity: "minor-1", Features: minorFeatures(),
		Engines: []*protocol.Engine{{
			Id: "minor", Features: minorFeatures()[4:], ValueModels: []*protocol.ValueModel{model},
			Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
				IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE},
			CommonObjects: &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_VIEW, protocol.CommonObjectSupport_COMMENT}},
			YamlSourceKeys: []*protocol.YAMLSourceKey{{Scope: protocol.YAMLSourceKey_INDEX, Key: "retention", Selector: "retention",
				Models: []string{minorModel}, Inherited: []string{"type"}}},
			HclSource: &protocol.HCLSourceDeclaration{HeaderKinds: []string{"retention"}, Blocks: []*protocol.HCLSourceBlock{{
				Site: protocol.HCLSourceBlock_TOP_LEVEL, Type: "retention", Naming: protocol.HCLSourceBlock_SCHEMA_AND_NAME, Selector: "retention",
				Models: []string{minorModel}, Attributes: []*protocol.HCLSourceAttribute{{Name: "seconds", Kind: protocol.HCLSourceAttribute_TEXT}}}}},
		}},
	}
}

// minorOwner is an owner of every 25.3 service. A retention value's payload
// is {"seconds":N}, and its YAML fragment is the number N.
type minorOwner struct {
	encoded []*protocol.EncodedFragment
}

func (*minorOwner) NormalizeValues(_ context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	result := &protocol.NormalizedValues{Complete: true}
	for _, input := range request.Values {
		result.Values = append(result.Values, minorValue(input.Id, input.Value.Owner, input.Value.Payload))
	}
	return result, nil
}

func (*minorOwner) CompareModels(context.Context, *protocol.CompareModelsRequest) (*protocol.ComparedModels, error) {
	return nil, status.Error(codes.Unimplemented, "no comparison")
}

// PrepareCommonObjects spells a view's query in capitals, as the engine
// stores it.
func (*minorOwner) PrepareCommonObjects(_ context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	prepared := proto.CloneOf(request.Declared)
	for _, view := range prepared.GetViews() {
		view.Body = strings.ToUpper(view.Body)
	}
	return &protocol.PreparedCommonObjects{Complete: true, Prepared: prepared}, nil
}

// Render writes text for every change, and has no text form for a comment,
// which the engine keeps as an attribute only its native call sets.
func (*minorOwner) Render(_ context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.Changes {
		if request.Form == protocol.RenderRequest_TEXT && change.GetCommonObject().GetSetComment() != nil {
			return &protocol.RenderResult{Refusal: &protocol.RenderRefusal{ChangeId: change.Id, Reason: protocol.RenderRefusal_NO_TEXT_FORM,
				Message: "a comment is set by a native call"}}, nil
		}
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.Id, Body: &protocol.Unit_Sql{Sql: "STATEMENT " + change.Id + ";"}})
	}
	return result, nil
}

// EncodeSource writes each facet as its YAML index key, the seconds as a
// number, and keeps what it wrote for the test to read back.
func (o *minorOwner) EncodeSource(ctx context.Context, request *protocol.EncodeSourceRequest) (*protocol.EncodedSource, error) {
	claim := minorDescription().Engines[0].YamlSourceKeys[0]
	result := &protocol.EncodedSource{Complete: true}
	for _, value := range request.Values {
		var payload struct{ Seconds int }
		if _, err := fmt.Sscanf(string(value.Value.Value.Payload), `{"seconds":%d}`, &payload.Seconds); err != nil {
			return nil, err
		}
		result.Fragments = append(result.Fragments, &protocol.EncodedFragment{ValueIds: []string{value.Value.Id}, Fragment: &protocol.SourceFragment{
			Id: value.Value.Id, Selector: claim.Selector, YamlKey: proto.CloneOf(claim), BindingId: value.BindingId, Subject: proto.CloneOf(value.Value.Value.Owner),
			Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Value: &protocol.SourceValue_Number{Number: fmt.Sprint(payload.Seconds)}}}}})
	}
	o.encoded = result.Fragments
	return result, ctx.Err()
}

// DecodeSource reads each fragment's number back into a facet of its subject.
func (*minorOwner) DecodeSource(_ context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	result := &protocol.DecodedSource{Complete: true}
	for _, document := range request.Documents {
		decoded := &protocol.DecodedSourceDocument{Id: document.Id, Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}}
		for _, model := range document.Models {
			decoded.Coverage.Kinds = append(decoded.Coverage.Kinds, &protocol.ModelKindCoverage{Model: model, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
		}
		for _, fragment := range document.Fragments {
			value := minorValue(fragment.Id, fragment.Subject, []byte(`{"seconds":`+fragment.GetValue().GetNumber()+`}`))
			decoded.Fragments = append(decoded.Fragments, &protocol.DecodedSourceFragment{Id: fragment.Id, Values: []*protocol.SourceModelValue{{
				Placement: protocol.SourceModelValue_FACET, Value: value,
				Source: &protocol.SourceTrace{DocumentId: document.Id, FragmentId: fragment.Id, Root: protocol.SourceTrace_CONTENT}}}})
			decoded.Coverage.Subjects = append(decoded.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: minorModel, Subject: proto.CloneOf(fragment.Subject),
				NormalizedSubject: proto.CloneOf(fragment.Subject), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
		}
		result.Documents = append(result.Documents, decoded)
	}
	return result, nil
}

func minorValue(id string, owner *protocol.Reference, payload []byte) *protocol.NormalizedValue {
	return &protocol.NormalizedValue{Id: id, NormalizedOwner: proto.CloneOf(owner), CanonicalFormat: "json-v1", CanonicalProjection: slices.Clone(payload),
		Value: &protocol.Envelope{ProviderId: "example.org/minor", Kind: minorModel, SchemaVersion: 1, Encoding: "json", DefinitionSha256: minorDefinition,
			Owner: proto.CloneOf(owner), Payload: slices.Clone(payload)}}
}

func minorServer(owner *minorOwner) Server {
	return Server{Description: minorDescription(), Sessions: &sessionOwner{}, Renderer: owner, ValueNormalizer: owner, SourceDecoder: owner,
		ModelComparer: owner, CommonObjects: owner, SourceEncoder: owner}
}

// minorLoopback forwards the calls of contract 25.3 to a server adapter, or
// answers with a canned reply when one is set, as a peer that is not the SDK
// server could.
type minorLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
	canned proto.Message
}

func minorCall[Q, R proto.Message](ctx context.Context, l *minorLoopback, serve func(context.Context, Q) (R, error), request Q) (R, error) {
	l.calls++
	if canned, ok := l.canned.(R); ok {
		return canned, nil
	}
	return serve(ctx, request)
}

func (l *minorLoopback) PrepareCommonObjects(ctx context.Context, request *protocol.PrepareCommonObjectsRequest, _ ...grpc.CallOption) (*protocol.PreparedCommonObjects, error) {
	return minorCall(ctx, l, l.server.PrepareCommonObjects, request)
}

func (l *minorLoopback) Render(ctx context.Context, request *protocol.RenderRequest, _ ...grpc.CallOption) (*protocol.RenderResult, error) {
	return minorCall(ctx, l, l.server.Render, request)
}

func (l *minorLoopback) EncodeSource(ctx context.Context, request *protocol.EncodeSourceRequest, _ ...grpc.CallOption) (*protocol.EncodedSource, error) {
	return minorCall(ctx, l, l.server.EncodeSource, request)
}

func (l *minorLoopback) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest, _ ...grpc.CallOption) (*protocol.DecodedSource, error) {
	return minorCall(ctx, l, l.server.DecodeSource, request)
}

// newMinorLoopback is a client and a server after a host listed hostFeatures.
func newMinorLoopback(owner *minorOwner, hostFeatures ...string) (*Client, *minorLoopback) {
	server := negotiatedServer(minorServer(owner), hostFeatures...)
	peer := &minorLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

func minorView() *protocol.Change {
	return &protocol.Change{Id: "view", Object: &protocol.Reference{Engine: "minor", Kind: "view", Schema: "app", Name: "active"},
		Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateView{
			CreateView: &protocol.ViewCreation{View: &protocol.SQLView{Schema: "app", Name: "active", Body: "select 1"}}}}}}
}

func minorComment() *protocol.Change {
	object := &protocol.Reference{Engine: "minor", Kind: "view", Schema: "app", Name: "active"}
	return &protocol.Change{Id: "comment", Object: object, Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
		Operation: &protocol.CommonObjectChange_SetComment{SetComment: &protocol.ObjectComment{Object: proto.CloneOf(object), Text: "Active"}}}}}
}

// A host that lists every 25.3 feature receives every declaration; a host
// that lists none receives the provider's services without them, and a
// description that is still valid.
func TestContractMinorAdditionsAreTwoWay(t *testing.T) {
	c := qt.New(t)
	server := newServerAdapter(minorServer(&minorOwner{}))
	c.Assert(server.server.Validate(), qt.IsNil)
	listed, err := server.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision, HostContract: provider.Contract(),
		HostFeatures: provider.TwoWayFeatures()})
	c.Assert(err, qt.IsNil)
	c.Assert(listed.Engines[0].Features, qt.DeepEquals, minorFeatures()[4:])
	c.Assert(listed.Engines[0].CommonObjects.GetKinds(), qt.HasLen, 2)
	c.Assert(listed.Engines[0].HclSource.GetBlocks(), qt.HasLen, 1)
	c.Assert(listed.Engines[0].YamlSourceKeys, qt.HasLen, 1)
	unlisted, err := newServerAdapter(minorServer(&minorOwner{})).Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision,
		HostContract: provider.Contract()})
	c.Assert(err, qt.IsNil)
	c.Assert(unlisted.Engines[0].Features, qt.DeepEquals, []string{provider.EncodeSourceFeature})
	c.Assert(unlisted.Engines[0].CommonObjects, qt.IsNil)
	c.Assert(unlisted.Engines[0].HclSource, qt.IsNil)
	c.Assert(unlisted.Engines[0].YamlSourceKeys, qt.HasLen, 0)
	c.Assert(unlisted.Engines[0].ValueModels[0].SourceFormats, qt.DeepEquals, []protocol.SourceDocument_Format{protocol.SourceDocument_HCL})
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
}

// The services and their implementations are advertised together.
func TestContractMinorServicesMatchTheirImplementations(t *testing.T) {
	c := qt.New(t)
	without := minorServer(&minorOwner{})
	without.CommonObjects = nil
	c.Assert(without.Validate(), qt.ErrorMatches, `.*schema\.common-objects advertisement differs from implementation`)
	without = minorServer(&minorOwner{})
	without.SourceEncoder = nil
	c.Assert(without.Validate(), qt.ErrorMatches, `.*source\.encode advertisement differs from implementation`)
	client, _ := newMinorLoopback(nil, provider.TwoWayFeatures()...)
	client.rpc.(*minorLoopback).server.server.CommonObjects = nil
	_, err := client.PrepareCommonObjects(c.Context(), &protocol.PrepareCommonObjectsRequest{Engine: "minor", Identifiers: &protocol.ComparisonIdentifiers{
		Rules: proto.CloneOf(minorDescription().Engines[0].Identifiers)}})
	c.Assert(err, qt.ErrorMatches, `provider prepare common objects failed \(Unimplemented\)`)
	client.rpc.(*minorLoopback).server.server.SourceEncoder = nil
	_, err = client.EncodeSource(c.Context(), minorEncode())
	c.Assert(err, qt.ErrorMatches, `provider encode source failed \(Unimplemented\)`)
}

func minorPrepare() *protocol.PrepareCommonObjectsRequest {
	return &protocol.PrepareCommonObjectsRequest{Engine: "minor", Identifiers: &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(minorDescription().Engines[0].Identifiers)},
		Declared: &protocol.DeclaredCommonObjects{Views: []*protocol.SQLView{{Schema: "app", Name: "active", Body: "select id from users", Comment: "Active"}}}}
}

func TestPrepareCommonObjectsConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	client, peer := newMinorLoopback(&minorOwner{}, provider.CommonObjectsFeature)
	request := minorPrepare()
	before := proto.CloneOf(request)
	prepared, err := client.PrepareCommonObjects(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(prepared.Prepared.Views[0].Body, qt.Equals, "SELECT ID FROM USERS")
	c.Assert(peer.calls, qt.Equals, 1)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
}

func TestPrepareCommonObjectsConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	client, peer := newMinorLoopback(&minorOwner{})
	_, err := client.PrepareCommonObjects(c.Context(), minorPrepare())
	c.Assert(err, qt.ErrorMatches, `.*the wire was not negotiated`)
	c.Assert(peer.calls, qt.Equals, 0)
	_, err = peer.server.PrepareCommonObjects(c.Context(), minorPrepare())
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)

	client, peer = newMinorLoopback(&minorOwner{}, provider.CommonObjectsFeature)
	renamed := proto.CloneOf(minorPrepare().Declared)
	renamed.Views[0].Name = "inactive"
	peer.canned = &protocol.PreparedCommonObjects{Complete: true, Prepared: renamed}
	_, err = client.PrepareCommonObjects(c.Context(), minorPrepare())
	c.Assert(err, qt.ErrorMatches, `.*preparation changed a view's identity`)

	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	peer.canned, peer.calls = nil, 0
	_, err = client.PrepareCommonObjects(canceled, minorPrepare())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(peer.calls, qt.Equals, 0)
}

func TestTextUnitsConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	client, _ := newMinorLoopback(&minorOwner{}, provider.CommonObjectsFeature, provider.TextUnitsFeature)
	text, err := client.Render(c.Context(), &protocol.RenderRequest{Engine: "minor", Form: protocol.RenderRequest_TEXT, Changes: []*protocol.Change{minorView()}})
	c.Assert(err, qt.IsNil)
	c.Assert(text.Units[0].GetSql(), qt.Equals, "STATEMENT view;")
	refused, err := client.Render(c.Context(), &protocol.RenderRequest{Engine: "minor", Form: protocol.RenderRequest_TEXT,
		Changes: []*protocol.Change{minorView(), minorComment()}})
	c.Assert(err, qt.IsNil)
	c.Assert(refused.Complete, qt.IsFalse)
	c.Assert(refused.Units, qt.HasLen, 0)
	c.Assert(refused.Refusal.ChangeId, qt.Equals, "comment")
	c.Assert(refused.Refusal.Reason, qt.Equals, protocol.RenderRefusal_NO_TEXT_FORM)
}

func TestTextUnitsConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	client, peer := newMinorLoopback(&minorOwner{}, provider.CommonObjectsFeature)
	request := &protocol.RenderRequest{Engine: "minor", Form: protocol.RenderRequest_TEXT, Changes: []*protocol.Change{minorView()}}
	_, err := client.Render(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `.*render\.text-units was not negotiated`)
	c.Assert(peer.calls, qt.Equals, 0)
	_, err = peer.server.Render(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)

	client, peer = newMinorLoopback(&minorOwner{}, provider.CommonObjectsFeature, provider.TextUnitsFeature)
	peer.canned = &protocol.RenderResult{Complete: true, Units: []*protocol.Unit{{ChangeId: "view",
		Body: &protocol.Unit_Native{Native: &protocol.Envelope{ProviderId: "example.org/minor", Kind: "view", SchemaVersion: 1, Encoding: "json"}}}}}
	_, err = client.Render(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `.*the TEXT form carries a native unit`)
	request.Form = protocol.RenderRequest_ANY
	refusal := &protocol.RenderRefusal{ChangeId: "view", Reason: protocol.RenderRefusal_NO_TEXT_FORM, Message: "native only"}
	peer.canned = &protocol.RenderResult{Refusal: refusal}
	_, err = client.Render(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `.*incomplete render`)
	peer.canned = &protocol.RenderResult{Complete: true, Refusal: refusal, Units: []*protocol.Unit{{ChangeId: "view", Body: &protocol.Unit_Sql{Sql: "STATEMENT view;"}}}}
	_, err = client.Render(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `.*a schema render refuses only in the TEXT form`)
}

func minorEncode() *protocol.EncodeSourceRequest {
	table := &protocol.Reference{Engine: "minor", Kind: "table", Schema: "app", Name: "users"}
	index := &protocol.Reference{Engine: "minor", Kind: "index", Schema: "app", Path: []string{"users"}, Name: "by_name"}
	return &protocol.EncodeSourceRequest{Engine: "minor", Format: protocol.SourceDocument_YAML,
		Bindings: []*protocol.SourceBinding{{Id: "users", Object: table, EntryKey: "users"}},
		Values:   []*protocol.EncodeSourceValue{{Placement: protocol.SourceModelValue_FACET, BindingId: "users", Value: minorValue("by-name", index, []byte(`{"seconds":60}`))}}}
}

// What a provider encodes decodes to the same value: the host writes the
// fragments with positions, reads them back, and gets the same canonical
// projection.
func TestEncodeSourceConformanceRoundTrips(t *testing.T) {
	c := qt.New(t)
	owner := &minorOwner{}
	client, _ := newMinorLoopback(owner, provider.EntryKeysFeature)
	request := minorEncode()
	encoded, err := client.EncodeSource(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(encoded.Fragments, qt.HasLen, 1)
	span := &protocol.SourceSpan{Start: &protocol.SourcePosition{Line: 1, Column: 1}, End: &protocol.SourcePosition{ByteOffset: 2, Line: 1, Column: 3}}
	written := proto.CloneOf(encoded.Fragments[0].Fragment)
	written.Span, written.GetValue().Span = span, span
	document := &protocol.SourceDocument{Id: "doc", Format: protocol.SourceDocument_YAML, File: "schema.yaml", ByteLength: 2,
		Bindings: request.Bindings, Fragments: []*protocol.SourceFragment{written}, Models: client.Description().Engines[0].ValueModels}
	decoded, err := client.DecodeSource(c.Context(), &protocol.DecodeSourceRequest{Engine: "minor", Documents: []*protocol.SourceDocument{document}})
	c.Assert(err, qt.IsNil)
	value := decoded.Documents[0].Fragments[0].Values[0].Value
	c.Assert(string(value.CanonicalProjection), qt.Equals, string(request.Values[0].Value.CanonicalProjection))
	c.Assert(proto.Equal(value.Value.Owner, request.Values[0].Value.Value.Owner), qt.IsTrue)
}

func TestEncodeSourceConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	// Without source.entry-keys the index key is not negotiated, so the SDK
	// server refuses the owner's reply that writes under it.
	client, peer := newMinorLoopback(&minorOwner{})
	_, err := client.EncodeSource(c.Context(), minorEncode())
	c.Assert(err, qt.ErrorMatches, `provider encode source failed \(Internal\)`)
	c.Assert(peer.calls, qt.Equals, 1)

	client, peer = newMinorLoopback(&minorOwner{}, provider.EntryKeysFeature)
	peer.canned = &protocol.EncodedSource{Complete: true}
	_, err = client.EncodeSource(c.Context(), minorEncode())
	c.Assert(err, qt.ErrorMatches, `.*a value is neither written nor lost`)
	peer.canned = &protocol.EncodedSource{Complete: true, Losses: []*protocol.EncodeLoss{{ValueId: "by-name", Message: "kept"}, {ValueId: "by-name", Message: "twice"}}}
	_, err = client.EncodeSource(c.Context(), minorEncode())
	c.Assert(err, qt.ErrorMatches, `.*a loss names a value of the request once, in one line`)
}
