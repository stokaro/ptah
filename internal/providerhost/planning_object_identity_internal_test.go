package providerhost

// White-box testing required: the protocol's absent-part spelling
// and namespace wildcard are private adapter details, not distinct core IDs.
import (
	"crypto/sha256"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestPlanningObjectDatabaseCanonicalizationPreservesOwnersAndScopes(t *testing.T) {
	c := qt.New(t)
	subject := &protocol.ComparisonIdentity{Kind: "fixture/query", Database: &protocol.ComparisonPart{}, Name: &protocol.ComparisonPart{Source: "copy", Normalized: "copy"}}
	owner := &protocol.Reference{Engine: "fixture", Kind: subject.Kind, Database: "/local", Name: "copy"}
	bound := &protocol.DatabaseContext{Database: "/local", SourceDatabase: "/local"}
	request := &protocol.PlanFeaturesRequest{DatabasePath: "/local", ChangedSubjects: []*protocol.ComparisonIdentity{proto.CloneOf(subject)}, CurrentObjects: &protocol.ComparisonState{Records: []*protocol.ComparisonRecord{{Kind: subject.Kind, Subject: proto.CloneOf(subject), Value: &protocol.NormalizedValue{Value: &protocol.Envelope{Owner: proto.CloneOf(owner), Context: bound}, NormalizedOwner: proto.CloneOf(owner)}}}, Coverage: &protocol.ComparisonCoverage{Subjects: []*protocol.ComparisonSubjectCoverage{{Kind: subject.Kind, Subject: proto.CloneOf(subject)}}, Kinds: []*protocol.ComparisonKindCoverage{{Sources: []*protocol.ComparisonKnowledgeSource{{Regions: []*protocol.ComparisonRegion{{Scope: &protocol.ComparisonNamespace{Database: &protocol.ComparisonPart{}}}}}}}}}}}
	coverage := proto.CloneOf(request.CurrentObjects.Coverage.Kinds[0])
	value := proto.CloneOf(request.CurrentObjects.Records[0].Value)
	canonicalizePlanningObjectDatabases(request)
	c.Assert(request.CurrentObjects.Records[0].Subject.Database, qt.IsNil)
	c.Assert(request.CurrentObjects.Coverage.Subjects[0].Subject.Database, qt.IsNil)
	c.Assert(request.ChangedSubjects[0].Database, qt.IsNil)
	c.Assert(request.CurrentObjects.Coverage.Kinds[0], qt.CmpEquals(protocmp.Transform()), coverage)
	c.Assert(request.CurrentObjects.Records[0].Value, qt.CmpEquals(protocmp.Transform()), value)
}

func TestPlanningObjectDatabaseCanonicalizationKeepsExplicitAndUnusualParts(t *testing.T) {
	for _, part := range []*protocol.ComparisonPart{{Source: "/other", Normalized: "/other"}, {Source: "/local", Normalized: "/local"}, {Quoted: true}, {Defaulted: true}, {Source: "/local"}} {
		c := qt.New(t)
		subject := &protocol.ComparisonIdentity{Database: proto.CloneOf(part)}
		request := &protocol.PlanFeaturesRequest{DatabasePath: "/local", DesiredObjects: &protocol.ComparisonState{Records: []*protocol.ComparisonRecord{{Subject: subject}}}}
		canonicalizePlanningObjectDatabases(request)
		c.Assert(subject.Database, qt.CmpEquals(protocmp.Transform()), part)
	}
}

func TestPlanningObjectDatabaseCanonicalizationChecksObservedSessionBinding(t *testing.T) {
	for _, test := range []struct {
		name, session, sourceOwner, normalizedOwner string
		want                                        error
	}{
		{"localized inspection", "/local", "/local", "/local", nil},
		{"foreign inspection", "/local", "/other", "/other", provider.ErrProtocol},
		{"disagreeing owner spellings", "/local", "/local", "/other", provider.ErrProtocol},
		{"missing session binding", "", "/local", "/local", provider.ErrProtocol},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := modelIdentityTarget()
			kind := "example.org/fixture/query"
			definition := []byte(`{"type":"object"}`)
			digest := fmt.Sprintf("%x", sha256.Sum256(definition))
			model := &protocol.ValueModel{Kind: kind, Representation: protocol.ValueInput_DESIRED, SchemaVersion: 1, Encoding: "json", DefinitionJson: definition, DefinitionSha256: digest, CanonicalFormat: "json"}
			observed := proto.CloneOf(model)
			observed.Representation = protocol.ValueInput_OBSERVED
			target.engine.ValueModels = []*protocol.ValueModel{model, observed}
			features := []string{provider.PlanFeaturesFeature, provider.PlanningObjectContextFeature, provider.NormalizeValuesFeature}
			target.engine.Features = features
			target.engine.Planning = []*protocol.ModelPlanning{{Id: "queries", ContextKinds: []string{kind}}}
			target.host.description.Features = features
			target.host.description.Engines = []*protocol.Engine{target.engine}
			source := &protocol.Reference{Engine: "fixture", Kind: kind, Database: test.sourceOwner, Name: "copy"}
			normalized := proto.CloneOf(source)
			normalized.Database = test.normalizedOwner
			subject := &protocol.ComparisonIdentity{Kind: kind, Database: &protocol.ComparisonPart{}, Name: &protocol.ComparisonPart{Source: "copy", Normalized: "copy"}}
			request := &protocol.PlanFeaturesRequest{Engine: "fixture", PlanningId: "queries", ContextKinds: []string{kind}, TargetFeatures: features, DatabasePath: test.session, Identifiers: &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(target.engine.Identifiers)}, DesiredObjects: &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{}}, CurrentObjects: &protocol.ComparisonState{Representation: protocol.ValueInput_OBSERVED, Coverage: &protocol.ComparisonCoverage{}, Records: []*protocol.ComparisonRecord{{Kind: kind, Subject: proto.CloneOf(subject), Value: &protocol.NormalizedValue{Id: "copy", Value: &protocol.Envelope{ProviderId: target.host.description.ProviderId, Kind: kind, SchemaVersion: 1, Encoding: "json", DefinitionSha256: digest, Owner: source, Payload: []byte(`{}`)}, NormalizedOwner: normalized, CanonicalFormat: "json", CanonicalProjection: []byte(`{}`)}}}}, ChangedSubjects: []*protocol.ComparisonIdentity{proto.CloneOf(subject)}}
			c.Assert(provider.ValidatePlanningRequest(target.host.description, request), qt.ErrorIs, provider.ErrProtocol)
			canonicalizePlanningObjectDatabases(request)
			err := provider.ValidatePlanningRequest(target.host.description, request)
			c.Assert(err, qt.ErrorIs, test.want)
		})
	}
}
