package provider

// White-box testing required: a validation that checks an engine's value
// models once per request answers exactly as one that checks them again for
// every table, so only the answers the validation holds show how often it
// asked.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

const checkedKind = "example.org/fixture/retention"

// checkedDescription offers one engine with one desired model.
func checkedDescription() *protocol.Description {
	return &protocol.Description{Revision: Revision, ProviderId: "example.org/fixture", Version: "1", SemanticIdentity: "table-1",
		Features: []string{NormalizeValuesFeature},
		Engines: []*protocol.Engine{{Id: "fixture", Features: []string{"retention"}, ValueModels: []*protocol.ValueModel{{
			Kind: checkedKind, Representation: protocol.ValueInput_DESIRED, SchemaVersion: 1, Encoding: "json", DefinitionJson: []byte(`{"type":"object"}`),
			DefinitionSha256: "a2c799262a3ce3c19ef5cdd983bf3d12b43ab3c426227091b909dcb7054738c0", CanonicalFormat: "seconds-json-v1",
		}}}},
	}
}

// checkedValue is a normalized desired value of the engine's model, owned by
// table name.
func checkedValue(description *protocol.Description, name string) *protocol.NormalizedValue {
	owner := &protocol.Reference{Engine: "fixture", Kind: "table", Name: name}
	return &protocol.NormalizedValue{Id: name, Value: &protocol.Envelope{
		DefinitionSha256: description.Engines[0].ValueModels[0].DefinitionSha256, ProviderId: description.ProviderId, Kind: checkedKind,
		SchemaVersion: 1, Encoding: "json", Payload: []byte(`{"seconds":60}`), Owner: owner,
	}, NormalizedOwner: proto.CloneOf(owner), CanonicalFormat: "seconds-json-v1", CanonicalProjection: []byte(`{"seconds":60}`)}
}

// A validation asks for each target once and keeps the answer; a validation
// that holds no answers asks every time.
func TestTargetChecksAskOncePerQuestion(t *testing.T) {
	c := qt.New(t)
	description := checkedDescription()
	checks := new(targetChecks)

	engine, _, err := checks.valueTarget(description, "fixture", []string{"retention"})
	c.Assert(err, qt.IsNil)
	again, _, err := checks.valueTarget(description, "fixture", []string{"retention"})
	c.Assert(err, qt.IsNil)
	_, features, err := checks.valueTarget(description, "fixture", nil)
	c.Assert(err, qt.IsNil)
	_, _, err = checks.valueTarget(description, "other", nil)
	c.Assert(err, qt.ErrorIs, ErrProtocol)
	var none *targetChecks
	unheld, _, err := none.valueTarget(description, "fixture", []string{"retention"})

	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.Equals, engine)
	c.Assert(unheld, qt.Equals, engine)
	c.Assert(features, qt.HasLen, 0)
	c.Assert(checks.answers, qt.HasLen, 3)
}

// The captured features of several tables, checked under one validation, ask
// for their target once and compare the model their coverage names, one
// message as a request holds it, with the declared one once.
func TestCapturedFeaturesShareTheAnswersOfTheirValidation(t *testing.T) {
	c := qt.New(t)
	description := checkedDescription()
	request := &protocol.PlanFeaturesRequest{Engine: "fixture", TargetFeatures: []string{"retention"}}
	checks := new(targetChecks)
	model := proto.CloneOf(description.Engines[0].ValueModels[0])

	for _, name := range []string{"a", "b", "c"} {
		captured := &protocol.CapturedFeatures{
			Objects: &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{}},
			Facets: &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{
				Kinds: []*protocol.ComparisonKindCoverage{{Model: model}},
			}, Records: []*protocol.ComparisonRecord{{
				Kind: checkedKind, Subject: &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: name, Normalized: name}},
				Value: checkedValue(description, name),
			}}},
		}
		c.Assert(checkCapturedFeatures(checks, description, request, description.Engines[0], protocol.ValueInput_DESIRED, captured), qt.IsNil)
	}

	c.Assert(checks.answers, qt.HasLen, 1)
	c.Assert(checks.same, qt.HasLen, 1)
}

// A walk for unresolved contexts enters the messages that can hold a
// normalized value and skips the rest: a value model, which a request's
// coverage repeats for every table, holds none.
func TestOnlyHoldersOfNormalizedValuesAreWalked(t *testing.T) {
	c := qt.New(t)
	for _, holder := range []proto.Message{&protocol.NormalizedValue{}, &protocol.PlanFeaturesRequest{}, &protocol.ComparisonState{}, &protocol.CapturedFeatures{}} {
		c.Assert(holdsNormalizedValues(holder.ProtoReflect().Descriptor()), qt.IsTrue, qt.Commentf("%T", holder))
	}
	for _, other := range []proto.Message{&protocol.ValueModel{}, &protocol.ComparisonCoverage{}, &protocol.ComparisonIdentity{}, &protocol.Envelope{}} {
		c.Assert(holdsNormalizedValues(other.ProtoReflect().Descriptor()), qt.IsFalse, qt.Commentf("%T", other))
	}
}

// A pair of models compared once is not compared again within a validation,
// so the answer the validation holds decides; without one, every comparison
// reads the models.
func TestCoverageModelsAreComparedOncePerValidation(t *testing.T) {
	c := qt.New(t)
	declared := checkedDescription().Engines[0].ValueModels[0]
	model := proto.CloneOf(declared)
	checks := new(targetChecks)

	first := checks.sameModel(model, declared)
	model.CanonicalFormat = "changed after the comparison"
	again := checks.sameModel(model, declared)
	var none *targetChecks
	unheld := none.sameModel(model, declared)

	c.Assert(first, qt.IsTrue)
	c.Assert(again, qt.IsTrue)
	c.Assert(unheld, qt.IsFalse)
}

// The values of one encoding request, checked under one validation, ask for
// their target once.
func TestEncodedValuesShareTheAnswersOfTheirValidation(t *testing.T) {
	c := qt.New(t)
	description := checkedDescription()
	request := &protocol.EncodeSourceRequest{Engine: "fixture", TargetFeatures: []string{"retention"}}
	bindings := make(map[string]*protocol.SourceBinding)
	checks := new(targetChecks)

	for _, name := range []string{"a", "b", "c"} {
		value := checkedValue(description, name)
		bindings[name] = &protocol.SourceBinding{Id: name, Object: proto.CloneOf(value.Value.Owner)}
		encoded := &protocol.EncodeSourceValue{Value: value, Placement: protocol.SourceModelValue_FACET, BindingId: name}
		c.Assert(validateEncodeValue(checks, description, request, bindings, encoded), qt.IsNil)
	}

	c.Assert(checks.answers, qt.HasLen, 1)
}

// Two coverage subjects have one key exactly when they have one kind and
// compareReferences finds their subjects equal, whatever separators their
// fields hold; the index finds the first record of a key.
func TestCoverageSubjectKeysMatchCompareReferences(t *testing.T) {
	c := qt.New(t)
	refs := []*protocol.Reference{
		{Engine: "e", Kind: "table", Name: "t"},
		{Engine: "e", Kind: "table", Name: "t", Schema: ""},
		{Engine: "e", Kind: "table", Name: "1:t"},
		{Engine: "e", Kind: "table|1:", Name: "t"},
		{Engine: "e", Kind: "table", Schema: "s", Name: "t"},
		{Engine: "e", Kind: "table", Schema: "s|1:t", Name: ""},
		{Engine: "e", Kind: "table", Path: []string{"a", "b"}, Name: "t"},
		{Engine: "e", Kind: "table", Path: []string{"a|1:b"}, Name: "t"},
		{Engine: "e", Kind: "table", Path: []string{"a"}, Name: "b", Overload: []string{"t"}},
		{Engine: "e", Kind: "table", Path: make([]string, 0), Name: "t", Overload: make([]string, 0)},
		{Engine: "e", Database: "d", Catalog: "c", Kind: "table", Name: "t"},
		{Engine: "e", Database: "dc", Kind: "table", Name: "t"},
		{Engine: "e", Catalog: "c|:", Kind: "table", Name: "t"},
		{Engine: "e", Catalog: "c", Schema: "|:", Kind: "table", Name: "t"},
	}
	keys, compared := make([]bool, 0, len(refs)*len(refs)), make([]bool, 0, len(refs)*len(refs))
	for _, left := range refs {
		for _, right := range refs {
			keys = append(keys, subjectKey("k", left) == subjectKey("k", right))
			compared = append(compared, compareReferences(left, right) == 0)
		}
	}
	c.Assert(keys, qt.DeepEquals, compared)
	c.Assert(subjectKey("k", refs[0]), qt.Not(qt.Equals), subjectKey("other", refs[0]))

	first := &protocol.ModelSubjectCoverage{Kind: "k", Subject: refs[0], Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}
	second := &protocol.ModelSubjectCoverage{Kind: "k", Subject: refs[1], Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED}}
	index := indexCoverageSubjects(&protocol.ModelCoverage{Subjects: []*protocol.ModelSubjectCoverage{first, second}})
	c.Assert(index.covered(&protocol.Envelope{Kind: "k", Owner: refs[1]}), qt.IsTrue)
	c.Assert(index.covered(&protocol.Envelope{Kind: "k", Owner: refs[4]}), qt.IsFalse)
}

// checkedCoverageKind is a complete coverage kind of the engine's model.
func checkedCoverageKind(description *protocol.Description) *protocol.ComparisonKindCoverage {
	return &protocol.ComparisonKindCoverage{Model: proto.CloneOf(description.Engines[0].ValueModels[0]),
		Sources: []*protocol.ComparisonKnowledgeSource{{Outside: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}}
}

// checkTablesCovering checks, under one validation, the captured features of
// one table for each kind message given, each covering its facets by it.
func checkTablesCovering(c *qt.C, description *protocol.Description, kinds ...*protocol.ComparisonKindCoverage) *targetChecks {
	c.Helper()
	request := &protocol.PlanFeaturesRequest{Engine: "fixture", TargetFeatures: []string{"retention"}}
	checks := new(targetChecks)
	for _, kind := range kinds {
		captured := &protocol.CapturedFeatures{
			Objects: &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{}},
			Facets: &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{
				Kinds: []*protocol.ComparisonKindCoverage{kind},
			}},
		}
		c.Assert(checkCapturedFeatures(checks, description, request, description.Engines[0], protocol.ValueInput_DESIRED, captured), qt.IsNil)
	}
	return checks
}

// The coverages a shared list was resolved into hold one message for each
// kind, so the tables of one validation check it once.
func TestCoverageKindsOfOneMessageAreCheckedOnce(t *testing.T) {
	c := qt.New(t)
	description := checkedDescription()
	kind := checkedCoverageKind(description)

	checks := checkTablesCovering(c, description, kind, kind, kind)

	c.Assert(checks.kinds, qt.HasLen, 1)
	c.Assert(checks.contexts, qt.HasLen, 1)
}

// Coverages that hold copies of a kind check each copy once.
func TestCoverageKindCopiesAreCheckedOnceEach(t *testing.T) {
	c := qt.New(t)
	description := checkedDescription()
	kind := checkedCoverageKind(description)

	checks := checkTablesCovering(c, description, kind, proto.CloneOf(kind), proto.CloneOf(kind))

	c.Assert(checks.kinds, qt.HasLen, 3)
	c.Assert(checks.contexts, qt.HasLen, 1)
}

// A kind's answer belongs to the validation that asked: without one, every
// check reads the kind again, and a kind changed after it was checked is
// refused.
func TestCoverageKindAnswersBelongToTheirValidation(t *testing.T) {
	c := qt.New(t)
	description := checkedDescription()
	context := &protocol.CompareModelsRequest{Engine: "fixture", Kinds: []string{checkedKind}}
	kind := &protocol.ComparisonKindCoverage{Model: proto.CloneOf(description.Engines[0].ValueModels[0]),
		Sources: []*protocol.ComparisonKnowledgeSource{{Outside: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}}
	checks := new(targetChecks)

	first := checks.coverageKind(description, context, protocol.ValueInput_DESIRED, kind)
	kind.Model.CanonicalFormat = "changed after the check"
	again := checks.coverageKind(description, context, protocol.ValueInput_DESIRED, kind)
	var none *targetChecks
	unheld := none.coverageKind(description, context, protocol.ValueInput_DESIRED, kind)

	c.Assert(first, qt.IsNil)
	c.Assert(again, qt.IsNil)
	c.Assert(unheld, qt.ErrorIs, ErrProtocol)
}

// The facets of the tables of one creation reply are checked in one context
// of each representation, built once, and under the answers of one
// validation: a context built for each table held every table of the
// request.
func TestCreationRecordsShareTheirContexts(t *testing.T) {
	c := qt.New(t)
	description := checkedDescription()
	request := &protocol.ProjectTableCreationsRequest{Engine: "fixture", TargetFeatures: []string{"retention"}}
	records := creationRecordChecks{checks: new(targetChecks), contexts: make(map[protocol.ValueInput_Representation]*protocol.CompareModelsRequest)}

	for _, name := range []string{"a", "b", "c"} {
		input := &protocol.TableCreationInput{Subject: &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: name, Normalized: name}}}
		c.Assert(records.validate(description, request, description.Engines[0], input, protocol.ValueInput_DESIRED, nil), qt.IsNil)
	}

	c.Assert(records.contexts, qt.HasLen, 1)
	c.Assert(records.contexts[protocol.ValueInput_DESIRED].GetKinds(), qt.DeepEquals, []string{checkedKind})
	c.Assert(records.checks.answers, qt.HasLen, 1)
}
