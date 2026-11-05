package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// referenceDescription declares one desired model, the one valueInputs
// declares.
func referenceDescription() *protocol.Description {
	d, _ := valueInputs()
	return d
}

// coveredTables is a validation request whose tables each cover the given
// models, one coverage record per model, in the order given.
func coveredTables(models ...*protocol.ValueModel) *protocol.ValidateSchemaRequest {
	request := &protocol.ValidateSchemaRequest{Engine: "fixture"}
	for _, name := range []string{"a", "b"} {
		coverage := &protocol.ComparisonCoverage{}
		for _, model := range models {
			coverage.Kinds = append(coverage.Kinds, &protocol.ComparisonKindCoverage{Model: model})
		}
		request.Tables = append(request.Tables, &protocol.ValidationTable{Declaration: &protocol.DeclaredTable{Name: name,
			Features: &protocol.CapturedFeatures{Facets: &protocol.ComparisonState{Coverage: coverage}}}})
	}
	return request
}

// coveredModels returns the models a coveredTables request names, table by
// table.
func coveredModels(request *protocol.ValidateSchemaRequest) []*protocol.ValueModel {
	var models []*protocol.ValueModel
	for _, table := range request.GetTables() {
		for _, record := range table.GetDeclaration().GetFeatures().GetFacets().GetCoverage().GetKinds() {
			models = append(models, record.GetModel())
		}
	}
	return models
}

// otherModel is the described model under another definition, which the
// description does not declare.
func otherModel(d *protocol.Description) *protocol.ValueModel {
	model := proto.CloneOf(d.Engines[0].ValueModels[0])
	model.DefinitionJson, model.DefinitionSha256 = []byte(`{"type":"string"}`), strings.Repeat("cd", 32)
	return model
}

func TestReferenceModels_HappyPath(t *testing.T) {
	t.Run("a described model travels by reference and the request keeps it whole", func(t *testing.T) {
		c := qt.New(t)
		d := referenceDescription()
		declared := d.Engines[0].ValueModels[0]
		request := coveredTables(declared, proto.CloneOf(declared))
		before := proto.CloneOf(request)

		sent := provider.ReferenceModels(d, request)

		reference := provider.ModelReference(declared)
		for _, model := range coveredModels(sent.(*protocol.ValidateSchemaRequest)) {
			c.Assert(proto.Equal(model, reference), qt.IsTrue)
		}
		c.Assert(proto.Equal(request, before), qt.IsTrue)
		c.Assert(len(reference.GetDefinitionJson()), qt.Equals, 0)
	})
	t.Run("a model the description does not declare travels whole", func(t *testing.T) {
		c := qt.New(t)
		d := referenceDescription()
		request := coveredTables(otherModel(d))

		sent := provider.ReferenceModels(d, request)

		c.Assert(sent, qt.Equals, proto.Message(request))
	})
	t.Run("a model two engines declare differently travels whole", func(t *testing.T) {
		c := qt.New(t)
		d := referenceDescription()
		second := proto.CloneOf(d.Engines[0])
		second.Id = "second"
		second.ValueModels[0].CanonicalFormat = "minutes-json-v1"
		d.Engines = append(d.Engines, second)
		request := coveredTables(d.Engines[0].ValueModels[0])

		sent := provider.ReferenceModels(d, request)

		c.Assert(sent, qt.Equals, proto.Message(request))
	})
	t.Run("a description keeps its own models", func(t *testing.T) {
		c := qt.New(t)
		d := referenceDescription()

		c.Assert(provider.ReferenceModels(d, d), qt.Equals, proto.Message(d))
	})
}

func TestResolveModelReferences_HappyPath(t *testing.T) {
	c := qt.New(t)
	d := referenceDescription()
	declared := d.Engines[0].ValueModels[0]
	request := coveredTables(declared, otherModel(d))
	received := proto.CloneOf(provider.ReferenceModels(d, request).(*protocol.ValidateSchemaRequest))

	err := provider.ResolveModelReferences(d, received)

	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(received, request), qt.IsTrue)
	models := coveredModels(received)
	// Every reference to the model resolves to one copy made for this
	// message; the description keeps its own.
	c.Assert(models[0] == models[2], qt.IsTrue)
	c.Assert(models[0] == declared, qt.IsFalse)
	c.Assert(models[1] == models[3], qt.IsFalse)
}

func TestResolveModelReferences_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name      string
		reference func(*protocol.Description) *protocol.ValueModel
	}{
		{"a reference to a model the description does not declare", func(d *protocol.Description) *protocol.ValueModel {
			return provider.ModelReference(otherModel(d))
		}},
		{"a reference that carries more than its identity", func(d *protocol.Description) *protocol.ValueModel {
			reference := provider.ModelReference(d.Engines[0].ValueModels[0])
			reference.Encoding = "json"
			return reference
		}},
		{"a reference to a model two engines declare differently", func(d *protocol.Description) *protocol.ValueModel {
			second := proto.CloneOf(d.Engines[0])
			second.Id = "second"
			second.ValueModels[0].DisplayName = "retentions"
			d.Engines = append(d.Engines, second)
			return provider.ModelReference(d.Engines[0].ValueModels[0])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := referenceDescription()
			request := coveredTables(test.reference(d))

			err := provider.ResolveModelReferences(d, request)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*a model reference names no model of the description: example\.org/fixture/retention of version 1`)
		})
	}
}

// Without the feature negotiated nothing resolves a reference, and the
// validation of the message refuses it as a model the description does not
// declare, where it accepts the model itself.
func TestModelReferenceWithoutResolution_FailurePath(t *testing.T) {
	c := qt.New(t)
	d, request := valueInputs()
	d.Features = append(d.Features, provider.DecodeSourceFeature)
	model := d.Engines[0].ValueModels[0]
	model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
	d.Engines[0].YamlSourceKeys = []*protocol.YAMLSourceKey{{Scope: protocol.YAMLSourceKey_DOCUMENT, Key: "declared", Selector: "declared", Models: []string{model.Kind}}}
	decode := func(model *protocol.ValueModel) *protocol.DecodeSourceRequest {
		return &protocol.DecodeSourceRequest{Engine: request.GetEngine(), Documents: []*protocol.SourceDocument{{Id: "a", File: "a.yaml",
			Format: protocol.SourceDocument_YAML, Models: []*protocol.ValueModel{model}}}}
	}

	c.Assert(provider.ValidateSourceRequest(d, decode(proto.CloneOf(model))), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(d, decode(provider.ModelReference(model))), qt.ErrorIs, provider.ErrProtocol)
}

func TestCloneSharingModels_HappyPath(t *testing.T) {
	c := qt.New(t)
	d := referenceDescription()
	declared := d.Engines[0].ValueModels[0]
	separate := proto.CloneOf(declared)
	request := coveredTables(declared, separate)

	copied := provider.CloneSharingModels(request)

	c.Assert(proto.Equal(copied, request), qt.IsTrue)
	models, original := coveredModels(copied), coveredModels(request)
	// The copy shares a model where the request shared it, and shares
	// nothing with the request.
	c.Assert(models[0] == models[2], qt.IsTrue)
	c.Assert(models[1] == models[3], qt.IsTrue)
	c.Assert(models[0] == models[1], qt.IsFalse)
	c.Assert(models[0] == original[0], qt.IsFalse)
	models[0].DisplayName = "changed"
	c.Assert(declared.GetDisplayName(), qt.Equals, "")
	c.Assert(models[2].GetDisplayName(), qt.Equals, "changed")
}

// A model whose definition bytes are replaced after a description validated
// is checked again, however often the description was validated before.
func TestValidatedModelDefinitionIsCheckedAgainWhenReplaced(t *testing.T) {
	c := qt.New(t)
	d := referenceDescription()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)

	d.Engines[0].ValueModels[0].DefinitionJson = []byte(`{"type":"string"}`)

	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*model definition digest differs from its content`)
}
