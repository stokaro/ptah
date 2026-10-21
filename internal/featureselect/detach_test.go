package featureselect_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/featureselect"
)

const hangKind schemaext.Kind = "example.org/hang"

// hang is a fixture model attached to one table without being its child, the
// shape of a SQL Server extended property, and binding others, the shape of a
// security policy.
type hang struct {
	On    string   `json:"on"`
	Binds []string `json:"binds,omitempty"`
}

func (*hang) Kind() schemaext.Kind { return hangKind }
func (v *hang) Clone() schemaext.Value {
	return &hang{On: v.On, Binds: slices.Clone(v.Binds)}
}
func (v *hang) Equal(other schemaext.Value) bool {
	right, ok := other.(*hang)
	return ok && v.On == right.On && slices.Equal(v.Binds, right.Binds)
}

// hangRelations reports the table a hang is on as its attachment, and the
// tables it binds as dependencies.
type hangRelations struct{}

func (hangRelations) DescribeRelations(_ context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
	builder := objectidentity.NewBuilder(request.Identifiers)
	result := schemaext.RelationResult{Complete: true}
	for _, value := range request.Values {
		record := schemaext.ValueRelations{Subject: value.Subject, Complete: true}
		if on := value.Value.(*hang).On; on != "" {
			record.Attachment = builder.Table(on)
		}
		for _, table := range value.Value.(*hang).Binds {
			record.Dependencies = append(record.Dependencies, builder.Table(table))
		}
		result.Values = append(result.Values, record)
	}
	return result, nil
}

// hangRuntime registers the hang owner, with relation discovery for declared
// hangs.
func hangRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{ID: "example.org/hangs", Codecs: modelCodecs(&hang{}),
		Targets:   []engine.Target{{Name: "hangs"}},
		Relations: []engine.RelationDiscovery{{Target: "hangs", Representation: schemaext.Desired, Kinds: []schemaext.Kind{hangKind}, Service: hangRelations{}}},
	}))
}

// hangSide is a desired side holding objects, the hang kind and each object
// with complete knowledge.
func hangSide(objects ...schemaext.Object) featureselect.Side {
	var kinds []schemaext.KindCoverage
	for _, model := range hangRuntime().Codecs().Definitions() {
		if model.Representation == schemaext.Desired {
			kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})})
		}
	}
	var subjects []schemaext.SubjectCoverage
	for _, object := range objects {
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: object.Value.Kind(), Subject: object.Ref,
			Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	}
	return featureselect.Side{Representation: schemaext.Desired, Objects: must.Must(schemaext.NewObjects(objects...)),
		Coverage: must.Must(schemaext.NewCoverage(schemaext.Desired, kinds, subjects))}
}

// TestBindingsDetach_RemovesWhatHangsOffADroppedTable pins that Detach removes
// an object attached to a dropped table, with its knowledge, and keeps one
// attached to another table, one attached to none, and one that only binds the
// dropped table, which the server refuses to drop for it.
func TestBindingsDetach_RemovesWhatHangsOffADroppedTable(t *testing.T) {
	c := qt.New(t)
	declared := hangSide(
		schemaext.Object{Ref: ref(hangKind, "on-a"), Value: &hang{On: "app.a"}},
		schemaext.Object{Ref: ref(hangKind, "on-b"), Value: &hang{On: "app.b"}},
		schemaext.Object{Ref: ref(hangKind, "loose"), Value: &hang{}},
		schemaext.Object{Ref: ref(hangKind, "binds-a"), Value: &hang{Binds: []string{"app.a"}}})
	bindings := must.Must(featureselect.CaptureBindings(context.Background(), hangRuntime(), "hangs", semantics, declared))

	objects, coverage := bindings.Detach(declared.Objects, declared.Coverage, keepTables("app.a"))

	kept := []objectidentity.ID{ref(hangKind, "binds-a"), ref(hangKind, "loose"), ref(hangKind, "on-b")}
	c.Assert(objects.Refs(), qt.DeepEquals, kept)
	var subjects []objectidentity.ID
	for _, record := range coverage.SubjectRecords() {
		subjects = append(subjects, record.Subject)
	}
	c.Assert(subjects, qt.ContentEquals, kept)
	c.Assert(declared.Objects.Len(), qt.Equals, 4)
}

// TestBindingsDetach_ZeroBindingsKeepEverything pins that bindings nothing
// was captured into detach nothing.
func TestBindingsDetach_ZeroBindingsKeepEverything(t *testing.T) {
	c := qt.New(t)
	declared := hangSide(schemaext.Object{Ref: ref(hangKind, "on-a"), Value: &hang{On: "app.a"}})

	objects, _ := featureselect.Bindings{}.Detach(declared.Objects, declared.Coverage, keepTables("app.a"))

	c.Assert(objects.Len(), qt.Equals, 1)
}
