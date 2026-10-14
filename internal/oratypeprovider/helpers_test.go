package oratypeprovider_test

import (
	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/oratype"
	"ptah.run/internal/oratypeprovider"
)

// newRuntime selects the owner beside a provider of the Oracle target and
// nothing else, so every answer below is the owner's.
func newRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	return must.Must(engine.New(engine.Provider{ID: "example.org/targets", Targets: []engine.Target{{Name: platform.Oracle}}},
		oratypeprovider.Provider()))
}

// declared is the declaration of the type name of the value's model, in the
// connection's schema.
func declared(name string, value schemaext.Value) schemaext.Object {
	return must.Must(oratype.DesiredObject(oratype.Ref(value.Kind(), "", name), value))
}

// held is the observation of the type name of the value's model, which the
// catalog reports in upper case and without a schema.
func held(name string, value schemaext.Value) schemaext.Object {
	return must.Must(oratype.ObservedObject(oratype.Ref(value.Kind(), "", name), value))
}

// comparison compares desired with current on Oracle 23, both sources
// describing every type.
func comparison(desired, current []schemaext.Object) schemaext.ObjectComparisonRequest {
	return schemaext.ObjectComparisonRequest{
		Target: platform.Oracle, Identifiers: identifier.ForDialect(platform.Oracle), Capabilities: capability.Oracle23(),
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(desired...)),
			Coverage: must.Must(oratype.CompleteCoverage(schemaext.Desired))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(current...)),
			Coverage: must.Must(oratype.CompleteCoverage(schemaext.Observed))},
	}
}

// objects is a short spelling of a list of objects.
func objects(values ...schemaext.Object) []schemaext.Object { return values }

// planning plans changes on Oracle 23, which holds both kinds.
func planning(changes ...schemaext.ChangeRecord) featureplan.Request {
	return featureplan.Request{Target: platform.Oracle, Identifiers: identifier.ForDialect(platform.Oracle),
		Capabilities: capability.Oracle23(), Changes: changes}
}

// change is a change of the type of kind named name.
func change(kind schemaext.Kind, name string, value schemaext.ChangeValue) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: oratype.Ref(kind, "", name), Value: value}
}

// planned is one step as a test reads it: the type its effect names, its
// payload and phase, and the effect's action.
type planned struct {
	Subject string
	Payload ast.ExtensionPayload
	Phase   featureplan.Phase
	Effect  plangraph.Action
}

// stepsOf lists the planned steps in the order the owner's dependencies put
// them.
func stepsOf(c *qt.C, contributions []plangraph.Contribution[featureplan.Operation]) []planned {
	c.Helper()
	c.Assert(contributions, qt.HasLen, 1)
	plan := must.Must(plangraph.Schedule(c.Context(), contributions[0]))
	listed := make([]planned, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		c.Assert(step.Effects, qt.HasLen, 1)
		subject := step.Effects[0].Subject
		listed = append(listed, planned{Subject: string(subject.Kind) + " " + subject.Name.Source, Payload: step.Payload.Payload,
			Phase: step.Payload.Phase, Effect: step.Effects[0].Action})
	}
	return listed
}

// domainTarget and objectTypeTarget name a type that names no schema.
func domainTarget(name string) oratype.Target {
	return oratype.Target{Kind: oratype.DomainKind, Name: name}
}

func objectTypeTarget(name string) oratype.Target {
	return oratype.Target{Kind: oratype.ObjectTypeKind, Name: name}
}
