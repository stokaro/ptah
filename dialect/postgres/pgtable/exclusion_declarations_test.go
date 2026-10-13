package pgtable_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
)

func exclusionDeclarations() featureplan.DeclarationRequest {
	ids := identifier.ForDialect("postgres")
	var objects []schemaext.Object
	for _, name := range []string{"first", "second"} {
		objects = append(objects, schemaext.Object{Ref: pgtable.ExclusionRef("app.rooms", name, ids), Value: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH =", Comment: name}}})
	}
	return featureplan.DeclarationRequest{Target: "postgres", Identifiers: ids, Capabilities: capability.Postgres18(), Objects: objects,
		Tables:      []schemacapture.TableDeclaration{{Table: schemamodel.Table{Schema: "app", Name: "rooms"}}},
		CommonSteps: []featureplan.CommonStep{{ID: plangraph.StepID{Owner: "test", Name: "table"}}, {ID: plangraph.StepID{Owner: "test", Name: "function"}}},
	}
}

func TestExclusionDeclarations_PreservesEveryIdentity(t *testing.T) {
	c := qt.New(t)
	request := exclusionDeclarations()
	result, err := (pgtable.ExclusionService{}).PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Declarations, qt.HasLen, 2)
	c.Assert(result.Contributions, qt.HasLen, 1)
	contribution := result.Contributions[0]
	c.Assert(contribution.Steps, qt.HasLen, 2)
	c.Assert(contribution.Dependencies, qt.HasLen, 4)
	for i, step := range contribution.Steps {
		c.Assert(result.Declarations[i].Subject, qt.DeepEquals, request.Objects[i].Ref)
		c.Assert(result.Declarations[i].Steps, qt.DeepEquals, []plangraph.StepID{step.ID})
		c.Assert(step.Payload.Parent, qt.DeepEquals, pgtable.ExclusionTable(request.Objects[i].Ref))
		c.Assert(step.Transaction, qt.Equals, plangraph.TransactionAllowed)
		operation := step.Payload.Payload.(*pgtable.AlterExclusion)
		c.Assert(operation.Name, qt.Equals, request.Objects[i].Ref.Name.Source)
		c.Assert(operation.Change.After.Comment, qt.Equals, operation.Name)
		c.Assert(step.Effects, qt.DeepEquals, []plangraph.Effect{{Subject: step.Payload.Parent, Action: plangraph.Read}, {Subject: request.Objects[i].Ref, Action: plangraph.Create}})
		for j, common := range request.CommonSteps {
			c.Assert(contribution.Dependencies[i*len(request.CommonSteps)+j], qt.DeepEquals, plangraph.Dependency{Before: common.ID, After: step.ID})
		}
		operation.Change.After.Comment = "mutated"
	}
	c.Assert(request.Objects[0].Value.(*pgtable.DesiredExclusion).Comment, qt.Equals, "first")
}

func TestExclusionDeclarations_RefusesWholeBatch(t *testing.T) {
	missing := exclusionDeclarations()
	missing.Tables = nil
	duplicate := exclusionDeclarations()
	duplicate.Tables = append(duplicate.Tables, duplicate.Tables[0])
	invalid := exclusionDeclarations()
	invalid.Objects[1].Value = &pgtable.DesiredExclusion{}
	observed := exclusionDeclarations()
	observed.Objects[1].Value = &pgtable.ObservedExclusion{UsingMethod: "btree", Elements: "room WITH ="}
	for _, test := range []struct {
		name    string
		request featureplan.DeclarationRequest
	}{{"missing parent", missing}, {"duplicate parent", duplicate}, {"invalid second object", invalid}, {"observed second object", observed}} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (pgtable.ExclusionService{}).PlanDeclarations(t.Context(), test.request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(test.request), qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Declarations, qt.HasLen, 0)
		})
	}
}

func TestExclusionDeclarations_Canceled(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (pgtable.ExclusionService{}).PlanDeclarations(ctx, exclusionDeclarations())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.DeclarationResult{})
}

func TestExclusionPlanning_CreateParentKeepsEquivalentChildren(t *testing.T) {
	c := qt.New(t)
	declarations := exclusionDeclarations()
	parent := objectidentity.NewBuilder(declarations.Identifiers).TableParts("app", "rooms")
	request := featureplan.Request{Target: "postgres", Identifiers: declarations.Identifiers, Capabilities: declarations.Capabilities, ParentKinds: []schemaext.Kind{pgtable.ExclusionKind},
		Tables: []featureplan.Table{{Subject: parent, Action: featureplan.CreateTable, Desired: schemacapture.TableDeclaration{Table: declarations.Tables[0].Table, OwnedObjects: must.Must(schemaext.NewObjects(declarations.Objects...))}}}}
	result, err := (pgtable.ExclusionService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Parents, qt.HasLen, 1)
	c.Assert(result.Parents[0].Steps, qt.HasLen, 2)
	c.Assert(result.Contributions, qt.HasLen, 1)
	for i, step := range result.Contributions[0].Steps {
		c.Assert(result.Parents[0].Steps[i], qt.DeepEquals, step.ID)
		c.Assert(step.Payload.Phase, qt.Equals, featureplan.PhaseDependent)
		c.Assert(step.Payload.Payload.(*pgtable.AlterExclusion).Name, qt.Equals, declarations.Objects[i].Ref.Name.Source)
	}
}
