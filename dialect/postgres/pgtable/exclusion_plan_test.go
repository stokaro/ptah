package pgtable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
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

func exclusionPlan(change *pgtable.ExclusionChange) featureplan.Request {
	return featureplan.Request{Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Postgres18(), ParentKinds: []schemaext.Kind{pgtable.ExclusionKind},
		Changes: []schemaext.ChangeRecord{{Subject: exclusionSubject(), Value: change}},
		Tables: []featureplan.Table{{Subject: pgtable.ExclusionTable(exclusionSubject()), Action: featureplan.AlterTable,
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Schema: "app", Name: "rooms"}, OwnedObjects: desiredExclusionObjects(change.After), FeatureCoverage: must.Must(pgtable.ExclusionCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))},
			Current: schemacapture.TableObservation{Table: catalog.Table{Schema: "app", Name: "rooms"}, OwnedObjects: observedExclusionObjects(change.Before), FeatureCoverage: must.Must(pgtable.ExclusionCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))},
		}},
	}
}

func TestExclusionPlanning_Transaction(t *testing.T) {
	for _, test := range []struct {
		name        string
		change      *pgtable.ExclusionChange
		action      plangraph.Action
		transaction plangraph.Transaction
	}{
		{"create", &pgtable.ExclusionChange{After: &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}}, plangraph.Create, plangraph.TransactionAllowed},
		{"drop", &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition()))}, plangraph.Drop, plangraph.TransactionAllowed},
		{"replace", &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition())), After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "during WITH &&"}}}, plangraph.Alter, plangraph.TransactionRequired},
		{"comment", &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition())), After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&", Comment: "Room schedule"}}, CommentOnly: true}, plangraph.Alter, plangraph.TransactionAllowed},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := exclusionPlan(test.change)
			result, err := (pgtable.ExclusionService{}).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Parents, qt.HasLen, 1)
			c.Assert(result.Contributions, qt.HasLen, 1)
			step := result.Contributions[0].Steps[0]
			c.Assert(step.Transaction, qt.Equals, test.transaction)
			c.Assert(step.Effects, qt.DeepEquals, []plangraph.Effect{{Subject: pgtable.ExclusionTable(exclusionSubject()), Action: plangraph.Read}, {Subject: exclusionSubject(), Action: test.action}})
			operation := step.Payload.Payload.(*pgtable.AlterExclusion)
			c.Assert(operation.Name, qt.Equals, "no_overlap")
			c.Assert(operation.Change.Copy(), qt.DeepEquals, test.change)
			c.Assert(operation.Change.Copy(), qt.Not(qt.Equals), test.change)
		})
	}
}

func TestExclusionPlanning_DependencyEffects(t *testing.T) {
	c := qt.New(t)
	after := &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}
	after.Dependencies = []objectidentity.ID{exclusionDependency("btree_gist")}
	request := exclusionPlan(&pgtable.ExclusionChange{After: after})
	result, err := (pgtable.ExclusionService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Contributions[0].Steps[0].Effects[2], qt.DeepEquals, plangraph.Effect{Subject: exclusionDependency("btree_gist"), Action: plangraph.Read})
	result.Contributions[0].Steps[0].Payload.Payload.(*pgtable.AlterExclusion).Change.After.Dependencies[0] = exclusionDependency("changed")
	c.Assert(after.Dependencies[0].Name.Source, qt.Equals, "btree_gist")
}

func TestExclusionPlanning_CaptureFailures(t *testing.T) {
	change := &pgtable.ExclusionChange{After: &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}}
	missing := exclusionPlan(change)
	missing.Tables = nil
	duplicated := exclusionPlan(change)
	duplicated.Tables = append(duplicated.Tables, duplicated.Tables[0])
	absent := exclusionPlan(change)
	absent.Tables[0].Desired.OwnedObjects = schemaext.Objects{}
	unknown := exclusionPlan(change)
	unknown.Tables[0].Current.FeatureCoverage = schemaext.Coverage{}
	for _, test := range []struct {
		name    string
		request featureplan.Request
	}{
		{"missing parent", missing}, {"duplicate parent", duplicated}, {"missing declaration", absent}, {"unknown absence", unknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (pgtable.ExclusionService{}).PlanFeatures(t.Context(), test.request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(test.request), qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Parents, qt.HasLen, 0)
		})
	}
}

func TestExclusionPlanning_CreateTableAddsNamedConstraints(t *testing.T) {
	c := qt.New(t)
	request := exclusionPlan(&pgtable.ExclusionChange{After: &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}})
	request.Changes = nil
	request.Tables[0].Action = featureplan.CreateTable
	request.Tables[0].Current = schemacapture.TableObservation{}
	result, err := (pgtable.ExclusionService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 1)
	c.Assert(result.Parents, qt.HasLen, 1)
	c.Assert(result.Parents[0].Steps, qt.HasLen, 1)
	c.Assert(result.Parents[0].Strategy, qt.Equals, "add each named exclusion constraint after its table exists")
}

func TestExclusionPlanning_RefusesPartialRebuild(t *testing.T) {
	c := qt.New(t)
	request := exclusionPlan(&pgtable.ExclusionChange{After: &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}})
	extra := request.Tables[0].Clone()
	extra.Subject = objectidentity.NewBuilder(request.Identifiers).Table("app.other")
	extra.Action = featureplan.RebuildTable
	request.Tables = append(request.Tables, extra)
	result, err := (pgtable.ExclusionService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Parents, qt.HasLen, 0)
	c.Assert(*result.Diagnostics[0].Parent, qt.Equals, 1)
}
