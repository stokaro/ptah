package pgtable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
)

func persistencePlan() featureplan.Request {
	subject := persistenceSubject()
	return featureplan.Request{Target: "postgres", ParentKinds: []schemaext.Kind{pgtable.PersistenceKind},
		Changes: []schemaext.ChangeRecord{{Subject: subject, Value: &pgtable.PersistenceChange{After: &pgtable.DesiredPersistence{Unlogged: true}}}},
		Tables: []featureplan.Table{{Subject: subject, Action: featureplan.AlterTable,
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Schema: "app", Name: "cache", Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPersistence{Unlogged: true}))}},
			Current: schemacapture.TableObservation{Table: catalog.Table{Schema: "app", Name: "cache"}},
		}},
	}
}

func TestPersistencePlanning_CapturedState(t *testing.T) {
	c := qt.New(t)
	request := persistencePlan()
	result, err := (pgtable.PersistenceService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Parents, qt.HasLen, 1)
	c.Assert(result.Contributions, qt.HasLen, 1)
	step := result.Contributions[0].Steps[0]
	c.Assert(step.Transaction, qt.Equals, plangraph.TransactionAllowed)
	c.Assert(step.Payload.Parent, qt.DeepEquals, persistenceSubject())
	c.Assert(step.Effects[0], qt.DeepEquals, plangraph.Effect{Subject: persistenceSubject(), Action: plangraph.Read})
	step.Payload.Payload.(*pgtable.AlterPersistence).Change.After.Unlogged = false
	c.Assert(request.Changes[0].Value.(*pgtable.PersistenceChange).After.Unlogged, qt.IsTrue)
}

func TestPersistencePlanning_RefusesMissingOrConflictingCapture(t *testing.T) {
	missing := persistencePlan()
	missing.Tables = nil
	conflict := persistencePlan()
	conflict.Tables[0].Desired.Table.Facets = schemaext.Facets{}
	removed := persistencePlan()
	removed.Tables[0].Action = featureplan.DropTable
	for _, request := range []featureplan.Request{missing, conflict, removed} {
		c := qt.New(t)
		result, err := (pgtable.PersistenceService{}).PlanFeatures(t.Context(), request)
		c.Assert(err, qt.IsNil)
		c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(result.Contributions, qt.HasLen, 0)
		c.Assert(result.Changes, qt.HasLen, 0)
		c.Assert(result.Parents, qt.HasLen, 0)
	}
}

func TestPersistencePlanning_RefusesRebuildAtomically(t *testing.T) {
	c := qt.New(t)
	request := persistencePlan()
	extra := request.Tables[0].Clone()
	extra.Action = featureplan.RebuildTable
	request.Tables = append(request.Tables, extra)
	result, err := (pgtable.PersistenceService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Parents, qt.HasLen, 0)
	c.Assert(*result.Diagnostics[0].Parent, qt.Equals, 1)
}

func TestPersistencePlanning_ParentLifecycle(t *testing.T) {
	for _, action := range []featureplan.ParentAction{featureplan.CreateTable, featureplan.DropTable, featureplan.AlterTable} {
		c := qt.New(t)
		request := persistencePlan()
		request.Changes = nil
		request.Tables[0].Action = action
		result, err := (pgtable.PersistenceService{}).PlanFeatures(t.Context(), request)
		c.Assert(err, qt.IsNil)
		c.Assert(result.Err(request), qt.IsNil)
		c.Assert(result.Contributions, qt.HasLen, 0)
		c.Assert(result.Parents, qt.HasLen, 1)
		c.Assert(result.Parents[0].Action, qt.Equals, action)
	}
}

func TestPersistencePlanning_RefusesUnloggedPartitionedTable(t *testing.T) {
	c := qt.New(t)
	request := persistencePlan()
	request.Tables[0].Current.Table.Partitioned = true
	result, err := (pgtable.PersistenceService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Parents, qt.HasLen, 0)
}
