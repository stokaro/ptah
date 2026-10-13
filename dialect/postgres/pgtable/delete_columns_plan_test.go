package pgtable_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
)

func deleteSelectionPlan() featureplan.Request {
	semantics := identifier.ForDialect("postgres")
	ids := objectidentity.NewBuilder(semantics)
	table := ids.Table("app.children")
	key := ids.ConstraintParts("app", "children", "parent_fk")
	after := &pgtable.DesiredDeleteColumns{Columns: []string{"parent_id"}}
	desired := schemamodel.Constraint{Name: "parent_fk", Type: "FOREIGN KEY", Columns: []string{"tenant", "parent_id"},
		ForeignTable: "app.parents", ForeignColumns: []string{"tenant", "id"}, OnDelete: "SET NULL", OnUpdate: "CASCADE", Deferrable: true, Initially: "deferred", Comment: "Parent's key",
		Facets: must.Must(schemaext.NewFacets(after))}
	return featureplan.Request{Target: "postgres", Identifiers: semantics, Capabilities: capability.Postgres18(), ParentKinds: []schemaext.Kind{pgtable.DeleteColumnsKind},
		Changes: []schemaext.ChangeRecord{{Subject: key, Value: &pgtable.DeleteColumnsChange{After: after}}},
		Tables: []featureplan.Table{{Subject: table, Action: featureplan.AlterTable,
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Schema: "app", Name: "children"}, Constraints: []schemamodel.Constraint{desired}},
			Current: schemacapture.TableObservation{Table: catalog.Table{Schema: "app", Name: "children"}, Constraints: []catalog.Constraint{{Name: "parent_fk", Type: "FOREIGN KEY", ColumnNames: []string{"tenant", "parent_id"}}}},
		}}}
}

func TestDeleteColumnsPlanning_RestoresCompleteKey(t *testing.T) {
	c := qt.New(t)
	request := deleteSelectionPlan()
	result, err := (pgtable.DeleteColumnsService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 1)
	step := result.Contributions[0].Steps[0]
	c.Assert(step.Transaction, qt.Equals, plangraph.TransactionRequired)
	operation := step.Payload.Payload.(*pgtable.ReplaceDeleteColumns)
	c.Assert(operation.Comment, qt.Equals, "Parent's key")
	c.Assert(operation.OnUpdate, qt.Equals, "CASCADE")
	c.Assert(operation.Deferrable, qt.IsTrue)
	c.Assert(operation.Initially, qt.Equals, "deferred")
	c.Assert(operation.ForeignColumns, qt.DeepEquals, []string{"tenant", "id"})
	c.Assert(operation.DeleteColumns, qt.DeepEquals, []string{"parent_id"})
	operation.Columns[0] = "changed"
	c.Assert(request.Tables[0].Desired.Constraints[0].Columns[0], qt.Equals, "tenant")
}

func TestDeleteColumnsPlanning_CommonSteps(t *testing.T) {
	tests := []struct {
		name          string
		actions       []plangraph.Action
		contributions int
	}{
		{name: "separate drop and creation", actions: []plangraph.Action{plangraph.Drop, plangraph.Create}, contributions: 0},
		{name: "comment alteration", actions: []plangraph.Action{plangraph.Alter}, contributions: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := deleteSelectionPlan()
			for _, action := range test.actions {
				request.CommonSteps = append(request.CommonSteps, featureplan.CommonStep{Effects: []plangraph.Effect{{Subject: request.Changes[0].Subject, Action: action}}})
			}
			result, err := (pgtable.DeleteColumnsService{}).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, test.contributions)
			c.Assert(result.Changes, qt.HasLen, 1)
		})
	}
}

func TestDeleteColumnsPlanning_FailurePath(t *testing.T) {
	missing := deleteSelectionPlan()
	missing.Tables[0].Current.Constraints = nil
	changed := deleteSelectionPlan()
	changed.Tables[0].Desired.Constraints[0].Facets = schemaext.Facets{}
	removed := deleteSelectionPlan()
	removed.Tables[0].Action = featureplan.DropTable
	half := deleteSelectionPlan()
	half.CommonSteps = []featureplan.CommonStep{{Effects: []plangraph.Effect{{Subject: half.Changes[0].Subject, Action: plangraph.Drop}}}}
	for _, test := range []struct {
		name    string
		request featureplan.Request
	}{
		{"missing observation", missing}, {"different captured selection", changed}, {"removed parent", removed}, {"unpaired drop", half},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (pgtable.DeleteColumnsService{}).PlanFeatures(t.Context(), test.request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

func TestDeleteColumnsOperationCodec_RoundTrip(t *testing.T) {
	c := qt.New(t)
	result, err := (pgtable.DeleteColumnsService{}).PlanFeatures(t.Context(), deleteSelectionPlan())
	c.Assert(err, qt.IsNil)
	operation := result.Contributions[0].Steps[0].Payload.Payload.(*pgtable.ReplaceDeleteColumns)
	codec := pgtable.DeleteColumnsOperationCodec()
	data, err := codec.Encode(operation)
	c.Assert(err, qt.IsNil)
	restored, err := codec.Decode(data)
	c.Assert(err, qt.IsNil)
	c.Assert(restored, qt.DeepEquals, operation)
	c.Assert(restored, qt.Not(qt.Equals), operation)
}

func TestDeleteColumnsCodecs_FailurePath(t *testing.T) {
	for _, codec := range pgtable.DeleteColumnsCodecs() {
		t.Run(string(codec.Representation), func(t *testing.T) {
			for _, text := range []string{`null`, `{"columns":null}`, `{"columns":[""]}`, `{"columns":[1]}`, `{"columns":["id"],"extra":true}`, `{"columns":[],"columns":["id"]}`} {
				t.Run(text, func(t *testing.T) {
					c := qt.New(t)
					value, err := codec.Decode(json.RawMessage(text))
					c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
					c.Assert(value, qt.IsNil)
				})
			}
		})
	}
}
