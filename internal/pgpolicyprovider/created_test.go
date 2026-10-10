package pgpolicyprovider_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/pgpolicyprovider"
)

const otherChildKind schemaext.Kind = "example.org/other/child"

// otherChild is a named child of a table that another owner creates.
type otherChild struct{}

func (*otherChild) Kind() schemaext.Kind { return otherChildKind }

func (*otherChild) Clone() schemaext.Value { return &otherChild{} }

func (*otherChild) Equal(other schemaext.Value) bool { _, ok := other.(*otherChild); return ok }

// otherOwner accounts for its children of every table with a receipt and no
// step.
type otherOwner struct{}

func (otherOwner) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	for _, table := range request.Tables {
		for _, kind := range request.ParentKinds {
			result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: kind, Action: table.Action,
				Strategy: "the other owner creates its child"})
		}
	}
	return result, nil
}

// TestPlanPolicies_LeavesAnotherOwnersChildAlone pins a created table that
// also has a child of another owner: the row-security owner creates its own
// policies and leaves the other child to its owner.
func TestPlanPolicies_LeavesAnotherOwnersChildAlone(t *testing.T) {
	c := qt.New(t)
	other := engine.Provider{ID: "example.org/other",
		Codecs: []schemaext.Codec{
			schemaext.ModelCodec[*otherChild]{Prototype: &otherChild{}, Representation: schemaext.Desired, Version: 1,
				Definition: json.RawMessage(`{"type":"object","additionalProperties":false}`)}.Codec(),
			schemaext.ModelCodec[*otherChild]{Prototype: &otherChild{}, Representation: schemaext.Observed, Version: 1,
				Definition: json.RawMessage(`{"type":"object","additionalProperties":false}`)}.Codec(),
		},
		Planning: []engine.Planning{{Target: "postgres", ParentKinds: []schemaext.Kind{otherChildKind}, Service: otherOwner{}}},
	}
	runtime, err := engine.New(engine.Provider{ID: "example.org/targets", Targets: []engine.Target{{Name: "postgres"}, {Name: "cockroachdb"},
		{Name: "yugabytedb"}}}, pgpolicyprovider.Provider(), other)
	c.Assert(err, qt.IsNil)
	orders := tableRef("orders")
	child := schemaext.Object{Ref: objectidentity.ID{Kind: objectidentity.Kind(otherChildKind), Schema: orders.Schema, Parent: orders.Name,
		Name: objectidentity.Part{Source: "feed", Normalized: "feed"}}, Value: &otherChild{}}
	request := planning()
	request.Tables = []featureplan.Table{{Subject: orders, Action: featureplan.CreateTable,
		Desired: schemacapture.TableDeclaration{Table: declaredOrders.Table, OwnedObjects: objects(c, desiredPolicy(c, "orders", "tenant", permissiveDeclared), child)}}}

	result, err := runtime.PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(summary(result.Contributions), qt.DeepEquals, []string{
		"created/000000/policy/000001 *pgpolicy.PolicyOperation create ptah.run/pgpolicy/policy app.orders.tenant, read table app.orders, read role reader allowed dependent",
	})
	c.Assert(result.Parents, qt.HasLen, 3)
}
