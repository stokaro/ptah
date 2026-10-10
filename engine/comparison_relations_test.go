package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

func declaredRelation(kind objectidentity.Kind, name string) objectidentity.ID {
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", name)
	ref.Kind = kind
	return ref
}

// TestObjectComparison_DeliversDeclaredRelations pins that an owner receives
// the views and materialized views a declaration names, in identity order and
// as a copy, so a model that shares their namespace can refuse a collision.
func TestObjectComparison_DeliversDeclaredRelations(t *testing.T) {
	c := qt.New(t)
	var received schemaext.ObjectComparisonRequest
	runtime := mustRuntime(c, comparisonProvider(comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		received = r
		return unchangedComparison(ctx, r)
	})))
	request := comparisonRequest(c)
	request.DeclaredRelations = []objectidentity.ID{declaredRelation(objectidentity.KindView, "recent"), declaredRelation(objectidentity.KindMatView, "daily")}

	_, err := runtime.CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(received.DeclaredRelations, qt.DeepEquals, []objectidentity.ID{
		declaredRelation(objectidentity.KindMatView, "daily"), declaredRelation(objectidentity.KindView, "recent"),
	})
	c.Assert(request.DeclaredRelations[0], qt.DeepEquals, declaredRelation(objectidentity.KindView, "recent"))
}

// TestObjectComparison_RefusesARelationThatIsNotOne pins the shape a declared
// relation must have: a named view or materialized view. A table belongs in
// Parents, and an unnamed relation names nothing.
func TestObjectComparison_RefusesARelationThatIsNotOne(t *testing.T) {
	tests := []struct {
		name     string
		relation objectidentity.ID
	}{
		{name: "a table", relation: declaredRelation(objectidentity.KindTable, "orders")},
		{name: "an unnamed view", relation: objectidentity.ID{Kind: objectidentity.KindView}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, comparisonProvider(comparisonFunc(unchangedComparison)))
			request := comparisonRequest(c)
			request.DeclaredRelations = []objectidentity.ID{test.relation}

			result, err := runtime.CompareObjects(t.Context(), request)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}
