package types_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestSchemaDiff_SetIndexAdditionsPreservesDuplicateMultiset(t *testing.T) {
	c := qt.New(t)
	refs := []types.IndexRef{
		{TableName: "zeta.accounts", Name: "idx_shared"},
		{TableName: "alpha.users", Name: "idx_z"},
		{TableName: "alpha.orders", Name: "idx_shared"},
		{TableName: "alpha.users", Name: "idx_a"},
	}
	diff := &types.SchemaDiff{}

	diff.SetIndexAdditions(refs)

	c.Assert(diff.IndexesAdded, qt.DeepEquals, []types.IndexRef{
		{TableName: "alpha.orders", Name: "idx_shared"},
		{TableName: "alpha.users", Name: "idx_a"},
		{TableName: "alpha.users", Name: "idx_z"},
		{TableName: "zeta.accounts", Name: "idx_shared"},
	})
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []types.IndexRef{
		{TableName: "alpha.orders", Name: "idx_shared"},
		{TableName: "alpha.users", Name: "idx_a"},
		{TableName: "alpha.users", Name: "idx_z"},
		{TableName: "zeta.accounts", Name: "idx_shared"},
	})
	c.Assert(diff.HasChanges(), qt.IsTrue)
}

func TestSchemaDiff_SetIndexRefsAreSymmetricAndCloneInputs(t *testing.T) {
	c := qt.New(t)
	additionRefs := []types.IndexRef{
		{TableName: "zeta.accounts", Name: "idx_shared"},
		{TableName: "alpha.orders", Name: "idx_shared"},
	}
	removalRefs := []types.IndexRef{
		{TableName: "alpha.orders", Name: "idx_shared"},
		{TableName: "zeta.accounts", Name: "idx_shared"},
	}
	additions := &types.SchemaDiff{}
	removals := &types.SchemaDiff{}

	additions.SetIndexAdditions(additionRefs)
	removals.SetIndexRemovals(removalRefs)
	c.Assert(additionRefs, qt.DeepEquals, []types.IndexRef{
		{TableName: "zeta.accounts", Name: "idx_shared"},
		{TableName: "alpha.orders", Name: "idx_shared"},
	})
	c.Assert(removalRefs, qt.DeepEquals, []types.IndexRef{
		{TableName: "alpha.orders", Name: "idx_shared"},
		{TableName: "zeta.accounts", Name: "idx_shared"},
	})
	additionRefs[0] = types.IndexRef{TableName: "mutated", Name: "mutated"}
	removalRefs[0] = types.IndexRef{TableName: "mutated", Name: "mutated"}

	c.Assert(additions.IndexesAdded, qt.DeepEquals, removals.IndexesRemoved)
	c.Assert(additions.IndexAdditions(), qt.DeepEquals, removals.IndexRemovals())
	c.Assert(additions.IndexesAdded, qt.DeepEquals, []types.IndexRef{
		{TableName: "alpha.orders", Name: "idx_shared"},
		{TableName: "zeta.accounts", Name: "idx_shared"},
	})
	c.Assert(additions.HasChanges(), qt.IsTrue)
	c.Assert(removals.HasChanges(), qt.IsTrue)
}

func TestSchemaDiff_IndexAccessorsReturnClones(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{TableName: "public.users", Name: "idx_users_email"},
		},
		IndexesRemoved: []types.IndexRef{
			{TableName: "public.orders", Name: "idx_orders_reference"},
		},
	}

	additions := diff.IndexAdditions()
	removals := diff.IndexRemovals()
	additions[0].TableName = "mutated"
	removals[0].TableName = "mutated"

	c.Assert(diff.IndexesAdded, qt.DeepEquals, []types.IndexRef{
		{TableName: "public.users", Name: "idx_users_email"},
	})
	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []types.IndexRef{
		{TableName: "public.orders", Name: "idx_orders_reference"},
	})
}

func TestSchemaDiff_IndexRefsJSONShape(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{TableName: "public.users", Name: "idx_email"},
	})
	diff.SetIndexRemovals([]types.IndexRef{
		{TableName: "audit.users", Name: "idx_email"},
	})

	data, err := json.Marshal(diff)
	c.Assert(err, qt.IsNil)

	var got struct {
		IndexesAdded   []types.IndexRef `json:"indexes_added"`
		IndexesRemoved []types.IndexRef `json:"indexes_removed"`
	}
	err = json.Unmarshal(data, &got)

	c.Assert(err, qt.IsNil)
	c.Assert(got.IndexesAdded, qt.DeepEquals, []types.IndexRef{
		{TableName: "public.users", Name: "idx_email"},
	})
	c.Assert(got.IndexesRemoved, qt.DeepEquals, []types.IndexRef{
		{TableName: "audit.users", Name: "idx_email"},
	})
	var fields map[string]json.RawMessage
	err = json.Unmarshal(data, &fields)
	c.Assert(err, qt.IsNil)
	_, hasAddedDetails := fields["indexes_added_with_tables"]
	_, hasRemovedDetails := fields["indexes_removed_with_tables"]
	c.Assert(hasAddedDetails, qt.IsFalse)
	c.Assert(hasRemovedDetails, qt.IsFalse)
	c.Assert(diff.HasChanges(), qt.IsTrue)
}
