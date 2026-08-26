package shadow

// White-box testing required: mismatch collection turns a schema diff into the
// deterministic list the verification error carries, and the ordering and
// qualification it applies are not observable without calling it directly.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestCollectMismatches_ReportsQualifiedIndex(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions([]difftypes.IndexRef{
		{Name: "idx_shared", TableName: "users"},
		{Name: "idx_shared", TableName: "orders"},
	})

	got := collectMismatches(diff)

	c.Assert(got, qt.DeepEquals, []Mismatch{
		{
			Kind:    "missing_index",
			Table:   "orders",
			Object:  "orders.idx_shared",
			Message: "missing index orders.idx_shared",
		},
		{
			Kind:    "missing_index",
			Table:   "users",
			Object:  "users.idx_shared",
			Message: "missing index users.idx_shared",
		},
	})
}
