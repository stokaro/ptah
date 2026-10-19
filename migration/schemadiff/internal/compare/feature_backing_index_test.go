package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// Feature ownership is a catalog fact. Common index comparison must not know
// the feature's kind, and it must still compare every independent index.
func TestIndexes_FeatureBackingIndex(t *testing.T) {
	owner := objectidentity.NewBuilder(identifier.ForDialect("postgres")).ConstraintParts("", "widget", "guard")
	owner.Kind = "example.com/feature/guard"
	for _, test := range []struct {
		name                string
		owner               objectidentity.ID
		desired             []schemamodel.Index
		additions, removals int
	}{
		{name: "undeclared backing index stays with its owner", owner: owner, removals: 1},
		{name: "unowned index is removed", removals: 2},
		{name: "explicitly declared index reaches comparison", owner: owner, desired: []schemamodel.Index{{Name: "guard_index", TableName: "widget", Fields: []string{"code"}}}, additions: 1, removals: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			live := &catalog.Database{Indexes: []catalog.Index{
				{Name: "guard_index", TableName: "widget", Columns: []string{"room"}, ManagedBy: test.owner},
				{Name: "guard_index", TableName: "other_widget", Columns: []string{"room"}},
			}}
			diff := &difftypes.SchemaDiff{}
			compare.IndexesWithDialect(&schemamodel.Database{Indexes: test.desired}, live, diff, "postgres", builtinRules(c, "postgres"))
			c.Assert(diff.IndexAdditions(), qt.HasLen, test.additions)
			c.Assert(diff.IndexRemovals(), qt.HasLen, test.removals)
		})
	}
}
