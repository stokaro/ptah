package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestIndexes_ExclusionConstraintBackingIndex pins issue #2013 with the two
// controls that keep the arm from widening.
//
// PostgreSQL enforces an EXCLUDE constraint with an index of the constraint's
// own name, and it is the one no filter written in terms of the index ROW can
// recognize. Measured on PostgreSQL 17.6, `EXCLUDE USING gist (room WITH =)`
// leaves:
//
//	relname        | indisprimary | indisunique | contype
//	ex_widget_room | f            | f           | x
//
// -- indistinguishable from an ordinary index on those flags, so the identity
// has to come from the constraint catalog. Dropping it is refused with
// `cannot drop index ex_widget_room because constraint ex_widget_room on table
// widget requires it`, which failed the whole migration.
func TestIndexes_ExclusionConstraintBackingIndex(t *testing.T) {
	backingIndex := catalog.Index{
		Name: "ex_widget_room", TableName: "widget", Columns: []string{"room"},
	}
	unrelatedIndex := catalog.Index{
		Name: "idx_widget_code", TableName: "widget", Columns: []string{"code"},
	}

	tests := []struct {
		name          string
		constraints   []catalog.Constraint
		desired       []schemamodel.Index
		wantAdditions int
		wantRemovals  int
	}{
		{
			// The defect: the description declares the constraint and no
			// index, and the index it is enforced with was planned for a drop
			// the server refuses.
			name:          "the constraint's own index is left alone",
			constraints:   widgetExclusion(),
			desired:       []schemamodel.Index{declaredUnrelatedWidgetIndex()},
			wantAdditions: 0,
			wantRemovals:  0,
		},
		{
			// The narrowing, which the arm is subject to like the other three:
			// a description that names the object as an INDEX owns it, so the
			// database row has to reach comparison to be replaced. Suppressed
			// instead, the declaration would have nothing to match and the
			// plan would carry the addition with no drop before it.
			name:        "an index the description declares is still compared",
			constraints: widgetExclusion(),
			desired: []schemamodel.Index{
				{Name: "ex_widget_room", TableName: "widget", Fields: []string{"code"}},
				declaredUnrelatedWidgetIndex(),
			},
			wantAdditions: 1,
			wantRemovals:  1,
		},
		{
			// A CHECK is the other clause constraint and is enforced with no
			// index at all, so an index that shares its name is an index
			// somebody wrote, and dropping it is the whole point.
			name: "a CHECK of the same name owns nothing",
			constraints: []catalog.Constraint{{
				Name: "ex_widget_room", TableName: "widget", Type: "CHECK",
				CheckClause: new("room > 0"),
			}},
			desired:       []schemamodel.Index{declaredUnrelatedWidgetIndex()},
			wantAdditions: 0,
			wantRemovals:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Indexes: test.desired}
			database := &catalog.Database{
				Constraints: test.constraints,
				Indexes:     []catalog.Index{backingIndex, unrelatedIndex},
			}
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(desired, database, diff, "postgres")

			c.Assert(diff.IndexAdditions(), qt.HasLen, test.wantAdditions)
			c.Assert(diff.IndexRemovals(), qt.HasLen, test.wantRemovals)
		})
	}
}

// widgetExclusion is the constraint as PostgreSQL reports it, beside the index
// of the same name that the index catalog reports separately.
func widgetExclusion() []catalog.Constraint {
	return []catalog.Constraint{{
		Name: "ex_widget_room", TableName: "widget", Type: "EXCLUDE",
		UsingMethod: new("gist"), ExcludeElements: new("room WITH ="),
	}}
}

// declaredUnrelatedWidgetIndex is in every row so no expectation can be met by
// a comparison that ignored the read entirely.
func declaredUnrelatedWidgetIndex() schemamodel.Index {
	return schemamodel.Index{
		Name: "idx_widget_code", TableName: "widget", Fields: []string{"code"},
	}
}
