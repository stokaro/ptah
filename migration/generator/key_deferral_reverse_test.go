package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback that adds back a key the forward change dropped adds it as it
// was, deferral included, read from the database the forward change started
// from. Put back plain, the key checks every row at once where it deferred.
func TestPlanBidirectionalSchemaDiff_DroppedKeyComesBackDeferrable(t *testing.T) {
	tests := []struct {
		name       string
		constraint catalog.Constraint
		marked     bool
		want       string
	}{
		{
			name: "a UNIQUE",
			constraint: catalog.Constraint{
				Name: "orders_code_uq", TableName: "orders", Schema: "app", Type: "UNIQUE",
				ColumnNames: []string{"code"}, ColumnName: "code", Deferrable: true, Initially: "deferred",
			},
			want: `ADD CONSTRAINT "orders_code_uq" UNIQUE ("code") DEFERRABLE INITIALLY DEFERRED;`,
		},
		{
			name: "a primary key",
			constraint: catalog.Constraint{
				Name: "orders_pkey", TableName: "orders", Schema: "app", Type: "PRIMARY KEY",
				ColumnNames: []string{"id"}, ColumnName: "id", Deferrable: true,
			},
			want: `ADD PRIMARY KEY ("id") DEFERRABLE;`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{ConstraintsRemoved: difftypes.ConstraintRemovals{{
				Name: test.constraint.Name, TableName: "app.orders", Type: test.constraint.Type,
			}}}

			_, sql := planConstraintCommentRollback(c, diff, &catalog.Database{Constraints: []catalog.Constraint{test.constraint}})

			c.Assert(sql, qt.Contains, test.want, qt.Commentf("rollback:\n%s", sql))
		})
	}
}

// A rollback of a UNIQUE the forward change removed through its backing index
// puts the constraint back deferrable too.
func TestPlanBidirectionalSchemaDiff_ConstraintBackedIndexComesBackDeferrable(t *testing.T) {
	c := qt.New(t)
	ref := difftypes.IndexRef{Name: "orders_code_uq", TableName: "app.orders"}
	diff := &difftypes.SchemaDiff{
		IndexesRemoved:                []difftypes.IndexRef{ref},
		ConstraintBackedIndexRemovals: []difftypes.IndexRef{ref},
	}
	current := &catalog.Database{Constraints: []catalog.Constraint{{
		Name: "orders_code_uq", TableName: "orders", Schema: "app", Type: "UNIQUE",
		ColumnNames: []string{"code"}, ColumnName: "code", Deferrable: true, Initially: "deferred",
	}}}

	_, sql := planConstraintCommentRollback(c, diff, current)

	c.Assert(sql, qt.Contains, `ADD CONSTRAINT "orders_code_uq" UNIQUE ("code") DEFERRABLE INITIALLY DEFERRED;`,
		qt.Commentf("rollback:\n%s", sql))
}
