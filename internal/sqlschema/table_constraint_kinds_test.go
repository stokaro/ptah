package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// TestToConstraint_CarriesEveryKindTheParserProduces is the defect, measured
// live before it was written.
//
// A `.sql` desired state declaring a table-level CHECK produced a plan without
// it, `ptah schema apply` reported success, and the database ended with no such
// constraint. Measured on PostgreSQL 18.6:
//
//	CREATE TABLE "t" ("id" BIGINT PRIMARY KEY, "a" INTEGER,
//	                  CONSTRAINT "t_a_positive" CHECK (a > 0));
//
//	planned:  CREATE TABLE "t" ("id" BIGINT PRIMARY KEY NOT NULL, "a" INTEGER);
//	catalog:  t_id_not_null | t_pkey
//
// The guarantee its author declared was gone, and nothing said so. The cause is
// this function: it answered two kinds and let `default` drop the rest, which
// silently covered CHECK and EXCLUDE, from a CREATE TABLE and from an
// ALTER TABLE ... ADD CONSTRAINT alike (stokaro/ptah#1215).
func TestToConstraint_CarriesEveryKindTheParserProduces(t *testing.T) {
	tests := []struct {
		name      string
		node      *ast.ConstraintNode
		wantType  string
		wantCarry bool
	}{
		{
			name: "a named CHECK",
			node: &ast.ConstraintNode{
				Type: ast.CheckConstraint, Name: `"t_a_positive"`, Expression: "a > 0",
			},
			wantType:  "CHECK",
			wantCarry: true,
		},
		{
			name:      "an unnamed CHECK",
			node:      &ast.ConstraintNode{Type: ast.CheckConstraint, Expression: "a > 0"},
			wantType:  "CHECK",
			wantCarry: true,
		},
		{
			name:      "a UNIQUE, which already worked",
			node:      ast.NewUniqueConstraint("t_a_key", "a"),
			wantType:  "UNIQUE",
			wantCarry: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			constraint, ok := sqlschema.ToConstraint(test.node, "T", "t", "")

			c.Assert(ok, qt.Equals, test.wantCarry)
			c.Assert(constraint.Type, qt.Equals, test.wantType)
			c.Assert(constraint.Table, qt.Equals, "t")
		})
	}
}

// TestToConstraint_ACheckKeepsItsCondition is what makes the carry worth
// anything.
//
// A CHECK carried without its expression would satisfy the rows above and would
// render `CHECK ()`, so the condition is asserted rather than the kind alone.
func TestToConstraint_ACheckKeepsItsCondition(t *testing.T) {
	c := qt.New(t)

	constraint, ok := sqlschema.ToConstraint(&ast.ConstraintNode{
		Type: ast.CheckConstraint, Name: `"t_a_positive"`, Expression: "a > 0",
	}, "T", "t", "")

	c.Assert(ok, qt.IsTrue)
	c.Assert(constraint.Name, qt.Equals, "t_a_positive")
	c.Assert(constraint.CheckExpression, qt.Equals, "a > 0")
}

// The source owner retains the method, elements and predicate together.
func TestRead_AnOwnedExclusionKeepsItsDefinition(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.ReadWith(t.Context(), []byte(`CREATE TABLE w (room integer,
CONSTRAINT w_room_excl EXCLUDE USING gist ("room" WITH =) WHERE (room IS NOT NULL));`),
		"postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	object, found, err := database.FeatureObjects.Get(pgtable.ExclusionRef("w", "w_room_excl", identifier.ForDialect("postgres")))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(database.Constraints, qt.HasLen, 0)
	constraint := object.Value.(*pgtable.DesiredExclusion)
	c.Assert(constraint.UsingMethod, qt.Equals, "gist")
	c.Assert(constraint.Elements, qt.Equals, `"room" WITH =`)
	c.Assert(constraint.WhereCondition, qt.Equals, "room IS NOT NULL")
}

// TestToConstraint_APrimaryKeyIsStillDeclined is the one kind that must not be
// carried, and the control for the change.
//
// A table-level PRIMARY KEY is carried on schemamodel.Table.PrimaryKey and, for a
// single column, on that column's Primary flag. Returning it here too would
// declare it twice, so a fix that made `default` carry everything would be
// wrong in the other direction.
func TestToConstraint_APrimaryKeyIsStillDeclined(t *testing.T) {
	c := qt.New(t)

	_, ok := sqlschema.ToConstraint(ast.NewPrimaryKeyConstraint("id"), "T", "t", "")

	c.Assert(ok, qt.IsFalse)
}
