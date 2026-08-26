package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
)

// checkedColumnWidget is a table whose column carries a CHECK, named or not.
func checkedColumnWidget(checkName string) *goschema.Database {
	return describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int"},
		goschema.Field{StructName: "Widget", Name: "b", Type: "int", Nullable: true,
			Check: "b > 0", CheckName: checkName},
	)
}

// checkedColumnCatalog is what a server reports back for it.
func checkedColumnCatalog(constraints ...catalog.Constraint) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "widget", Schema: "public", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO"},
				{Name: "b", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: constraints,
	}
}

// catalogColumnCheck is one CHECK row as a catalog reports it.
//
// The columns are populated because a real read populates them: measured
// against Ptah's own reader on PostgreSQL 18.6, every CHECK row carries the
// columns its condition mentions, derived by the server -- `["b"]` for
// `b int CHECK (b > 0)` and `["d","e"]` for a table-level `CHECK (d > 0 AND
// e > 0)`. A fixture leaving them out is not the shape this has to survive.
func catalogColumnCheck(name, clause string, columns ...string) catalog.Constraint {
	row := catalog.Constraint{
		Name: name, TableName: "widget", Schema: "public",
		Type: "CHECK", ColumnNames: columns,
	}
	if clause != "" {
		row.CheckClause = &clause
	}
	return row
}

// catalogClauses are the spellings one condition arrives in.
//
// `(b > 0)` is what Ptah's reader returns from PostgreSQL 18.6 --
// information_schema wraps the condition in one pair. `CHECK ((b > 0))` is what
// `pg_get_constraintdef` returns for the same constraint, which is the form a
// reader taking the definition rather than the clause produces. Both have to
// fold to what a declaration writes, and a fixture spelling the clause exactly
// as the declaration does compares two strings that are already equal and
// measures nothing.
var catalogClauses = []struct {
	name   string
	clause string
}{
	{name: "the clause a catalog reports", clause: "(b > 0)"},
	{name: "the definition a server prints", clause: "CHECK ((b > 0))"},
}

// TestAColumnCheckIsNotRebuiltOnEveryRun is the rebuild identity by condition
// exists to stop, on the other side of the pairing.
//
// Matching the two sides on the condition is half the answer. The comparison
// that follows read the two spellings verbatim, called the condition changed,
// and rendered a drop and an add under one Modify -- so the pair matched as one
// object and then replaced itself, every run, which is the outcome the identity
// was changed to prevent (stokaro/ptah#1663).
func TestAColumnCheckIsNotRebuiltOnEveryRun(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		reported string
	}{
		{name: "the server named it", declared: "", reported: "widget_b_check"},
		{name: "the author named it", declared: "b_positive", reported: "b_positive"},
	}
	for _, test := range tests {
		for _, spelling := range catalogClauses {
			t.Run(test.name+", "+spelling.name, func(t *testing.T) {
				c := qt.New(t)

				changes := changesFor(c, checkedColumnWidget(test.declared),
					checkedColumnCatalog(catalogColumnCheck(test.reported, spelling.clause, "b")))

				c.Assert(changes, qt.HasLen, 0)
			})
		}
	}
}

// TestAColumnCheckWhoseConditionChangedIsStillPlanned is the control.
//
// A fold that returned the same answer for everything would satisfy the rows
// above and would never notice a condition being edited.
func TestAColumnCheckWhoseConditionChangedIsStillPlanned(t *testing.T) {
	c := qt.New(t)
	description := checkedColumnWidget("")
	description.Fields[1].Check = "b > 5"

	changes := changesFor(c, description,
		checkedColumnCatalog(catalogColumnCheck("widget_b_check", "(b > 0)", "b")))

	c.Assert(changes, qt.Not(qt.HasLen), 0)
}

// TestATableLevelCheckIsNotModifiedByItsDerivedColumns is the other half of the
// column list.
//
// A table-level `CHECK (b > 0)` is declared with no column list --
// goschema.Constraint.Columns is what a UNIQUE fills in -- and PostgreSQL 18.6
// reports `["b"]` for it, derived from the condition. Comparing the two called
// the constraint changed on every run over a fact the declaration never stated.
func TestATableLevelCheckIsNotModifiedByItsDerivedColumns(t *testing.T) {
	c := qt.New(t)
	description := describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int"},
		goschema.Field{StructName: "Widget", Name: "b", Type: "int", Nullable: true},
	)
	description.Constraints = []goschema.Constraint{{
		StructName: "Widget", Name: "b_positive", Type: "CHECK", CheckExpression: "b > 0",
	}}

	changes := changesFor(c, description,
		checkedColumnCatalog(catalogColumnCheck("b_positive", "(b > 0)", "b")))

	c.Assert(changes, qt.HasLen, 0)
}

// TestPostgresNotNullRowsAreNotDroppedAsCheckConstraints pins what PostgreSQL
// 18 added underneath this family.
//
// It catalogs NOT NULL as a constraint, and Ptah's reader surfaces one row per
// not-null column, typed CHECK, naming the column and carrying no condition at
// all. Measured on 18.6: `id int PRIMARY KEY, d int NOT NULL` reports
// `widget_id_not_null` and `widget_d_not_null`. No description declares a
// constraint by those names, so every one of them read as a constraint to drop
// -- an apply that strips the nullability off every column that has it.
//
// The rule is the absence of a condition, not the name. The second row is
// deliberately NOT spelled PostgreSQL's way: the convention is one server's,
// and an implementation matching on it would record a clause-less row that any
// other reader spelled differently.
func TestPostgresNotNullRowsAreNotDroppedAsCheckConstraints(t *testing.T) {
	tests := []struct {
		name string
		row  string
	}{
		{name: "the name PostgreSQL 18.6 derives", row: "widget_id_not_null"},
		{name: "a name following no convention", row: "widget_0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c, checkedColumnWidget(""), checkedColumnCatalog(
				catalogColumnCheck("widget_b_check", "(b > 0)", "b"),
				catalogColumnCheck(test.row, "", "id"),
			))

			c.Assert(changes, qt.HasLen, 0)
		})
	}
}

// TestOneConditionTwiceIsReadRatherThanRefused pins what identity by condition
// costs and how it is paid.
//
// `CHECK (b > 0)` written twice on one table is legal SQL and a server keeps
// both rows. Identified by condition they are one object, and a state cannot
// hold two under one identity -- so reading such a database failed outright,
// with "two constraints carry one identity", and every verb that reads a schema
// died on a schema the server was perfectly happy with.
//
// The second row keeps its own NAME as identity instead. It is then a
// constraint the description does not declare, which is a removal a reader can
// see and argue with.
func TestOneConditionTwiceIsReadRatherThanRefused(t *testing.T) {
	c := qt.New(t)

	state, err := schemastate.FromCatalog(checkedColumnCatalog(
		catalogColumnCheck("widget_b_check", "(b > 0)", "b"),
		catalogColumnCheck("widget_b_check1", "(b > 0)", "b"),
	), platform.Postgres, identifier.ForDialect(platform.Postgres))

	c.Assert(err, qt.IsNil)
	// Both rows are there. The catalog declares nothing else of this kind, so
	// the count is the two checks and an implementation that dropped one to
	// avoid the collision would report one.
	c.Assert(state.OfKind(objectidentity.KindConstraint), qt.HasLen, 2)
}
