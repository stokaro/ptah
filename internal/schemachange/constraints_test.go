package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemachange"
)

// CHECK, PRIMARY KEY and EXCLUDE are the constraint kinds the prototype left
// out. They share one payload because they share one shape -- a name, a table,
// and a body no engine alters in place -- and differ only in the body. UNIQUE
// is not among them: it is a uniqueness guarantee, because it answers the
// question a foreign key asks and these do not (stokaro/ptah#1663).

func TestAClauseConstraintIsPlanned(t *testing.T) {
	tests := []struct {
		name          string
		description   *goschema.Database
		catalog       *dbschematypes.DBSchema
		wantOperation schemachange.Operation
		wantChanged   []string
	}{
		{
			name:          "a check the database does not have",
			description:   constrainedWidget(checkConstraint("price > 0")),
			catalog:       constrainedCatalog(),
			wantOperation: schemachange.Add,
		},
		{
			name:          "a check the desired schema no longer declares",
			description:   constrainedWidget(),
			catalog:       constrainedCatalog(catalogCheck("price > 0")),
			wantOperation: schemachange.Remove,
		},
		{
			name:          "a check whose condition changed",
			description:   constrainedWidget(checkConstraint("price >= 0")),
			catalog:       constrainedCatalog(catalogCheck("price > 0")),
			wantOperation: schemachange.Modify,
			wantChanged:   []string{"expression"},
		},
		{
			name: "a primary key the database does not have",
			description: constrainedWidget(goschema.Constraint{
				StructName: "Widget", Name: "pk_widget", Type: "PRIMARY KEY",
				Columns: []string{"id"},
			}),
			catalog:       constrainedCatalog(),
			wantOperation: schemachange.Add,
		},
		{
			name:          "an exclusion the database does not have",
			description:   constrainedWidget(exclusionConstraint("room WITH =")),
			catalog:       constrainedCatalog(),
			wantOperation: schemachange.Add,
		},
		{
			name:          "an exclusion whose elements changed",
			description:   constrainedWidget(exclusionConstraint("room WITH =, id WITH =")),
			catalog:       constrainedCatalog(catalogExclusion("room WITH =")),
			wantOperation: schemachange.Modify,
			wantChanged:   []string{"exclusion"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c, test.description, test.catalog)

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(changes[0].Operation, qt.Equals, test.wantOperation)
			c.Assert(changes[0].Changed, qt.DeepEquals, test.wantChanged)
			c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
		})
	}
}

// TestAnUnchangedClauseConstraintIsNotAChange is the control the rows above
// need: a comparison that reported a change for every constraint would satisfy
// all six and rebuild every constraint on every run.
//
// The body is compared as written, folded for whitespace and case only, so a
// condition the server merely re-spaced is not a change either.
func TestAnUnchangedClauseConstraintIsNotAChange(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		reported string
	}{
		{name: "the same condition", declared: "price > 0", reported: "price > 0"},
		{name: "re-spaced", declared: "price > 0", reported: "  price > 0  "},
		{name: "re-cased", declared: "PRICE > 0", reported: "price > 0"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c,
				constrainedWidget(checkConstraint(test.declared)),
				constrainedCatalog(catalogCheck(test.reported)))

			c.Assert(changes, qt.HasLen, 0)
		})
	}
}

// TestAConstraintOfATableBeingCreatedIsNotItsOwnChange pins the rule a column
// of a new table already follows. The constraints of a table that does not
// exist yet ride inside the CREATE, and planning one here would declare it
// twice -- measured: a table-level primary key produced both the inline key and
// an ALTER TABLE ... ADD PRIMARY KEY.
func TestAConstraintOfATableBeingCreatedIsNotItsOwnChange(t *testing.T) {
	c := qt.New(t)

	changes := changesFor(c,
		constrainedWidget(checkConstraint("price > 0")), &dbschematypes.DBSchema{})

	c.Assert(kindsOf(changes), qt.DeepEquals, []string{"table"})
}

// TestAConstraintInAnUndescribedSchemaIsNotDropped is the coverage gate, the
// same rule every other family in this comparison carries.
func TestAConstraintInAnUndescribedSchemaIsNotDropped(t *testing.T) {
	c := qt.New(t)
	description := constrainedWidget()
	description.NotDescribed = coverage.Set{}.WithObject(coverage.Schema, "public")

	changes := changesFor(c, description, constrainedCatalog(catalogCheck("price > 0")))

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Undecidable)
	c.Assert(changes[0].Diagnostic, qt.Contains, "is not a request to drop it")
}

// TestAnExclusionNamesTheTableItsStructDeclares is the one shape where this
// path deliberately does NOT match the shipping planner, and the divergence is
// measured rather than argued.
//
// goschema.Constraint.Table is documented as optional -- "Table name (if
// different from struct name)" -- and CHECK and PRIMARY KEY honour that. The
// shipping planner's EXCLUDE route reads the field directly, with no fallback
// to the struct, and renders `ALTER TABLE ""`. Filed as stokaro/ptah#2008.
func TestAnExclusionNamesTheTableItsStructDeclares(t *testing.T) {
	c := qt.New(t)

	operations, err := schemachange.Plan(
		changesFor(c, constrainedWidget(exclusionConstraint("room WITH =")), constrainedCatalog()),
		postgresProfile())

	c.Assert(err, qt.IsNil)
	c.Assert(schemachange.Statements(operations), qt.HasLen, 1)
	c.Assert(schemachange.Statements(operations)[0], qt.Contains, `ALTER TABLE "widget"`)
	c.Assert(schemachange.Statements(operations)[0], qt.Contains, "EXCLUDE USING gist (room WITH =)")
}

func checkConstraint(expression string) goschema.Constraint {
	return goschema.Constraint{
		StructName: "Widget", Name: "ck_widget_price", Type: "CHECK", CheckExpression: expression,
	}
}

func catalogCheck(clause string) dbschematypes.DBConstraint {
	return dbschematypes.DBConstraint{
		Name: "ck_widget_price", TableName: "widget", Schema: "public",
		Type: "CHECK", CheckClause: &clause,
	}
}

func exclusionConstraint(elements string) goschema.Constraint {
	return goschema.Constraint{
		StructName: "Widget", Name: "ex_widget_room", Type: "EXCLUDE",
		UsingMethod: "gist", ExcludeElements: elements,
	}
}

func catalogExclusion(elements string) dbschematypes.DBConstraint {
	method := "gist"
	return dbschematypes.DBConstraint{
		Name: "ex_widget_room", TableName: "widget", Schema: "public",
		Type: "EXCLUDE", UsingMethod: &method, ExcludeElements: &elements,
	}
}

// constrainedWidget is a table carrying the given table-level constraints.
func constrainedWidget(constraints ...goschema.Constraint) *goschema.Database {
	description := describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int"},
		goschema.Field{StructName: "Widget", Name: "price", Type: "int", Nullable: true},
		goschema.Field{StructName: "Widget", Name: "room", Type: "int", Nullable: true},
	)
	description.Constraints = append(description.Constraints, constraints...)
	return description
}

// constrainedCatalog is that table as a catalog read reports it.
func constrainedCatalog(constraints ...dbschematypes.DBConstraint) *dbschematypes.DBSchema {
	catalog := catalogTable(
		dbschematypes.DBColumn{Name: "id", DataType: "integer", IsNullable: "NO"},
		dbschematypes.DBColumn{Name: "price", DataType: "integer", IsNullable: "YES"},
		dbschematypes.DBColumn{Name: "room", DataType: "integer", IsNullable: "YES"},
	)
	catalog.Constraints = append(catalog.Constraints, constraints...)
	return catalog
}
