package schemachange_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemachange"
)

// CHECK, PRIMARY KEY and EXCLUDE are the constraint kinds the prototype left
// out. They share one payload because they share one shape -- a name, a table,
// and a body no engine alters in place -- and differ only in the body. UNIQUE
// is not among them: it is a uniqueness guarantee, because it answers the
// question a foreign key asks and these do not (stokaro/ptah#1663).

func TestAClauseConstraintIsPlanned(t *testing.T) {
	tests := []constraintCase{
		{
			name:           "a check the database does not have",
			description:    constrainedWidget(checkConstraint("price > 0")),
			currentCatalog: constrainedCatalog(),
			wantOperation:  schemachange.Add,
		},
		{
			name:           "a check the desired schema no longer declares",
			description:    constrainedWidget(),
			currentCatalog: constrainedCatalog(catalogCheck("price > 0")),
			wantOperation:  schemachange.Remove,
		},
		{
			// A CHECK is identified by its condition, so a different condition
			// is a different guarantee rather than the same one altered. The
			// SQL is what it always was -- no engine alters a CHECK's condition
			// in place, so a modification rendered a drop and an add -- but the
			// change now says which object goes and which arrives
			// (stokaro/ptah#1663). Its two changes are asserted below.
			name:           "a check whose condition changed",
			description:    constrainedWidget(checkConstraint("price >= 0")),
			currentCatalog: constrainedCatalog(catalogCheck("price > 0")),
			wantPair:       []schemachange.Operation{schemachange.Add, schemachange.Remove},
		},
		{
			name: "a primary key the database does not have",
			description: constrainedWidget(schemamodel.Constraint{
				StructName: "Widget", Name: "pk_widget", Type: "PRIMARY KEY",
				Columns: []string{"id"},
			}),
			currentCatalog: constrainedCatalog(),
			wantOperation:  schemachange.Add,
		},
		{
			name:           "an exclusion the database does not have",
			description:    constrainedWidget(exclusionConstraint("room WITH =")),
			currentCatalog: constrainedCatalog(),
			wantOperation:  schemachange.Add,
		},
		{
			name:           "an exclusion whose elements changed",
			description:    constrainedWidget(exclusionConstraint("room WITH =, id WITH =")),
			currentCatalog: constrainedCatalog(catalogExclusion("room WITH =")),
			wantOperation:  schemachange.Modify,
			wantChanged:    []string{"exclusion"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c, test.description, test.currentCatalog)

			assertConstraintChanges(c, changes, test)
		})
	}
}

// constraintCase is one comparison, expecting either a single operation or the
// pair a replacement produces.
type constraintCase struct {
	name           string
	description    *schemamodel.Database
	currentCatalog *catalog.Database
	wantOperation  schemachange.Operation
	wantChanged    []string
	wantPair       []schemachange.Operation
}

// assertConstraintChanges reads one row's expectation, which is either a single
// operation or the pair a replacement produces.
func assertConstraintChanges(c *qt.C, changes []schemachange.Change, test constraintCase) {
	c.Helper()

	if len(test.wantPair) > 0 {
		c.Assert(operationsOf(changes), qt.DeepEquals, test.wantPair)
		return
	}
	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, test.wantOperation)
	c.Assert(changes[0].Changed, qt.DeepEquals, test.wantChanged)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
}

// operationsOf lists the operations a change set carries, in order.
func operationsOf(changes []schemachange.Change) []schemachange.Operation {
	operations := make([]schemachange.Operation, 0, len(changes))
	for _, change := range changes {
		operations = append(operations, change.Operation)
	}
	slices.Sort(operations)
	return operations
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
		constrainedWidget(checkConstraint("price > 0")), &catalog.Database{})

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
// schemamodel.Constraint.Table is documented as optional -- "Table name (if
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

func checkConstraint(expression string) schemamodel.Constraint {
	return schemamodel.Constraint{
		StructName: "Widget", Name: "ck_widget_price", Type: "CHECK", CheckExpression: expression,
	}
}

func catalogCheck(clause string) catalog.Constraint {
	return catalog.Constraint{
		Name: "ck_widget_price", TableName: "widget", Schema: "public",
		Type: "CHECK", CheckClause: &clause,
	}
}

func exclusionConstraint(elements string) schemamodel.Constraint {
	return schemamodel.Constraint{
		StructName: "Widget", Name: "ex_widget_room", Type: "EXCLUDE",
		UsingMethod: "gist", ExcludeElements: elements,
	}
}

func catalogExclusion(elements string) catalog.Constraint {
	method := "gist"
	return catalog.Constraint{
		Name: "ex_widget_room", TableName: "widget", Schema: "public",
		Type: "EXCLUDE", UsingMethod: &method, ExcludeElements: &elements,
	}
}

// constrainedWidget is a table carrying the given table-level constraints.
func constrainedWidget(constraints ...schemamodel.Constraint) *schemamodel.Database {
	description := describedTable(
		schemamodel.Field{StructName: "Widget", Name: "id", Type: "int"},
		schemamodel.Field{StructName: "Widget", Name: "price", Type: "int", Nullable: true},
		schemamodel.Field{StructName: "Widget", Name: "room", Type: "int", Nullable: true},
	)
	description.Constraints = append(description.Constraints, constraints...)
	return description
}

// constrainedCatalog is that table as a currentCatalog read reports it.
func constrainedCatalog(constraints ...catalog.Constraint) *catalog.Database {
	currentCatalog := catalogTable(
		catalog.Column{Name: "id", DataType: "integer", IsNullable: "NO"},
		catalog.Column{Name: "price", DataType: "integer", IsNullable: "YES"},
		catalog.Column{Name: "room", DataType: "integer", IsNullable: "YES"},
	)
	currentCatalog.Constraints = append(currentCatalog.Constraints, constraints...)
	return currentCatalog
}
