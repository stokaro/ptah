package schemachange_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemachange"
	"go.5x5.cz/ptah/internal/schemastate"
)

// The change model reached tables and columns last, and they are the family
// that shows what a name in a slice costs. `TablesAdded []string` says a table
// appeared and nothing about what is in it; `ColumnsModified` says a column
// moved and nothing about from what to what, so every planner recovers the
// answer from the desired description it is handed separately (ADR 0001
// decision 8, stokaro/ptah#1662).

// TestColumnModificationCarriesBothSides pins the first definition-of-done item:
// a type change carries before and after rather than a name.
func TestColumnModificationCarriesBothSides(t *testing.T) {
	c := qt.New(t)

	changes := changesFor(c, describedTable(goschema.Field{
		StructName: "Widget", Name: "code", Type: "varchar(200)", Nullable: true,
	}), catalogTable(dbschematypes.DBColumn{
		Name: "code", DataType: "varchar(50)", IsNullable: "YES",
	}))

	c.Assert(changes, qt.HasLen, 1)
	change := changes[0]
	c.Assert(change.Operation, qt.Equals, schemachange.Modify)
	c.Assert(change.Changed, qt.DeepEquals, []string{"type"})
	c.Assert(change.Before.Column.Type, qt.Equals, "varchar(50)")
	c.Assert(change.After.Column.Type, qt.Equals, "varchar(200)")
	c.Assert(change.Risk, qt.Equals, schemachange.RiskDataDependent)
	c.Assert(change.Reversibility, qt.Equals, schemachange.ReversibleWithData)
	c.Assert(change.Status, qt.Equals, schemachange.Planned)
}

// TestTypeSpellingsThatMeanOneTypeAreNotAChange is the control on the row above.
// A comparison that reported every spelling difference would satisfy it
// completely, and would report an ALTER for every column of a database Ptah
// itself created: the declaration says `int` and the catalog says `integer`.
func TestTypeSpellingsThatMeanOneTypeAreNotAChange(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		reported string
	}{
		{name: "int against integer", declared: "int", reported: "integer"},
		{name: "case", declared: "INTEGER", reported: "integer"},
		{name: "a postgres typecast the catalog spells out", declared: "text", reported: "text"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c, describedTable(goschema.Field{
				StructName: "Widget", Name: "code", Type: test.declared, Nullable: true,
			}), catalogTable(dbschematypes.DBColumn{
				Name: "code", DataType: test.reported, IsNullable: "YES",
			}))

			c.Assert(changes, qt.HasLen, 0)
		})
	}
}

// TestNotNullColumnAdditionAnswersFromTheRowStatistics is the second
// definition-of-done item, and the reason it takes three rows: a table the plan
// knows is empty, a table the plan knows has rows, and a table with no
// statistics are three different answers, and the third is the one a model with
// two states turns into the first.
func TestNotNullColumnAdditionAnswersFromTheRowStatistics(t *testing.T) {
	tests := []struct {
		name           string
		rows           int64
		statsUnknown   bool
		wantStatus     schemachange.Status
		wantDiagnostic string
	}{
		{
			name:           "a table the database reports as holding rows",
			rows:           42,
			wantStatus:     schemachange.Blocked,
			wantDiagnostic: "estimated to hold 42 rows",
		},
		{
			name:       "a table the database reports as empty",
			rows:       0,
			wantStatus: schemachange.Planned,
		},
		{
			name:           "a table the database keeps no statistics for",
			statsUnknown:   true,
			wantStatus:     schemachange.Undecidable,
			wantDiagnostic: "no usable row statistics",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			catalog := catalogTable(dbschematypes.DBColumn{
				Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
			})
			catalog.Tables[0].EstimatedRows = test.rows
			catalog.Tables[0].RowStatsUnknown = test.statsUnknown

			changes := changesFor(c, describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{StructName: "Widget", Name: "code", Type: "text"},
			), catalog)

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(changes[0].Operation, qt.Equals, schemachange.Add)
			c.Assert(changes[0].Status, qt.Equals, test.wantStatus)
			c.Assert(changes[0].Diagnostic, qt.Contains, test.wantDiagnostic)
		})
	}
}

// TestAColumnThatFillsItselfIsPlannedOnAPopulatedTable is the control on the
// rule above. It blocks a NOT NULL addition because nothing supplies a value;
// a rule that blocked every NOT NULL addition would pass the rows above and
// stop three legitimate changes.
func TestAColumnThatFillsItselfIsPlannedOnAPopulatedTable(t *testing.T) {
	tests := []struct {
		name  string
		field goschema.Field
	}{
		{
			name:  "nullable",
			field: goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
		},
		{
			name: "a declared default",
			field: goschema.Field{
				StructName: "Widget", Name: "code", Type: "text", Default: "unset", DefaultSet: true,
			},
		},
		{
			name: "a default expression",
			field: goschema.Field{
				StructName: "Widget", Name: "code", Type: "text", DefaultExpr: "now()",
			},
		},
		{
			name: "a column the engine fills",
			field: goschema.Field{
				StructName: "Widget", Name: "code", Type: "int", AutoInc: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			catalog := catalogTable(dbschematypes.DBColumn{
				Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
			})
			catalog.Tables[0].EstimatedRows = 42

			changes := changesFor(c, describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				test.field,
			), catalog)

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
			c.Assert(changes[0].Diagnostic, qt.Equals, "")
		})
	}
}

// TestDroppingSaysWhatItCosts is the third definition-of-done item. A table drop
// and a column drop destroy what was in them, and re-creating either produces
// an empty shape rather than what was there, so neither may be handed a
// rollback the plan cannot execute.
func TestDroppingSaysWhatItCosts(t *testing.T) {
	tests := []struct {
		name        string
		description *goschema.Database
		wantKind    string
	}{
		{
			name:        "a table",
			description: &goschema.Database{},
			wantKind:    "table",
		},
		{
			name: "a column",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true}),
			wantKind: "column",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c, test.description, catalogTable(
				dbschematypes.DBColumn{
					Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
				},
				dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"},
			))

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(string(changes[0].ID.Kind), qt.Equals, test.wantKind)
			c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
			c.Assert(changes[0].Risk, qt.Equals, schemachange.RiskDataLoss)
			c.Assert(changes[0].Reversibility, qt.Equals, schemachange.Irreversible)
			c.Assert(changes[0].Before.ID.Key(), qt.Equals, changes[0].ID.Key())
		})
	}
}

// TestCreatingATableSaysItCostsNothing is the control on the row above: a model
// that marked every change irreversible would satisfy it and would stop every
// plan that creates anything.
func TestCreatingATableSaysItCostsNothing(t *testing.T) {
	c := qt.New(t)

	changes := changesFor(c, describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
	), &dbschematypes.DBSchema{})

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Add)
	c.Assert(changes[0].Risk, qt.Equals, schemachange.RiskLow)
	c.Assert(changes[0].Reversibility, qt.Equals, schemachange.Reversible)
	c.Assert(changes[0].After.Table.Columns, qt.HasLen, 1)
}

// changesFor runs the adapters, normalization and comparison for one input
// pair, which is what the table family needs; the graph and the planner have
// their own tests and this family does not render yet.
func changesFor(
	c *qt.C,
	description *goschema.Database,
	catalog *dbschematypes.DBSchema,
) []schemachange.Change {
	c.Helper()
	return changesForProfile(c, description, catalog, postgresProfile())
}

// changesForProfile is [changesFor] against a named target, which the type fold
// needs: what a declared type folds to is the TARGET's rule.
func changesForProfile(
	c *qt.C,
	description *goschema.Database,
	catalog *dbschematypes.DBSchema,
	profile schemastate.Profile,
) []schemachange.Change {
	c.Helper()
	desired, err := schemastate.FromDescription(description, profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	current, err := schemastate.FromCatalog(catalog, profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	normalizedDesired, err := schemastate.Normalize(desired, profile)
	c.Assert(err, qt.IsNil)
	normalizedCurrent, err := schemastate.Normalize(current, profile)
	c.Assert(err, qt.IsNil)
	changes, err := schemachange.Compare(normalizedCurrent, normalizedDesired, profile)
	c.Assert(err, qt.IsNil)
	return changes
}

// describedTable is one authored table carrying the given fields.
func describedTable(fields ...goschema.Field) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Widget", Name: "widget"}},
		Fields: fields,
	}
}

// catalogTable is the same table as a catalog read reports it.
func catalogTable(columns ...dbschematypes.DBColumn) *dbschematypes.DBSchema {
	return catalogTableInSchema("public", columns...)
}

// catalogTableInSchema is [catalogTable] in a named schema.
//
// The schema has to agree with the PROFILE the row runs against: an unqualified
// declaration resolves to the profile dialect's default schema, which is
// "public" on PostgreSQL and empty on Oracle, so a "public" catalog read past
// an Oracle profile describes a different table (stokaro/ptah#1662).
func catalogTableInSchema(schema string, columns ...dbschematypes.DBColumn) *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{Name: "widget", Schema: schema, Columns: columns}},
	}
}

// A description that declined a whole schema is silent about it rather than
// empty, and reading that silence as absence is what turns a partial read into
// a drop. The rule runs in both directions and they gate opposite changes: the
// desired side's record gates REMOVALS, the current side's gates ADDITIONS
// (stokaro/ptah#1276).
func TestCoverageGatesTablesInBothDirections(t *testing.T) {
	tests := []struct {
		name           string
		desiredLimits  coverage.Set
		currentLimits  coverage.Set
		description    *goschema.Database
		catalog        *dbschematypes.DBSchema
		wantOperation  schemachange.Operation
		wantStatus     schemachange.Status
		wantDiagnostic string
	}{
		{
			name:          "a table the desired schema never claimed to describe",
			desiredLimits: coverage.Set{}.WithObject(coverage.Schema, "public"),
			description:   &goschema.Database{},
			catalog: catalogTable(dbschematypes.DBColumn{
				Name: "id", DataType: "integer", IsNullable: "NO",
			}),
			wantOperation:  schemachange.Remove,
			wantStatus:     schemachange.Undecidable,
			wantDiagnostic: "is not a request to drop it",
		},
		{
			name:          "a table the read never looked for",
			currentLimits: coverage.Set{}.WithObject(coverage.Schema, "public"),
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true}),
			catalog:        &dbschematypes.DBSchema{},
			wantOperation:  schemachange.Add,
			wantStatus:     schemachange.Undecidable,
			wantDiagnostic: "CREATE TABLE has no conditional form",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.description.NotDescribed = test.desiredLimits
			test.catalog.NotDescribed = test.currentLimits

			changes := changesFor(c, test.description, test.catalog)

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(changes[0].Operation, qt.Equals, test.wantOperation)
			c.Assert(changes[0].Status, qt.Equals, test.wantStatus)
			c.Assert(changes[0].Diagnostic, qt.Contains, test.wantDiagnostic)
		})
	}
}

// TestCoverageDoesNotWithholdWhatBothSidesDescribe is the control on the rows
// above. A comparison that withheld every table would satisfy both of them and
// would plan nothing at all.
func TestCoverageDoesNotWithholdWhatBothSidesDescribe(t *testing.T) {
	c := qt.New(t)

	changes := changesFor(c, describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
	), &dbschematypes.DBSchema{})

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
}

// TestATableCreationOrdersBeforeTheConstraintThatNeedsIt is the fourth
// definition-of-done item, and the first time either endpoint of the edge is a
// change.
//
// The graph has recorded an ordering between a foreign key and the tables it
// touches since the constraint slice landed, and it ordered nothing: an edge
// whose other end is not in the change set is skipped, and no table was ever in
// the change set. Both ends are now.
//
// The two rows isolate the two edges. A fixture that creates BOTH tables leaves
// either edge able to produce the answer alone, so dropping one would not be
// visible -- which is what a first version of this test measured.
func TestATableCreationOrdersBeforeTheConstraintThatNeedsIt(t *testing.T) {
	tests := []struct {
		name    string
		catalog *dbschematypes.DBSchema
		wantNew string
	}{
		{
			// Only the OWNING edge: the referenced table is already there.
			name:    "the table that carries it",
			catalog: parentOnlyCatalog(),
			wantNew: "child",
		},
		{
			// Only the REFERENCED edge: the table carrying the key is already
			// there, and the table it points at is not.
			name:    "the table it references",
			catalog: childOnlyCatalog(),
			wantNew: "parent",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			forward := forwardOrderFor(c, parentChildSchema(), test.catalog, "constraint", "table")

			c.Assert(kindsOf(forward), qt.DeepEquals, []string{"table", "constraint"})
			c.Assert(forward[0].ID.Name.Source, qt.Equals, test.wantNew)
			c.Assert(forward[1].ID.Name.Source, qt.Equals, "fk_child_parent")
		})
	}
}

// parentChildSchema is a parent with a key and a child whose column references
// it.
func parentChildSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parent"},
			{StructName: "Child", Name: "child"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "int", Primary: true},
			{StructName: "Child", Name: "id", Type: "int", Primary: true},
			{
				StructName: "Child", Name: "parent_id", Type: "int", Nullable: true,
				Foreign: "parent(id)", ForeignKeyName: "fk_child_parent",
			},
		},
	}
}

// parentOnlyCatalog holds the referenced table and not the one that carries the
// key.
func parentOnlyCatalog() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "parent", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
		},
	}
}

// childOnlyCatalog holds the table that carries the key and not the one it
// references.
func childOnlyCatalog() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "child", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
	}
}

// TestATableDropOrdersAfterTheConstraintOnIt is the other direction of the same
// rule, asserted separately because one edge orientation passing says nothing
// about the other: a removal has to go before the table it depends on, not
// after it.
func TestATableDropOrdersAfterTheConstraintOnIt(t *testing.T) {
	c := qt.New(t)
	parent := "parent"
	column := "id"
	catalog := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "parent", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "child", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []dbschematypes.DBConstraint{{
			Name: "fk_child_parent", TableName: "child", Schema: "public", Type: "FOREIGN KEY",
			ColumnName: "parent_id", ColumnNames: []string{"parent_id"},
			ForeignTable: &parent, ForeignSchema: "public", ForeignColumn: &column,
		}},
	}

	forward := forwardOrderFor(c, &goschema.Database{}, catalog, "table", "constraint")

	c.Assert(kindsOf(forward), qt.DeepEquals, []string{"constraint", "table", "table"})
	c.Assert(forward[0].ID.Name.Source, qt.Equals, "fk_child_parent")
}

// forwardOrderFor runs the adapters, normalization, comparison and graph for one
// input pair and returns the order the changes must be applied in.
//
// inputKinds is the order the changes are handed to the graph in, and every
// caller passes the REVERSE of what it expects back. Compare walks the state's
// objects in insertion order, which already groups tables and constraints, so
// an assertion about the output order of an input that arrived in that order
// would pass with no edges recorded at all -- it would be measuring Compare's
// loop rather than the graph.
func forwardOrderFor(
	c *qt.C,
	description *goschema.Database,
	catalog *dbschematypes.DBSchema,
	inputKinds ...string,
) []schemachange.Change {
	c.Helper()
	profile := postgresProfile()
	desired, err := schemastate.FromDescription(description, profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	current, err := schemastate.FromCatalog(catalog, profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	normalizedDesired, err := schemastate.Normalize(desired, profile)
	c.Assert(err, qt.IsNil)
	normalizedCurrent, err := schemastate.Normalize(current, profile)
	c.Assert(err, qt.IsNil)
	changes, err := schemachange.Compare(normalizedCurrent, normalizedDesired, profile)
	c.Assert(err, qt.IsNil)
	slices.SortStableFunc(changes, func(a, b schemachange.Change) int {
		return slices.Index(inputKinds, string(a.ID.Kind)) - slices.Index(inputKinds, string(b.ID.Kind))
	})
	graph, err := schemachange.BuildGraph(changes, normalizedCurrent, normalizedDesired)
	c.Assert(err, qt.IsNil)
	forward, err := graph.Forward()
	c.Assert(err, qt.IsNil)
	return forward
}

// kindsOf names the family of each change in order, which is what an ordering
// assertion is about.
func kindsOf(changes []schemachange.Change) []string {
	kinds := make([]string, 0, len(changes))
	for _, change := range changes {
		kinds = append(kinds, string(change.ID.Kind))
	}
	return kinds
}

// TestADeclaredTypeIsAskedInTheTargetsSpelling pins the asymmetric half of the
// fold, which a PostgreSQL row cannot reach: there the declared and catalog
// spellings mostly agree already.
//
// Oracle has no counterpart for most declared type names -- a declared TEXT is
// a CLOB, an INT is a NUMBER(10) -- so the question is not "are these the same
// word" but "would rendering this declaration produce the type the catalog
// holds". Comparing them as words reported an ALTER for every column of a
// database Ptah had just built from that declaration.
func TestADeclaredTypeIsAskedInTheTargetsSpelling(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		reported string
	}{
		{name: "text is a CLOB", declared: "text", reported: "CLOB"},
		{name: "int is a NUMBER(10)", declared: "int", reported: "NUMBER(10)"},
		{name: "boolean is a NUMBER(1)", declared: "boolean", reported: "NUMBER(1)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesForProfile(c, describedTable(goschema.Field{
				StructName: "Widget", Name: "code", Type: test.declared, Nullable: true,
			}), catalogTableInSchema("", dbschematypes.DBColumn{
				Name: "code", DataType: test.reported, IsNullable: "YES",
			}), oracleProfile())

			c.Assert(changes, qt.HasLen, 0)
		})
	}
}

// TestADefaultChangeIsAChange pins the third property [changedColumnProperties]
// reads. A comparison that read the type and the nullability only would satisfy
// every other test in this file and would leave a column's default wherever the
// database happened to have it.
func TestADefaultChangeIsAChange(t *testing.T) {
	tests := []struct {
		name     string
		declared goschema.Field
		reported dbschematypes.DBColumn
	}{
		{
			name: "a default the database does not have",
			declared: goschema.Field{
				StructName: "Widget", Name: "code", Type: "text", Nullable: true,
				Default: "unset", DefaultSet: true,
			},
			reported: dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"},
		},
		{
			name: "a different default",
			declared: goschema.Field{
				StructName: "Widget", Name: "code", Type: "text", Nullable: true,
				Default: "unset", DefaultSet: true,
			},
			reported: dbschematypes.DBColumn{
				Name: "code", DataType: "text", IsNullable: "YES", ColumnDefault: new("other"),
			},
		},
		{
			// The empty string is a default, and a model carrying only the
			// string cannot tell it from a column that has none.
			name: "an empty-string default against no default",
			declared: goschema.Field{
				StructName: "Widget", Name: "code", Type: "text", Nullable: true,
				Default: "", DefaultSet: true,
			},
			reported: dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c, describedTable(test.declared), catalogTable(test.reported))

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(changes[0].Operation, qt.Equals, schemachange.Modify)
			c.Assert(changes[0].Changed, qt.DeepEquals, []string{"default"})
		})
	}
}
