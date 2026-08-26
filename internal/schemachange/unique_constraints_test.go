package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemachange"
)

// A uniqueness guarantee was an object the foreign-key gate could read and
// nothing could plan: the comparator skipped a constraint object whose
// ForeignKey was nil, so a UNIQUE constraint a schema declared and a database
// lacked produced no change at all (stokaro/ptah#1663).

func TestAUniqueConstraintIsPlanned(t *testing.T) {
	tests := []struct {
		name           string
		description    *schemamodel.Database
		currentCatalog *catalog.Database
		wantOperation  schemachange.Operation
		wantRisk       schemachange.Risk
		wantChanged    []string
	}{
		{
			name:           "declared and absent from the database",
			description:    scopedWidget([]string{"tenant", "code"}),
			currentCatalog: scopedWidgetCatalog(nil),
			wantOperation:  schemachange.Add,
			wantRisk:       schemachange.RiskDataDependent,
		},
		{
			name:           "present and no longer declared",
			description:    scopedWidget(nil),
			currentCatalog: scopedWidgetCatalog([]string{"tenant", "code"}),
			wantOperation:  schemachange.Remove,
			// Dropping one destroys a guarantee rather than data.
			wantRisk: schemachange.RiskGuaranteeLoss,
		},
		{
			name:           "declared over different columns",
			description:    scopedWidget([]string{"tenant"}),
			currentCatalog: scopedWidgetCatalog([]string{"tenant", "code"}),
			wantOperation:  schemachange.Modify,
			wantRisk:       schemachange.RiskDataDependent,
			wantChanged:    []string{"columns"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c, test.description, test.currentCatalog)

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(changes[0].Operation, qt.Equals, test.wantOperation)
			c.Assert(changes[0].Risk, qt.Equals, test.wantRisk)
			c.Assert(changes[0].Changed, qt.DeepEquals, test.wantChanged)
			c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
			c.Assert(changes[0].ID.Name.Source, qt.Equals, "uq_widget_scope")
		})
	}
}

// TestAGuaranteeInTheColumnSyntaxIsNotAConstraintChange is the control the rows
// above need, and the reason UniqueKey has a Standalone flag at all.
//
// A column's own UNIQUE flag, a primary key and a unique index are all
// uniqueness guarantees and all become UniqueKey objects, because all three
// answer the question a foreign key asks. None of them is added or dropped by
// the statement this family plans: the flag renders beside its column, so
// planning it here would declare the same guarantee twice.
func TestAGuaranteeInTheColumnSyntaxIsNotAConstraintChange(t *testing.T) {
	tests := []struct {
		name        string
		description *schemamodel.Database
		// wantKinds is every family the row plans, in order. A unique INDEX
		// plans two changes -- the table and the index -- and the point of the
		// row is that neither of them is a CONSTRAINT change.
		wantKinds []string
	}{
		{
			name: "a column's own flag",
			description: describedTable(
				schemamodel.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				schemamodel.Field{
					StructName: "Widget", Name: "code", Type: "text", Nullable: true, Unique: true,
				}),
			wantKinds: []string{"table"},
		},
		{
			name: "a table-level primary key",
			description: describedTableWithKey([]string{"id", "code"},
				schemamodel.Field{StructName: "Widget", Name: "id", Type: "int"},
				schemamodel.Field{StructName: "Widget", Name: "code", Type: "text"}),
			wantKinds: []string{"table"},
		},
		{
			name: "a unique index",
			description: withUniqueIndex(describedTable(
				schemamodel.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				schemamodel.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
			), "idx_widget_code", "code"),
			// The index is its own object and its own change. What the row
			// asserts is that it is not ALSO a constraint change: the
			// guarantee would then be declared twice.
			wantKinds: []string{"table", "index"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			// The whole table is created, and a declared index is created
			// beside it. What must NOT appear is a constraint change: this
			// family planning a guarantee the CREATE already carries would
			// declare it twice.
			changes := changesFor(c, test.description, &catalog.Database{})

			c.Assert(kindsOf(changes), qt.DeepEquals, test.wantKinds)
		})
	}
}

// scopedWidget is a table whose named UNIQUE constraint covers the given
// columns, or which declares none when they are absent.
func scopedWidget(columns []string) *schemamodel.Database {
	description := describedTable(
		schemamodel.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
		schemamodel.Field{StructName: "Widget", Name: "tenant", Type: "int", Nullable: true},
		schemamodel.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
	return withUniqueConstraint(description, columns)
}

// withUniqueConstraint adds the named guarantee, or none for an empty list.
func withUniqueConstraint(description *schemamodel.Database, columns []string) *schemamodel.Database {
	return map[bool]func() *schemamodel.Database{
		true: func() *schemamodel.Database { return description },
		false: func() *schemamodel.Database {
			description.Constraints = append(description.Constraints, schemamodel.Constraint{
				StructName: "Widget", Name: "uq_widget_scope", Type: "UNIQUE", Columns: columns,
			})
			return description
		},
	}[len(columns) == 0]()
}

// withUniqueIndex adds a unique index over one column.
func withUniqueIndex(description *schemamodel.Database, name, column string) *schemamodel.Database {
	description.Indexes = append(description.Indexes, schemamodel.Index{
		StructName: "Widget", Name: name, Fields: []string{column}, Unique: true,
	})
	return description
}

// scopedWidgetCatalog is that table as a currentCatalog read reports it, with the
// guarantee as a constraint row when the database holds one.
func scopedWidgetCatalog(columns []string) *catalog.Database {
	currentCatalog := catalogTable(
		catalog.Column{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		catalog.Column{Name: "tenant", DataType: "integer", IsNullable: "YES"},
		catalog.Column{Name: "code", DataType: "text", IsNullable: "YES"},
	)
	return withCatalogUniqueConstraint(currentCatalog, columns)
}

func withCatalogUniqueConstraint(
	currentCatalog *catalog.Database,
	columns []string,
) *catalog.Database {
	return map[bool]func() *catalog.Database{
		true: func() *catalog.Database { return currentCatalog },
		false: func() *catalog.Database {
			currentCatalog.Constraints = append(currentCatalog.Constraints, catalog.Constraint{
				Name: "uq_widget_scope", TableName: "widget", Schema: "public",
				Type: "UNIQUE", ColumnNames: columns,
			})
			return currentCatalog
		},
	}[len(columns) == 0]()
}

// TestAChangedUniqueConstraintDropsBeforeItAdds is the one shape where the
// canonical path deliberately does NOT match the existing planner, and the
// reason is measured rather than argued.
//
// The existing comparator has no Modify: it reports a changed constraint as a
// removal and an addition of one name, and the planner emits the addition
// first. Measured on PostgreSQL 17, against a table whose `uq_widget_scope`
// covers (tenant, code):
//
//	ALTER TABLE widget ADD CONSTRAINT uq_widget_scope UNIQUE (tenant);
//	  ERROR: relation "uq_widget_scope" already exists
//	ALTER TABLE widget DROP CONSTRAINT IF EXISTS uq_widget_scope;
//	  ALTER TABLE
//
// The name belongs to the backing index too, so the add cannot precede the
// drop. Inside a transaction the failure aborts and the change never applies;
// outside one the drop still runs and the table is left with no unique
// constraint at all. Reversing the pair applies cleanly, measured the same way.
//
// A change that carries both halves cannot be ordered wrongly by a later stage,
// which is what the Modify operation is for. The shipping path's version of the
// same defect is filed as stokaro/ptah#1987: it pairs the two halves by the
// table NAME, and a description that leaves the schema off spells it `widget`
// where the currentCatalog spells it `public.widget`, so the pairing misses.
func TestAChangedUniqueConstraintDropsBeforeItAdds(t *testing.T) {
	c := qt.New(t)

	operations, err := schemachange.Plan(
		changesFor(c, scopedWidget([]string{"tenant"}), scopedWidgetCatalog([]string{"tenant", "code"})),
		postgresProfile())

	c.Assert(err, qt.IsNil)
	c.Assert(schemachange.Statements(operations), qt.HasLen, 2)
	c.Assert(schemachange.Statements(operations)[0], qt.Contains, "DROP CONSTRAINT")
	c.Assert(schemachange.Statements(operations)[1], qt.Contains, "ADD CONSTRAINT")
	// Both statements belong to ONE change, so nothing downstream can separate
	// them or reorder them.
	c.Assert(operations[0].Change.ID.Key(), qt.Equals, operations[1].Change.ID.Key())
	c.Assert(operations[0].Change.Operation, qt.Equals, schemachange.Modify)
}

// TestAUniqueConstraintInAnUndescribedSchemaIsNotDropped is the coverage gate on
// this family, and it is the same rule the table family carries: a description
// that declined a whole schema is silent about it rather than empty, and reading
// that silence as absence turns a partial read into a drop (stokaro/ptah#1276).
func TestAUniqueConstraintInAnUndescribedSchemaIsNotDropped(t *testing.T) {
	c := qt.New(t)
	description := scopedWidget(nil)
	description.NotDescribed = coverage.Set{}.WithObject(coverage.Schema, "public")

	changes := changesFor(c, description, scopedWidgetCatalog([]string{"tenant", "code"}))

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Undecidable)
	c.Assert(changes[0].Diagnostic, qt.Contains, "is not a request to drop it")
}
