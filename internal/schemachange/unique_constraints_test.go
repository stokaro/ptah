package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemachange"
)

// A uniqueness guarantee was an object the foreign-key gate could read and
// nothing could plan: the comparator skipped a constraint object whose
// ForeignKey was nil, so a UNIQUE constraint a schema declared and a database
// lacked produced no change at all (stokaro/ptah#1663).

func TestAUniqueConstraintIsPlanned(t *testing.T) {
	tests := []struct {
		name          string
		description   *goschema.Database
		catalog       *dbschematypes.DBSchema
		wantOperation schemachange.Operation
		wantRisk      schemachange.Risk
		wantChanged   []string
	}{
		{
			name:          "declared and absent from the database",
			description:   scopedWidget([]string{"tenant", "code"}),
			catalog:       scopedWidgetCatalog(nil),
			wantOperation: schemachange.Add,
			wantRisk:      schemachange.RiskDataDependent,
		},
		{
			name:          "present and no longer declared",
			description:   scopedWidget(nil),
			catalog:       scopedWidgetCatalog([]string{"tenant", "code"}),
			wantOperation: schemachange.Remove,
			// Dropping one destroys a guarantee rather than data.
			wantRisk: schemachange.RiskGuaranteeLoss,
		},
		{
			name:          "declared over different columns",
			description:   scopedWidget([]string{"tenant"}),
			catalog:       scopedWidgetCatalog([]string{"tenant", "code"}),
			wantOperation: schemachange.Modify,
			wantRisk:      schemachange.RiskDataDependent,
			wantChanged:   []string{"columns"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c, test.description, test.catalog)

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
		description *goschema.Database
		// wantKinds is every family the row plans, in order. A unique INDEX
		// plans two changes -- the table and the index -- and the point of the
		// row is that neither of them is a CONSTRAINT change.
		wantKinds []string
	}{
		{
			name: "a column's own flag",
			description: describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{
					StructName: "Widget", Name: "code", Type: "text", Nullable: true, Unique: true,
				}),
			wantKinds: []string{"table"},
		},
		{
			name: "a table-level primary key",
			description: describedTableWithKey([]string{"id", "code"},
				goschema.Field{StructName: "Widget", Name: "id", Type: "int"},
				goschema.Field{StructName: "Widget", Name: "code", Type: "text"}),
			wantKinds: []string{"table"},
		},
		{
			name: "a unique index",
			description: withUniqueIndex(describedTable(
				goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
				goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
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
			changes := changesFor(c, test.description, &dbschematypes.DBSchema{})

			c.Assert(kindsOf(changes), qt.DeepEquals, test.wantKinds)
		})
	}
}

// scopedWidget is a table whose named UNIQUE constraint covers the given
// columns, or which declares none when they are absent.
func scopedWidget(columns []string) *goschema.Database {
	description := describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
		goschema.Field{StructName: "Widget", Name: "tenant", Type: "int", Nullable: true},
		goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
	return withUniqueConstraint(description, columns)
}

// withUniqueConstraint adds the named guarantee, or none for an empty list.
func withUniqueConstraint(description *goschema.Database, columns []string) *goschema.Database {
	return map[bool]func() *goschema.Database{
		true: func() *goschema.Database { return description },
		false: func() *goschema.Database {
			description.Constraints = append(description.Constraints, goschema.Constraint{
				StructName: "Widget", Name: "uq_widget_scope", Type: "UNIQUE", Columns: columns,
			})
			return description
		},
	}[len(columns) == 0]()
}

// withUniqueIndex adds a unique index over one column.
func withUniqueIndex(description *goschema.Database, name, column string) *goschema.Database {
	description.Indexes = append(description.Indexes, goschema.Index{
		StructName: "Widget", Name: name, Fields: []string{column}, Unique: true,
	})
	return description
}

// scopedWidgetCatalog is that table as a catalog read reports it, with the
// guarantee as a constraint row when the database holds one.
func scopedWidgetCatalog(columns []string) *dbschematypes.DBSchema {
	catalog := catalogTable(
		dbschematypes.DBColumn{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		dbschematypes.DBColumn{Name: "tenant", DataType: "integer", IsNullable: "YES"},
		dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"},
	)
	return withCatalogUniqueConstraint(catalog, columns)
}

func withCatalogUniqueConstraint(
	catalog *dbschematypes.DBSchema,
	columns []string,
) *dbschematypes.DBSchema {
	return map[bool]func() *dbschematypes.DBSchema{
		true: func() *dbschematypes.DBSchema { return catalog },
		false: func() *dbschematypes.DBSchema {
			catalog.Constraints = append(catalog.Constraints, dbschematypes.DBConstraint{
				Name: "uq_widget_scope", TableName: "widget", Schema: "public",
				Type: "UNIQUE", ColumnNames: columns,
			})
			return catalog
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
// where the catalog spells it `public.widget`, so the pairing misses.
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
