package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// ordersTable declares one `orders` table whose schema is spelled as given.
func ordersTable(tableSchema string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Order", Name: "orders", Schema: tableSchema}},
		Fields: []goschema.Field{
			{StructName: "Order", Name: "id", Type: "INT", Primary: true},
			{StructName: "Order", Name: "note", Type: "TEXT"},
		},
	}
}

// ordersTableOneColumn is the same table with a single column.
//
// SQL Server's offline identifier semantics compare every name as
// ComparisonCatalogUnknown, so two unresolved column names in one table are
// reported as a possible catalog collision and the diff is refused before any
// planning happens. That guard belongs to stokaro/ptah#1290 and is not what
// these rows measure, so the SQL Server row carries one column.
func ordersTableOneColumn(tableSchema string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Order", Name: "orders", Schema: tableSchema}},
		Fields: []goschema.Field{{StructName: "Order", Name: "note", Type: "TEXT"}},
	}
}

// TestColumnDDLResolvesTheTableAcrossSchemaSpellings pins mysql's
// findGeneratedTable at both of its column-DDL call sites.
//
// This is the only table lookup SQL Server inherits -- its planner IS the
// MySQL-family planner under NewForDialect -- so the row that names `sqlserver`
// is not decoration. Reverting the function to the raw `QualifiedName() ==
// tableName` loop left the whole suite green: `addNewTableColumns` emits
// nothing at all for an unresolved table (no statement, no comment) and
// `modifyExistingColumns` degrades to an `ERROR: Could not find field
// definition` comment that applies cleanly and changes nothing.
func TestColumnDDLResolvesTheTableAcrossSchemaSpellings(t *testing.T) {

	tests := []struct {
		name      string
		dialect   string
		generated *goschema.Database
		diffName  string
		// wantAdd is the column-addition fragment the target's renderer emits.
		// T-SQL spells it `ADD [note]` where the MySQL family spells it
		// `ADD COLUMN`, and that is the renderer's business, not the lookup's.
		wantAdd string
	}{
		{
			// Control: both sides already agree.
			name:      "MySQL with both sides spelling the table the same way",
			dialect:   "mysql",
			generated: ordersTable("app"),
			diffName:  "app.orders",
			wantAdd:   "ADD COLUMN",
		},
		{
			// MySQL's schema IS its database, so identifier.ForDialect leaves
			// DefaultSchema empty and only the unqualified tier can join these.
			name:      "the diff names the database and the declaration does not",
			dialect:   "mysql",
			generated: ordersTable(""),
			diffName:  "app.orders",
			wantAdd:   "ADD COLUMN",
		},
		{
			name:      "the declaration names the database and the diff does not",
			dialect:   "mysql",
			generated: ordersTable("app"),
			diffName:  "orders",
			wantAdd:   "ADD COLUMN",
		},
		{
			name:      "MariaDB resolves it the same way",
			dialect:   "mariadb",
			generated: ordersTable(""),
			diffName:  "app.orders",
			wantAdd:   "ADD COLUMN",
		},
		{
			// SQL Server carries DefaultSchema `dbo`, so this pair is one object
			// at the identity tier rather than the unqualified one.
			name:      "SQL Server resolves a bare declaration against dbo",
			dialect:   "sqlserver",
			generated: ordersTableOneColumn(""),
			diffName:  "dbo.orders",
			wantAdd:   "ADD [note]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			added, err := planner.GenerateSchemaDiffSQLStatements(
				&types.SchemaDiff{TablesModified: []types.TableDiff{{
					TableName:    test.diffName,
					ColumnsAdded: []string{"note"},
				}}},
				test.generated,
				test.dialect,
			)
			c.Assert(err, qt.IsNil)
			addedPlan := strings.Join(added, "\n")
			c.Assert(addedPlan, qt.Contains, test.wantAdd, qt.Commentf("plan:\n%s", addedPlan))

			modified, err := planner.GenerateSchemaDiffSQLStatements(
				&types.SchemaDiff{TablesModified: []types.TableDiff{{
					TableName: test.diffName,
					ColumnsModified: []types.ColumnDiff{{
						ColumnName: "note",
						Changes:    map[string]string{"type": "VARCHAR(10) -> TEXT"},
					}},
				}}},
				test.generated,
				test.dialect,
			)
			c.Assert(err, qt.IsNil)
			modifiedPlan := strings.Join(modified, "\n")
			c.Assert(modifiedPlan, qt.Not(qt.Contains), "Could not find field definition",
				qt.Commentf("plan:\n%s", modifiedPlan))
		})
	}
}

// TestColumnDDLDoesNotGuessBetweenSchemas is the control the widened match must
// not swallow. A table declared in one schema is not the table a diff names in
// another, and the statement would be rendered against the DIFF's name -- so a
// lookup that crossed schemas writes the column onto a relation the desired
// schema never declared.
func TestColumnDDLDoesNotGuessBetweenSchemas(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(
		&types.SchemaDiff{TablesModified: []types.TableDiff{{
			TableName:    "app.orders",
			ColumnsAdded: []string{"note"},
		}}},
		ordersTable("reporting"),
		"mysql",
	)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "ADD COLUMN")
}

// TestPrimaryKeyIsPlannedOnceAcrossSchemaSpellings pins
// primaryKeyColumnChangeOwnedByTableConstraint.
//
// A PRIMARY KEY that arrives BOTH as a column change and as a table-level
// constraint addition must be emitted once. The suppression asked
// `info.TableName == tableName`, and those two are the sources the sweep's own
// SQLite comment already names as divergent: ConstraintAdditionInfo.TableName
// follows the constraint declaration, TableDiff.TableName follows
// genTable.QualifiedName(). Where they disagreed the plan carried
// `ALTER TABLE app.orders MODIFY COLUMN id INT PRIMARY KEY` AND
// `ALTER TABLE orders ADD PRIMARY KEY (id)`; measured on MySQL 9.7.1 the second
// fails with `ERROR 1068 (42000): Multiple primary key defined` and the
// migration aborts partway through.
func TestPrimaryKeyIsPlannedOnceAcrossSchemaSpellings(t *testing.T) {

	tests := []struct {
		name            string
		tableSchema     string
		diffTableName   string
		constraintTable string
	}{
		{
			// Control: both sides already agree, and the plan is what it has
			// always been.
			name:            "both sides spell the table the same way",
			tableSchema:     "app",
			diffTableName:   "app.orders",
			constraintTable: "app.orders",
		},
		{
			name:            "the diff names the database and the constraint does not",
			tableSchema:     "app",
			diffTableName:   "app.orders",
			constraintTable: "orders",
		},
		{
			name:            "the constraint names the database and the diff does not",
			tableSchema:     "app",
			diffTableName:   "orders",
			constraintTable: "app.orders",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &types.SchemaDiff{
				TablesModified: []types.TableDiff{{
					TableName: test.diffTableName,
					ColumnsModified: []types.ColumnDiff{{
						ColumnName: "id",
						Changes:    map[string]string{"primary_key": "false -> true"},
					}},
				}},
				ConstraintsAdded: []string{"pk_orders"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
					Name:      "pk_orders",
					TableName: test.constraintTable,
					Type:      "PRIMARY KEY",
					Columns:   []string{"id"},
				}},
			}
			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, ordersTable(test.tableSchema), "mysql")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(strings.Count(plan, "PRIMARY KEY"), qt.Equals, 1, qt.Commentf("plan:\n%s", plan))
			c.Assert(plan, qt.Contains, "ADD PRIMARY KEY", qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestPrimaryKeyOwnershipDoesNotCrossSchemas is the control for the same
// suppression: a PRIMARY KEY constraint added to a DIFFERENT table must not
// suppress this table's inline key, or the column change is dropped and the
// table never gets a primary key at all.
func TestPrimaryKeyOwnershipDoesNotCrossSchemas(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName: "app.orders",
			ColumnsModified: []types.ColumnDiff{{
				ColumnName: "id",
				Changes:    map[string]string{"primary_key": "false -> true"},
			}},
		}},
		ConstraintsAdded: []string{"pk_orders"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name:      "pk_orders",
			TableName: "reporting.orders",
			Type:      "PRIMARY KEY",
			Columns:   []string{"id"},
		}},
	}
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, ordersTable("app"), "mysql")
	c.Assert(err, qt.IsNil)
	plan := strings.Join(statements, "\n")
	c.Assert(plan, qt.Contains, "MODIFY COLUMN `id` INT PRIMARY KEY", qt.Commentf("plan:\n%s", plan))
}
