//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestOracleTablePrimaryKeyConvergesE2E pins that a TABLE-level primary key is
// not planned again once it exists.
//
// It is the path an HCL document takes -- the `column` block carries no
// `primary` attribute, so `primary_key { columns = [...] }` is the only way to
// declare one -- and it was the one path nothing measured.
// TestOracleDeclarationConvergesE2E passes on the same server and declares its
// keys on the FIELD, which renders inline in CREATE TABLE and is compared
// through `column.IsPrimaryKey`.
//
// Measured on Oracle Free 23 before the fix: applying one document twice
// answered `ORA-02260: table can have only one primary key`. The migration
// stopped, rather than doing needless work like every other convergence defect
// in this family (stokaro/ptah#2057).
//
// The cause is a reader that is right on its own terms: Oracle names an
// undeclared key itself -- `SYS_C008646` -- so the reader drops the row after
// copying `IsPrimaryKey` onto the columns (stokaro/ptah#1890), and a
// table-level declaration then had nothing to compare against.
func TestOracleTablePrimaryKeyConvergesE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ora_pk_%d", time.Now().UnixNano()%100000000)
	dropOracleTable(ctx, conn, table)
	defer dropOracleTable(context.WithoutCancel(ctx), conn, table)

	c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id NUMBER(10) NOT NULL, code VARCHAR2(40) NOT NULL, `+
			`qty NUMBER(8) NOT NULL, PRIMARY KEY (id))`,
		table)), qt.IsNil)

	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// The premise, and the reason a unit test could not hold this: the read
	// carries the key on the COLUMN and not as a constraint row.
	c.Assert(oraclePrimaryKeyConstraintCount(read, table), qt.Equals, 0)
	c.Assert(oraclePrimaryKeyColumns(read, table), qt.DeepEquals, []string{"ID"})

	diff, err := schemadiff.CompareWithDatabase(
		ctx, conn, oraclePrimaryKeyDeclaration(table, []string{"id"}),
		read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)

	// The control: a key on a DIFFERENT column is a real change and is still
	// reported, so the silence above is not a comparison that stopped looking.
	changed, err := schemadiff.CompareWithDatabase(
		ctx, conn, oraclePrimaryKeyDeclaration(table, []string{"code"}),
		read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(changed.ConstraintsAdded, qt.Not(qt.HasLen), 0)
}

// oraclePrimaryKeyDeclaration declares the table with a TABLE-level key, the
// way an HCL `primary_key` block does.
func oraclePrimaryKeyDeclaration(table string, keyColumns []string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "K", Name: table, PrimaryKeyParts: primaryKeyParts(keyColumns),
		}},
		Fields: []schemamodel.Field{
			{StructName: "K", Name: "id", Type: "NUMBER(10)"},
			{StructName: "K", Name: "code", Type: "VARCHAR2(40)"},
			// Declared with the scale Oracle does not keep. `NUMBER(8,0)` and
			// `NUMBER(8)` are one type and the catalog reports the second, so
			// this column is the live half of the second defect in #2057.
			{StructName: "K", Name: "qty", Type: "NUMBER(8,0)"},
		},
	}
}

// primaryKeyParts spells a column list the way a table-level key carries it.
func primaryKeyParts(columns []string) []schemamodel.PrimaryKeyPart {
	parts := make([]schemamodel.PrimaryKeyPart, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, schemamodel.PrimaryKeyPart{Name: column})
	}
	return parts
}

// oraclePrimaryKeyConstraintCount counts the PRIMARY KEY rows a read carries
// for one table.
func oraclePrimaryKeyConstraintCount(schema *catalog.Database, table string) int {
	count := 0
	for _, constraint := range schema.Constraints {
		if constraint.Type == "PRIMARY KEY" && strings.EqualFold(constraint.TableName, table) {
			count++
		}
	}
	return count
}

// oraclePrimaryKeyColumns names the columns a read marked as the key.
func oraclePrimaryKeyColumns(schema *catalog.Database, table string) []string {
	var columns []string
	for _, dbTable := range schema.Tables {
		if !strings.EqualFold(dbTable.Name, table) {
			continue
		}
		for _, column := range dbTable.Columns {
			if column.IsPrimaryKey {
				columns = append(columns, column.Name)
			}
		}
	}
	return columns
}

// dropOracleTable removes what this test creates, and says nothing when there
// is nothing to remove.
func dropOracleTable(ctx context.Context, conn *dbschema.DatabaseConnection, table string) {
	_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP TABLE "+table+" CASCADE CONSTRAINTS")
}
