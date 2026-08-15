//go:build integration

package mysql_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// liveMySQLAdminURLForPrimaryKeyIdentity returns the server-administration URL.
// Its configured database is used only to create and remove the scratch realm;
// the planner and catalog assertions run through the derived database URL.
func liveMySQLAdminURLForPrimaryKeyIdentity(t *testing.T) string {
	t.Helper()
	dbURL := dbtarget.URL(t, dbtarget.MySQLAdmin)
	if !strings.HasPrefix(dbURL, "mysql://") {
		t.Skip("MySQL URL required for live planner test")
	}
	return dbURL
}

// createPrimaryKeyIdentityDatabase provisions one database per row and drops it
// again afterwards. The shared development server is dirty, and a row that asks
// information_schema which columns carry the primary key needs a table nothing
// else touched.
func createPrimaryKeyIdentityDatabase(tb testing.TB, adminURL string) (dbURL, database string) {
	c := qt.New(tb)
	c.Helper()
	name := fmt.Sprintf("ptah_pkident_%d_%d", os.Getpid(), time.Now().UnixNano())
	admin, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })

	quotedName := sqlident.Quote("mysql", name)
	_, err = admin.ExecContext(context.Background(), "CREATE DATABASE "+quotedName)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, cleanupErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+quotedName)
		c.Check(cleanupErr, qt.IsNil)
	})

	return mySQLURLWithDatabase(c.TB, adminURL, name), name
}

func mySQLURLWithDatabase(tb testing.TB, rawURL, database string) string {
	c := qt.New(tb)
	c.Helper()

	if strings.Contains(rawURL, "@tcp(") {
		scheme, dsn, found := strings.Cut(rawURL, "://")
		c.Assert(found, qt.IsTrue)
		config, err := mysqldriver.ParseDSN(dsn)
		c.Assert(err, qt.IsNil)
		config.DBName = database
		return scheme + "://" + config.FormatDSN()
	}

	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	return parsed.String()
}

// executeMySQL runs every statement in order and fails on the first error.
func executeMySQL(tb testing.TB, dbURL string, statements []string) {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(context.Background(), statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// primaryKeyColumns reports the columns information_schema attributes to the
// table's PRIMARY KEY, in key order.
func primaryKeyColumns(tb testing.TB, dbURL, database, table string) []string {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(),
		`SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
		  WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		  ORDER BY ORDINAL_POSITION`, database, table)
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	found := []string{}
	for rows.Next() {
		var column string
		c.Assert(rows.Scan(&column), qt.IsNil)
		found = append(found, column)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// TestPrimaryKeyIsPlannedOnceLiveMySQL executes the plan rather than reading it.
//
// A PRIMARY KEY that arrives both as a column change and as a table-level
// constraint addition was emitted twice whenever the two sides spelled the
// database differently, because the suppression compared the two names as raw
// text. Both statements are individually valid; the pair is not. Measured on
// MySQL 9.7.1 the second one fails with
// `Error 1068 (42000): Multiple primary key defined` -- and it fails AFTER the
// first has already run, so the migration stops halfway with the key applied by
// the wrong statement.
//
// Only the catalog settles it: this row applies the plan and asks
// information_schema.KEY_COLUMN_USAGE what the table ended up with.
func TestPrimaryKeyIsPlannedOnceLiveMySQL(t *testing.T) {
	c := qt.New(t)
	adminURL := liveMySQLAdminURLForPrimaryKeyIdentity(t)

	tests := []struct {
		name string
		// diffTable and constraintTable spell the same table as the TableDiff
		// and as the constraint addition respectively; databasePlaceholder
		// stands in for the throwaway database each row creates. The defect is
		// exactly the row where the two spellings disagree.
		diffTable       string
		constraintTable string
	}{
		{
			// Control: both sides already agree.
			name:            "both sides name the database",
			diffTable:       databasePlaceholder + ".orders",
			constraintTable: databasePlaceholder + ".orders",
		},
		{
			name:            "the diff names the database and the constraint does not",
			diffTable:       databasePlaceholder + ".orders",
			constraintTable: "orders",
		},
		{
			name:            "the constraint names the database and the diff does not",
			diffTable:       "orders",
			constraintTable: databasePlaceholder + ".orders",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dbURL, database := createPrimaryKeyIdentityDatabase(c.TB, adminURL)
			executeMySQL(c.TB, dbURL, []string{
				"CREATE TABLE `orders` (`id` INT NOT NULL, `note` TEXT)",
			})
			c.Assert(primaryKeyColumns(c.TB, dbURL, database, "orders"), qt.DeepEquals, []string{})

			generated := &goschema.Database{
				Tables: []goschema.Table{{StructName: "Order", Name: "orders", Schema: database}},
				Fields: []goschema.Field{
					{StructName: "Order", Name: "id", Type: "INT", Primary: true},
					{StructName: "Order", Name: "note", Type: "TEXT"},
				},
			}
			diff := &types.SchemaDiff{
				TablesModified: []types.TableDiff{{
					TableName: withDatabase(test.diffTable, database),
					ColumnsModified: []types.ColumnDiff{{
						ColumnName: "id",
						Changes:    map[string]string{"primary_key": "false -> true"},
					}},
				}},
				ConstraintsAdded: []string{"pk_orders"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
					Name:      "pk_orders",
					TableName: withDatabase(test.constraintTable, database),
					Type:      "PRIMARY KEY",
					Columns:   []string{"id"},
				}},
			}

			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, "mysql")
			c.Assert(err, qt.IsNil)
			c.Logf("plan:\n%s", strings.Join(statements, "\n"))
			executeMySQL(c.TB, dbURL, statements)

			c.Assert(primaryKeyColumns(c.TB, dbURL, database, "orders"), qt.DeepEquals, []string{"id"})
		})
	}
}

// databasePlaceholder stands in for the throwaway database name a row cannot
// know until it has been created.
const databasePlaceholder = "{db}"

// withDatabase fills a row's spelling in with the database it created.
func withDatabase(spelling, database string) string {
	return strings.ReplaceAll(spelling, databasePlaceholder, database)
}
