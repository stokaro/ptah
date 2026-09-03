//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver for database/sql

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// sameNamedConstraintsSchema is a table both engines accept and Ptah could not
// read: a UNIQUE and a foreign key sharing one name. One statement per entry,
// because execMySQL sends one and the driver refuses a batch.
var sameNamedConstraintsSchema = []string{
	`CREATE TABLE p (id INT PRIMARY KEY);`,
	`CREATE TABLE c (
  a INT,
  CONSTRAINT same UNIQUE (a),
  CONSTRAINT same FOREIGN KEY (a) REFERENCES p(id)
);`,
}

// TestMySQLSameNamedConstraintsReadLive is stokaro/ptah#2774 asked of the
// servers.
//
// Both engines accept a UNIQUE and a foreign key under one name on one table,
// and their catalogs report two constraints. Ptah's read joined
// TABLE_CONSTRAINTS to KEY_COLUMN_USAGE and REFERENTIAL_CONSTRAINTS on schema,
// table and name without a type discriminator, so the join was a cross product:
// the unique constraint came back over four copies of its one column, and the
// foreign key was gone.
//
// The column count is the assertion that catches the cross product. A test
// asserting only that two constraints came back would pass against a read that
// quadrupled every column list.
func TestMySQLSameNamedConstraintsReadLive(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, test.engine))
			c.Assert(err, qt.IsNil)
			defer adminDB.Close()
			c.Assert(adminDB.PingContext(ctx), qt.IsNil)

			name := fmt.Sprintf("ptah_same_%d", time.Now().UnixNano())
			createMySQLDatabase(c, ctx, adminDB, name)
			defer dropMySQLDatabase(c, context.Background(), adminDB, name)
			for _, statement := range sameNamedConstraintsSchema {
				execMySQL(c, ctx, adminDB, name, statement)
			}

			conn, err := dbschema.ConnectToDatabase(
				ctx, replaceMySQLDatabaseName(c, dbtarget.URL(c, test.engine), name))
			c.Assert(err, qt.IsNil)
			defer conn.Close()

			live, err := conn.Reader().ReadSchemaContext(ctx)
			c.Assert(err, qt.IsNil)

			c.Assert(constraintColumnsByType(live, "c", "same", "UNIQUE"), qt.DeepEquals, []string{"a"})
			c.Assert(
				constraintColumnsByType(live, "c", "same", "FOREIGN KEY"),
				qt.DeepEquals,
				[]string{"a"},
			)
		})
	}
}

// constraintColumnsByType answers one named constraint's columns, and nil where
// the read produced no constraint of that type under that name.
func constraintColumnsByType(live *catalog.Database, table, name, kind string) []string {
	for _, constraint := range live.Constraints {
		if constraint.TableName != table || constraint.Name != name || constraint.Type != kind {
			continue
		}
		return constraint.ColumnNamesOrDefault()
	}
	return nil
}
