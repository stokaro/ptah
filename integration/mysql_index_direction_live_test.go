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

// TestMySQLIndexDirectionIsReadLive is stokaro/ptah#2816.
//
// The index read selected `SUB_PART` and not `COLLATION`, so every key part
// arrived ascending and `KEY (a DESC)` was indistinguishable from `KEY (a)`.
// Both engines report the difference -- `information_schema.STATISTICS.COLLATION`
// is 'A' or 'D' -- and both build different indexes for the two declarations,
// so a schema read back from a live server described one the server did not
// have.
//
// The table is written by hand rather than by Ptah. A table Ptah created would
// round trip through whatever the renderer writes, and a reader defect stays
// invisible against a fixture the renderer wrote.
//
// The ascending key is the control and it is not decoration: a reader that
// marked EVERY part descending would satisfy the first assertion alone, and
// that is a one-character mistake in the same expression.
func TestMySQLIndexDirectionIsReadLive(t *testing.T) {
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

			name := fmt.Sprintf("ptah_idx_dir_%d", time.Now().UnixNano())
			createMySQLDatabase(c, ctx, adminDB, name)
			defer dropMySQLDatabase(c, context.Background(), adminDB, name)

			_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
				"CREATE TABLE `%s`.`d` (`a` INT, `b` INT, "+
					"KEY `k_desc` (`a` DESC), KEY `k_asc` (`b`), "+
					"KEY `k_mixed` (`a` ASC, `b` DESC))", name))
			c.Assert(err, qt.IsNil)

			// What the catalog says, before Ptah is asked. Without it the
			// assertions below could agree with a server that stored something
			// else, and the test would pass while measuring nothing.
			c.Assert(mysqlKeyPartCollations(c, ctx, adminDB, name), qt.DeepEquals, []string{
				"k_asc/1/b=A", "k_desc/1/a=D", "k_mixed/1/a=A", "k_mixed/2/b=D",
			})

			conn, err := dbschema.ConnectToDatabase(ctx,
				replaceMySQLDatabaseName(c, dbtarget.URL(t, test.engine), name))
			c.Assert(err, qt.IsNil)
			defer conn.Close()

			read, err := conn.Reader().ReadSchemaContext(ctx)
			c.Assert(err, qt.IsNil)

			c.Assert(indexPartDirections(c, read.Indexes, "k_desc"), qt.DeepEquals, []bool{true})
			c.Assert(indexPartDirections(c, read.Indexes, "k_mixed"), qt.DeepEquals, []bool{false, true})
			// The ascending key carries no Parts at all, which is what the
			// reader does for a key whose parts say nothing its Columns do not.
			// Asserting the absence rather than a list of falses is the honest
			// shape: a Parts list of two falses would be a different answer.
			c.Assert(indexNamed(c, read.Indexes, "k_asc").Parts, qt.HasLen, 0)
		})
	}
}

// mysqlKeyPartCollations is what the catalog says the directions are.
func mysqlKeyPartCollations(c *qt.C, ctx context.Context, db *sql.DB, schema string) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, COLLATION
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'd'
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`, schema)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var found []string
	for rows.Next() {
		var indexName, columnName string
		var sequence int
		var collation sql.NullString
		c.Assert(rows.Scan(&indexName, &sequence, &columnName, &collation), qt.IsNil)
		found = append(found, fmt.Sprintf("%s/%d/%s=%s",
			indexName, sequence, columnName, collation.String))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// indexPartDirections is the Desc flag of each part of one index, in key order.
func indexPartDirections(c *qt.C, indexes []catalog.Index, name string) []bool {
	c.Helper()
	index := indexNamed(c, indexes, name)
	directions := make([]bool, len(index.Parts))
	for position, part := range index.Parts {
		directions[position] = part.Desc
	}
	return directions
}

// indexNamed is the read index carrying that name.
func indexNamed(c *qt.C, indexes []catalog.Index, name string) catalog.Index {
	c.Helper()
	for _, index := range indexes {
		if index.Name == name {
			return index
		}
	}
	c.Fatalf("no index named %q was read", name)
	return catalog.Index{}
}
