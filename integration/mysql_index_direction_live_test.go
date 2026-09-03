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

// TestMySQLIndexKeyPartDirectionLive is stokaro/ptah#2816 asked of the servers.
//
// The read discarded each key part's direction, so every index in a MySQL or
// MariaDB catalog arrived ascending. `KEY (a DESC)` and `KEY (a)` are different
// indexes on both engines, and a reader that reports both as ascending cannot
// tell a declaration that changed direction from one that did not.
//
// This drives the whole read rather than the query, because the direction has
// to survive the part assembly and the pass that drops a parts list saying
// nothing the column names do not -- a descending part is one of the two things
// that keep it, and a unit test over the query alone would not see that pass.
//
// The ascending index is not decoration. An assertion that only looked at the
// descending one would pass just as well against a reader that marked every
// part descending, which is the same defect wearing the other sign.
func TestMySQLIndexKeyPartDirectionLive(t *testing.T) {
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

			name := fmt.Sprintf("ptah_dir_%d", time.Now().UnixNano())
			createMySQLDatabase(c, ctx, adminDB, name)
			defer dropMySQLDatabase(c, context.Background(), adminDB, name)

			execMySQL(c, ctx, adminDB, name, `CREATE TABLE t (
				a INT NOT NULL,
				b VARCHAR(32) NOT NULL,
				KEY asc_key (a),
				KEY desc_key (a DESC),
				KEY mixed_key (a, b DESC)
			);`)

			// replaceMySQLDatabaseName rather than the generic helper: CI hands
			// MySQL a driver DSN whose tcp(host:port) form url.Parse does not
			// read, so the generic one fails at setup.
			schemaURL := replaceMySQLDatabaseName(c, dbtarget.URL(c, test.engine), name)
			conn, err := dbschema.ConnectToDatabase(ctx, schemaURL)
			c.Assert(err, qt.IsNil)
			defer conn.Close()

			live, err := conn.Reader().ReadSchemaContext(ctx)
			c.Assert(err, qt.IsNil)

			c.Assert(indexPartDirections(live, "asc_key"), qt.DeepEquals, []bool{false})
			c.Assert(indexPartDirections(live, "desc_key"), qt.DeepEquals, []bool{true})
			c.Assert(indexPartDirections(live, "mixed_key"), qt.DeepEquals, []bool{false, true})
		})
	}
}

// indexPartDirections answers one bool per key part of the named index, in key
// order, and an empty slice for an index the read did not report.
//
// An index whose parts say nothing its column names do not arrives with no
// parts at all, which is the reader dropping an uninformative list rather than
// losing anything: every such part is ascending, so the answer is built from
// the column count where the parts are absent.
func indexPartDirections(live *catalog.Database, name string) []bool {
	for _, index := range live.Indexes {
		if index.Name != name {
			continue
		}
		if len(index.Parts) == 0 {
			return make([]bool, len(index.Columns))
		}
		directions := make([]bool, 0, len(index.Parts))
		for _, part := range index.Parts {
			directions = append(directions, part.Desc)
		}
		return directions
	}
	return nil
}
