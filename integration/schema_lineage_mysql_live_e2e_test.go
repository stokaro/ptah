//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the mysql driver for database/sql

	"ptah.run/internal/dbtarget"
)

// mysqlLineageSeed is one table and one procedure over it. The procedure
// writes one column, walks a cursor, and runs a statement it composes at run
// time.
var mysqlLineageSeed = []string{
	"CREATE TABLE lineage_customers (id INT PRIMARY KEY, email TEXT, country TEXT)",
	`CREATE PROCEDURE lineage_touch(IN target INT)
	  BEGIN
	    DECLARE cur CURSOR FOR SELECT id FROM lineage_customers;
	    OPEN cur;
	    CLOSE cur;
	    UPDATE lineage_customers SET country = 'CZ' WHERE id = target;
	    SET @s = 'DELETE FROM lineage_customers';
	    PREPARE stmt FROM @s;
	    EXECUTE stmt;
	    DEALLOCATE PREPARE stmt;
	  END`,
}

// TestSchemaLineageReadsALiveMySQLFamilyProcedureE2E drives the MySQL owner's
// routine body reader with a body the server stored, not one a test wrote.
//
// The UPDATE is the write. PREPARE and EXECUTE each leave the writes incomplete
// and are named; the cursor statements and the DEALLOCATE write nothing and are
// not mentioned. MySQL cannot tell a variable from a column, so no read is
// reported.
func TestSchemaLineageReadsALiveMySQLFamilyProcedureE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		admin dbtarget.Engine
	}{
		{name: "mysql", admin: dbtarget.MySQLAdmin},
		{name: "mariadb", admin: dbtarget.MariaDBAdmin},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			databaseURL := newMySQLLineageDatabase(c, ctx, test.admin)

			document := runLineageJSON(c, databaseURL)

			c.Assert(document.Routines.Reads, qt.HasLen, 0)
			c.Assert(document.Routines.Writes, qt.HasLen, 1)
			c.Assert(document.Routines.Writes[0].Table, qt.Equals, "lineage_customers")
			c.Assert(document.Routines.Writes[0].Column, qt.Equals, "country")
			c.Assert(document.Routines.Writes[0].ByRoutine, qt.Contains, "lineage_touch")
			c.Assert(document.Routines.Undecided, qt.HasLen, 1)
			c.Assert(document.Routines.Undecided[0].Reason, qt.Matches, `the body is \w+: `+
				`a PREPARE composes its statement at run time; an EXECUTE composes its statement at run time, `+
				`so the writes are not complete; the columns it reads are not resolved, .*`)
		})
	}
}

// newMySQLLineageDatabase creates a throwaway database carrying the seed above.
func newMySQLLineageDatabase(c *qt.C, ctx context.Context, admin dbtarget.Engine) string {
	c.Helper()

	adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, admin))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("ptah_lineage_%d", time.Now().UnixNano())
	createMySQLDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), adminDB, name) })

	conn, err := adminDB.Conn(ctx)
	c.Assert(err, qt.IsNil)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, fmt.Sprintf("USE `%s`", name))
	c.Assert(err, qt.IsNil)
	for _, statement := range mysqlLineageSeed {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}
	return replaceMySQLDatabaseName(c, dbtarget.URL(c, admin), name)
}
