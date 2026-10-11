//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/microsoft/go-mssqldb" // registers the sqlserver driver for database/sql

	"ptah.run/internal/dbtarget"
)

// sqlServerLineageSeed is one table and one procedure over it. The procedure
// writes one column inside a transaction and runs a statement it composes at
// run time.
var sqlServerLineageSeed = []string{
	"CREATE TABLE lineage_customers (id INT PRIMARY KEY, email NVARCHAR(200), country NVARCHAR(2))",
	`CREATE PROCEDURE lineage_touch @target INT AS
	BEGIN
	  DECLARE @s NVARCHAR(200) = N'DELETE FROM lineage_customers';
	  BEGIN TRAN;
	  UPDATE lineage_customers SET country = 'CZ' WHERE id = @target;
	  COMMIT;
	  EXEC sp_executesql @s;
	END`,
}

// TestSchemaLineageReadsALiveSQLServerProcedureE2E drives the SQL Server
// owner's routine body reader with a body the server stored, not one a test
// wrote.
//
// The UPDATE is the write, and the column its predicate names is the read. The
// transaction statements write nothing and are not mentioned; sp_executesql
// leaves the writes incomplete and is named.
func TestSchemaLineageReadsALiveSQLServerProcedureE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	databaseURL := newSQLServerLineageDatabase(c, ctx, dbURL)

	document := runLineageJSON(c, databaseURL)

	c.Assert(document.Routines.Reads, qt.HasLen, 1)
	c.Assert(document.Routines.Reads[0].Column, qt.Equals, "id")
	c.Assert(document.Routines.Writes, qt.HasLen, 1)
	c.Assert(document.Routines.Writes[0].Table, qt.Equals, "lineage_customers")
	c.Assert(document.Routines.Writes[0].Column, qt.Equals, "country")
	c.Assert(document.Routines.Writes[0].ByRoutine, qt.Contains, "lineage_touch")
	c.Assert(document.Routines.Undecided, qt.HasLen, 1)
	c.Assert(document.Routines.Undecided[0].Reason, qt.Equals, "the body is sqlserver: "+
		"an EXEC composes its statement at run time, so the writes are not complete; "+
		"the reads are those of its statements that name one table")
}

// newSQLServerLineageDatabase creates a throwaway database carrying the seed
// above.
func newSQLServerLineageDatabase(c *qt.C, ctx context.Context, dbURL string) string {
	c.Helper()

	adminDB, err := sql.Open("sqlserver", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("ptah_lineage_%d", time.Now().UnixNano())
	_, err = adminDB.ExecContext(ctx, "CREATE DATABASE ["+name+"]")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := adminDB.ExecContext(context.Background(),
			"ALTER DATABASE ["+name+"] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ["+name+"]")
		c.Check(dropErr, qt.IsNil)
	})

	databaseURL := replaceSQLServerDatabase(c, dbURL, name)
	db, err := sql.Open("sqlserver", databaseURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	for _, statement := range sqlServerLineageSeed {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}
	return databaseURL
}
