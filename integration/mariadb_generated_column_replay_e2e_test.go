//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestMariaDBGeneratedColumnReplaysE2E pins that a MariaDB database holding a
// generated column can be replayed at all.
//
// It could not. MariaDB reports COLUMN_DEFAULT as the literal text `NULL` for a
// column that has no default, where MySQL reports SQL NULL, so the reader
// recorded a default on every nullable column and the renderer wrote
// `DEFAULT NULL`. An ordinary column tolerates that; a generated column does
// not:
//
//	`full_name` varchar(300) ... GENERATED ALWAYS AS (concat(`email`,' ')) STORED DEFAULT NULL,
//	Error 1064 (42000): You have an error in your SQL syntax; ... near 'DEFAULT NULL'
//
// `schema apply --dry-run` against the source answered `Schema is synced, no
// changes to be made.` the whole time, because both sides of that comparison
// read through the same reader. Only a second database can tell
// (stokaro/ptah#2128).
//
// The string column is the control, and it is a real MariaDB answer rather than
// an invented one: `DEFAULT 'NULL'` is stored WITH its quotes, so a rule that
// folded both would replay a column that has a default as one that has none,
// and this test would still pass without it.
func TestMariaDBGeneratedColumnReplaysE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, dbtarget.MariaDBAdmin))
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	stamp := time.Now().UnixNano()
	sourceName := fmt.Sprintf("ptah_gen_src_%d", stamp)
	replayName := fmt.Sprintf("ptah_gen_replay_%d", stamp)
	createMySQLDatabase(c, ctx, adminDB, sourceName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, sourceName)
	createMySQLDatabase(c, ctx, adminDB, replayName)
	defer dropMySQLDatabase(c, context.Background(), adminDB, replayName)

	adminURL := dbtarget.URL(t, dbtarget.MariaDBAdmin)
	sourceURL := replaceDatabaseName(c, adminURL, sourceName)
	replayURL := replaceDatabaseName(c, adminURL, replayName)

	// Written by hand rather than by Ptah. A table Ptah created would round-trip
	// through whatever the renderer writes, and a reader defect stays invisible
	// against a fixture the renderer wrote.
	seedMariaDBGeneratedFixture(c, ctx, adminDB, sourceName)

	// What the catalog says, before Ptah is asked. Without it the assertions
	// below could agree with a server that answered differently, and the test
	// would pass while measuring nothing.
	c.Assert(mariaDBCatalogDefaults(c, ctx, adminDB, sourceName), qt.DeepEquals, map[string]string{
		"email":     "<SQL NULL>",
		"bio":       "NULL",
		"full_name": "NULL",
		"tag":       "'NULL'",
	})

	repoRoot := e2eRepoRoot(t)
	binary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, binary)

	dir := c.TempDir()
	documentPath := filepath.Join(dir, "source.hcl")
	document, stderr, err := runCLIProcess(ctx, dir, binary, "schema", "inspect", "--db-url", sourceURL)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	c.Assert(os.WriteFile(documentPath, []byte(replaceSchemaName(document, sourceName, replayName)), 0o600), qt.IsNil)

	_, applyErr, err := runCLIProcess(ctx, dir, binary,
		"schema", "apply", "--db-url", replayURL, "--to", "file://"+documentPath, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("the replay was refused:\n%s\ndocument:\n%s", applyErr, document))

	// The replayed database read back from the catalog rather than through
	// Ptah: a reader that misunderstands a default on the way in would
	// misunderstand it on the way out, and the two sides would agree while both
	// were wrong.
	c.Assert(mariaDBCatalogDefaults(c, ctx, adminDB, replayName), qt.DeepEquals, map[string]string{
		"email":     "<SQL NULL>",
		"bio":       "NULL",
		"full_name": "NULL",
		"tag":       "'NULL'",
	})
}

// replaceSchemaName retargets the document at the database it is replayed into.
//
// A MySQL-family document names its schema, and the schema IS the database, so
// applying the source's document verbatim would write back into the source and
// prove nothing.
func replaceSchemaName(document, from, to string) string {
	return strings.ReplaceAll(document, from, to)
}

// seedMariaDBGeneratedFixture creates the table this test reads.
func seedMariaDBGeneratedFixture(c *qt.C, ctx context.Context, adminDB *sql.DB, database string) {
	c.Helper()

	_, err := adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`customers` ("+
			"`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "+
			"`email` VARCHAR(255) NOT NULL, "+
			"`bio` TEXT, "+
			"`tag` VARCHAR(10) DEFAULT 'NULL', "+
			"`full_name` VARCHAR(300) AS (CONCAT(`email`,' ')) STORED"+
			")", database))
	c.Assert(err, qt.IsNil)
}

// mariaDBCatalogDefaults asks the catalog what each column's default is, with
// SQL NULL spelled apart from the text NULL -- which is the whole distinction
// under test and the one the client prints identically.
func mariaDBCatalogDefaults(c *qt.C, ctx context.Context, adminDB *sql.DB, database string) map[string]string {
	c.Helper()

	rows, err := adminDB.QueryContext(ctx,
		"SELECT COLUMN_NAME, IFNULL(COLUMN_DEFAULT, '<SQL NULL>') "+
			"FROM information_schema.COLUMNS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'customers' AND COLUMN_NAME <> 'id'",
		database)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	defaults := make(map[string]string)
	for rows.Next() {
		var name, value string
		c.Assert(rows.Scan(&name, &value), qt.IsNil)
		defaults[name] = value
	}
	c.Assert(rows.Err(), qt.IsNil)
	return defaults
}
