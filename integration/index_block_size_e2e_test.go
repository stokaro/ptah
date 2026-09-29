//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// blockSizeSchema includes secondary, unique and primary indexes, so the
// primary key's separate comparison and planner paths are exercised too.
func blockSizeSchema(size int, comment, options string) string {
	return fmt.Sprintf("CREATE TABLE bt (id int NOT NULL, a int, b int, PRIMARY KEY(id) KEY_BLOCK_SIZE=%d COMMENT '%s', KEY k(a) KEY_BLOCK_SIZE=%d, UNIQUE KEY u(b) KEY_BLOCK_SIZE=%d)%s;", size, comment, size, size, options)
}

func blockSizeDDL(c *qt.C, dsn string) string {
	c.Helper()
	conn, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	var name, ddl string
	c.Assert(conn.QueryRowContext(c.Context(), "SHOW CREATE TABLE bt").Scan(&name, &ddl), qt.IsNil)
	return ddl
}

// Compare with a database built directly from the requested SQL. Each apply
// must converge to the server's own representation, including MySQL dropping
// an uncompressed table's hints, and removing previously stored hints.
func TestSchemaApplyIndexBlockSizeE2E(t *testing.T) {
	for _, engine := range []struct {
		name    string
		engine  dbtarget.Engine
		options string
	}{
		{"MySQL compressed", dbtarget.MySQLAdmin, " ROW_FORMAT=COMPRESSED"},
		{"MySQL dynamic", dbtarget.MySQLAdmin, ""},
		{"MariaDB", dbtarget.MariaDBAdmin, ""},
	} {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, target := scratch.database(c, "block_target")
			for _, desired := range []struct {
				size    int
				comment string
			}{{8, "first"}, {4, "changed"}, {0, ""}} {
				file := blockSizeSchema(desired.size, desired.comment, engine.options)
				builtName, _ := scratch.builtFrom(c, file)
				schema := writeKeyFile(c, file)
				runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
				c.Assert(blockSizeDDL(c, mySQLDSNForDatabase(c, scratch.adminDSN, name)), qt.Equals,
					blockSizeDDL(c, mySQLDSNForDatabase(c, scratch.adminDSN, builtName)))
				c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run"), qt.Contains, "Schema is synced")
			}
		})
	}
}

// A DB read must preserve the hint and the compression that makes MySQL keep
// it. Replaying the SQL into another database must reproduce SHOW CREATE TABLE.
func TestDBReadIndexBlockSizeE2E(t *testing.T) {
	for _, engine := range []struct {
		name    string
		engine  dbtarget.Engine
		options string
	}{
		{"MySQL", dbtarget.MySQLAdmin, " ROW_FORMAT=COMPRESSED"},
		{"MariaDB", dbtarget.MariaDBAdmin, ""},
	} {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			sourceName, source := scratch.builtFrom(c, blockSizeSchema(8, "lookup", engine.options))
			targetName, target := scratch.database(c, "block_copy")
			read, stderr, err := runPtahSplitStreams(c.Context(), []string{"db", "read", "--db-url", source})
			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			c.Assert(strings.Count(read, "KEY_BLOCK_SIZE=8"), qt.Equals, 3)
			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", writeKeyFile(c, read), "--auto-approve")
			c.Assert(blockSizeDDL(c, mySQLDSNForDatabase(c, scratch.adminDSN, targetName)), qt.Equals,
				blockSizeDDL(c, mySQLDSNForDatabase(c, scratch.adminDSN, sourceName)))
		})
	}
}

// A primary-key option change must keep the key available to AUTO_INCREMENT
// and incoming foreign keys throughout the ALTER, including when removing it.
func TestSchemaApplyPrimaryKeyOptionsWithDependenciesE2E(t *testing.T) {
	for _, engine := range keyIndexDropEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			const schemaFormat = "CREATE TABLE bt (id int NOT NULL AUTO_INCREMENT, PRIMARY KEY(id) KEY_BLOCK_SIZE=%d COMMENT '%s') ROW_FORMAT=COMPRESSED; CREATE TABLE child (id int PRIMARY KEY, parent_id int, CONSTRAINT fk FOREIGN KEY(parent_id) REFERENCES bt(id));"
			const data = " INSERT INTO bt VALUES(1); INSERT INTO child VALUES(1,1);"
			name, target := scratch.builtFrom(c, fmt.Sprintf(schemaFormat, 8, "old")+data)
			conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, name))
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(conn.Close(), qt.IsNil) }()
			for _, desired := range []struct {
				size    int
				comment string
			}{{4, "new"}, {0, ""}} {
				schema := fmt.Sprintf(schemaFormat, desired.size, desired.comment)
				file := writeKeyFile(c, schema)
				built, _ := scratch.builtFrom(c, schema+data)
				runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")
				c.Assert(blockSizeDDL(c, mySQLDSNForDatabase(c, scratch.adminDSN, name)), qt.Equals,
					blockSizeDDL(c, mySQLDSNForDatabase(c, scratch.adminDSN, built)))
				c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run"), qt.Contains, "Schema is synced")
				var count int
				c.Assert(conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM child JOIN bt ON child.parent_id=bt.id").Scan(&count), qt.IsNil)
				c.Assert(count, qt.Equals, 1)
				_, err = conn.ExecContext(c.Context(), "INSERT INTO child VALUES(2,999)")
				c.Assert(err, qt.ErrorMatches, `(?s)Error 1452 .*`)
			}
		})
	}
}
