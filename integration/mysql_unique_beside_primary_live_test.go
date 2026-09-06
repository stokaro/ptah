//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver for database/sql

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
)

// TestMySQLUniqueBesidePrimaryConvergesLive is stokaro/ptah#2787 asked of the
// servers rather than of the renderer.
//
// A rendered-text assertion proves what Ptah writes and says nothing about
// whether the writing means what the source meant. The two spellings this is
// about produce DIFFERENT catalog state on MariaDB -- `a INT PRIMARY KEY UNIQUE`
// yields the primary key alone while `a INT UNIQUE, PRIMARY KEY (a)` yields a
// secondary unique index beside it -- so "renders something reasonable" and
// "renders something that means the same" are not the same claim, and only a
// live catalog separates them.
//
// So each form is created twice in one database: once as the source wrote it,
// once as Ptah renders it. The assertion is that the two agree, index name
// included. Nothing here spells what the answer should be, because the answer
// is the engine's and differs between the two.
//
// # Both engines, and neither one is redundant
//
// The defect has two halves and each engine sees exactly one of them. Measured
// by reverting each half in turn against this test:
//
//	half reverted                              mysql   mariadb
//	the renderer drops UNIQUE beside PRIMARY   FAIL    pass
//	the table-level key collapses onto the     pass    FAIL
//	column
//
// MariaDB folds `PRIMARY KEY UNIQUE` into one key, so it agrees with a renderer
// that dropped the UNIQUE; MySQL keeps both indexes whichever spelling it is
// given, so it agrees with a renderer that collapsed the table-level key. A
// single-engine version of this test would report one of the two halves as
// correct.
func TestMySQLUniqueBesidePrimaryConvergesLive(t *testing.T) {
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

			name := fmt.Sprintf("ptah_uq_pk_%d", time.Now().UnixNano())
			createMySQLDatabase(c, ctx, adminDB, name)
			defer dropMySQLDatabase(c, context.Background(), adminDB, name)

			dialect := mysqlFamilyDialect(test.engine)
			assertUniqueBesidePrimaryConverges(c, ctx, adminDB, name, dialect,
				"table_level", `CREATE TABLE t (a INT UNIQUE, PRIMARY KEY (a));`)
			assertUniqueBesidePrimaryConverges(c, ctx, adminDB, name, dialect,
				"inline", `CREATE TABLE t (a INT PRIMARY KEY UNIQUE);`)
		})
	}
}

// assertUniqueBesidePrimaryConverges creates one source form and Ptah's
// rendering of it, then requires the two tables to carry the same indexes.
func assertUniqueBesidePrimaryConverges(
	c *qt.C, ctx context.Context, db *sql.DB, schema, dialect, label, source string,
) {
	c.Helper()

	sourceTable := "src_" + label
	renderedTable := "ren_" + label

	database, _, err := sqlschema.Read([]byte(source), dialect)
	c.Assert(err, qt.IsNil)
	rendered, err := renderer.GetOrderedCreateStatements(&database, dialect)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.HasLen, 1)

	// Each statement names its own table, so both live in one database and one
	// query reads them together. A second database would compare two servers'
	// answers rather than two statements'.
	execMySQL(c, ctx, db, schema, renameCreatedTable(source, "t", sourceTable))
	execMySQL(c, ctx, db, schema, renameCreatedTable(rendered[0], "`t`", "`"+renderedTable+"`"))

	sourceIndexes := mysqlIndexRows(c, ctx, db, schema, sourceTable)
	// The source has to produce more than the primary key somewhere, or the
	// comparison below would hold for a renderer that emitted nothing at all.
	c.Assert(len(sourceIndexes) > 0, qt.IsTrue)
	c.Assert(mysqlIndexRows(c, ctx, db, schema, renderedTable), qt.DeepEquals, sourceIndexes,
		qt.Commentf("%s: rendered %q", label, rendered[0]))
}

// renameCreatedTable points one CREATE TABLE at another name.
func renameCreatedTable(statement, from, to string) string {
	return strings.Replace(statement, "CREATE TABLE "+from, "CREATE TABLE "+to, 1)
}

// execMySQL runs one statement inside the named schema.
func execMySQL(c *qt.C, ctx context.Context, db *sql.DB, schema, statement string) {
	c.Helper()
	// #nosec G201 -- the schema name is this test's own generated identifier.
	_, err := db.ExecContext(ctx, "USE `"+schema+"`")
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, statement)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
}

// mysqlIndexRows is what the catalog says one table's indexes are.
//
// The index NAME is part of the comparison. MySQL derives `a` for a unique key
// over column `a`, and a rendering that produced the same two indexes under a
// different name would be a different schema to anything that reads names --
// which is most of Ptah.
func mysqlIndexRows(c *qt.C, ctx context.Context, db *sql.DB, schema, table string) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`, schema, table)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var found []string
	for rows.Next() {
		var indexName, columnName string
		var nonUnique, sequence int
		c.Assert(rows.Scan(&indexName, &nonUnique, &sequence, &columnName), qt.IsNil)
		found = append(found, fmt.Sprintf("%s non_unique=%d seq=%d column=%s",
			indexName, nonUnique, sequence, columnName))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// mysqlFamilyDialect is the dialect name for the engine a row names.
func mysqlFamilyDialect(engine dbtarget.Engine) string {
	names := map[dbtarget.Engine]string{
		dbtarget.MySQLAdmin:   platform.MySQL,
		dbtarget.MariaDBAdmin: platform.MariaDB,
	}
	return names[engine]
}
