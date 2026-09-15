//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemaload"
)

// keywordColumnTarget is one live engine and the columns named after MySQL's
// index keywords that the engine accepts.
type keywordColumnTarget struct {
	name    string
	engine  dbtarget.Engine
	dialect string
	// idColumn and suffix complete a CREATE TABLE the engine accepts around the
	// column under test.
	idColumn string
	suffix   string
	// drop removes the table. Oracle keeps a dropped table in its recycle bin
	// unless the statement purges it.
	drop    string
	columns []keywordColumn
}

// keywordColumn is a column named after an index keyword, with a type. A type
// carrying parentheses is the silent shape: read as an index, the element
// declares one named after the type and the column is lost without an error.
type keywordColumn struct {
	name       string
	columnType string
}

var keywordColumnTargets = []keywordColumnTarget{
	{
		// The control: PostgreSQL has read these words as columns since
		// stokaro/ptah#3089, so a red row here is the harness, not the dialect.
		name: "postgres", engine: dbtarget.PostgreSQL, dialect: platform.Postgres,
		idColumn: "id integer PRIMARY KEY", drop: "DROP TABLE %s",
		columns: []keywordColumn{
			{name: "key", columnType: "varchar(32)"},
			{name: "spatial", columnType: "text"},
			{name: "fulltext", columnType: "text"},
			{name: "index", columnType: "varchar(32)"},
		},
	},
	{
		// `index varchar(32)` is not a row: CockroachDB reads it as an index
		// too and refuses the statement at `32`.
		name: "cockroachdb", engine: dbtarget.CockroachDB, dialect: platform.CockroachDB,
		idColumn: "id integer PRIMARY KEY", drop: "DROP TABLE %s",
		columns: []keywordColumn{
			{name: "key", columnType: "varchar(32)"},
			{name: "spatial", columnType: "text"},
			{name: "fulltext", columnType: "text"},
			{name: "index", columnType: "text"},
		},
	},
	{
		name: "spanner", engine: dbtarget.Spanner, dialect: platform.Spanner,
		idColumn: "id bigint PRIMARY KEY", drop: "DROP TABLE %s",
		columns: []keywordColumn{
			{name: "key", columnType: "text"},
			{name: "spatial", columnType: "varchar(32)"},
			{name: "fulltext", columnType: "text"},
			{name: "index", columnType: "varchar(32)"},
		},
	},
	{
		// `index String` is not a row: ClickHouse reads INDEX as its
		// data-skipping index and refuses the statement.
		name: "clickhouse", engine: dbtarget.ClickHouse, dialect: platform.ClickHouse,
		idColumn: "id Int32", suffix: " ENGINE = MergeTree ORDER BY id", drop: "DROP TABLE %s",
		columns: []keywordColumn{
			{name: "key", columnType: "String"},
			{name: "spatial", columnType: "FixedString(32)"},
			{name: "fulltext", columnType: "Nullable(String)"},
		},
	},
	{
		// SQL Server reserves `key` and reads `index` as its inline index, so
		// only these two are columns there.
		name: "sqlserver", engine: dbtarget.SQLServer, dialect: platform.SQLServer,
		idColumn: "id int PRIMARY KEY", drop: "DROP TABLE %s",
		columns: []keywordColumn{
			{name: "spatial", columnType: "nvarchar(32)"},
			{name: "fulltext", columnType: "int"},
		},
	},
	{
		// Oracle reserves `index` (ORA-03050).
		name: "oracle", engine: dbtarget.Oracle, dialect: platform.Oracle,
		idColumn: "id NUMBER PRIMARY KEY", drop: "DROP TABLE %s PURGE",
		columns: []keywordColumn{
			{name: "key", columnType: "VARCHAR2(32)"},
			{name: "spatial", columnType: "NUMBER"},
			{name: "fulltext", columnType: "VARCHAR2(32)"},
		},
	},
}

// TestTableElementKeywordNamesAColumnLive holds a `.sql` schema source to the
// columns the engine creates from the same statement.
//
// KEY, SPATIAL, FULLTEXT and INDEX open a table-level index on MySQL. Read that
// way on an engine that has no such element, `key text` is refused at the type
// and `spatial nvarchar(32)` parses without an error as an index named after
// the type, so the column is lost (stokaro/ptah#3299).
//
// Each row executes its CREATE TABLE first and asks the server which columns the
// table has, so the engine's acceptance and its answer are measured on every
// run rather than written down. The same statement is then loaded through the
// loader every desired-schema verb uses, and the two column lists must agree.
func TestTableElementKeywordNamesAColumnLive(t *testing.T) {
	for _, target := range keywordColumnTargets {
		t.Run(target.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(t.Context(), dbtarget.URL(c, target.engine))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			for _, column := range target.columns {
				t.Run(column.name, func(t *testing.T) {
					c := qt.New(t)
					table := fmt.Sprintf("ptah_kw_%s_%d", column.name, time.Now().UnixNano())
					ddl := fmt.Sprintf("CREATE TABLE %s (%s, %s %s)%s",
						table, target.idColumn, column.name, column.columnType, target.suffix)

					_, err := conn.ExecContext(t.Context(), ddl)
					c.Assert(err, qt.IsNil, qt.Commentf("the engine refused %s", ddl))
					c.Cleanup(func() {
						_, _ = conn.ExecContext(context.Background(), fmt.Sprintf(target.drop, table))
					})

					served := servedColumnNames(c, conn, table)
					c.Assert(served, qt.DeepEquals, []string{"id", column.name})
					c.Assert(loadedColumnNames(c, ddl, target.dialect), qt.DeepEquals, served,
						qt.Commentf("the engine accepted %s", ddl))
				})
			}
		})
	}
}

// servedColumnNames asks the engine which columns a table has. The names are
// folded to lower case because Oracle reports an unquoted name in upper case.
func servedColumnNames(c *qt.C, conn *dbschema.DatabaseConnection, table string) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), "SELECT * FROM "+table+" WHERE 1 = 0")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()

	names, err := rows.Columns()
	c.Assert(err, qt.IsNil)
	c.Assert(rows.Next(), qt.IsFalse)
	c.Assert(rows.Err(), qt.IsNil)
	for i, name := range names {
		names[i] = strings.ToLower(name)
	}
	return names
}

// loadedColumnNames reads one statement as a `.sql` schema source for a dialect,
// through the loader `ptah viz`, `ptah schema apply` and every other
// desired-schema verb use, and returns the columns it declares.
func loadedColumnNames(c *qt.C, ddl, dialect string) []string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(ddl+";\n"), 0o600), qt.IsNil)

	db, err := schemaload.LoadContext(c.Context(), schemaload.Options{
		SchemaFiles: []string{path},
		Dialect:     dialect,
	})
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(db.Fields))
	for _, field := range db.Fields {
		names = append(names, field.Name)
	}
	return names
}
