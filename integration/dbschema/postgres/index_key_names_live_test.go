//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
)

// These tests read back an index whose columns have names that must be quoted
// to be written -- mixed case, a reserved word, a space -- and compare the file
// that declared it with the database (stokaro/ptah#3615). pg_get_indexdef
// prints such a name quoted, and read from that text the index key arrived as
// `"ParentId"` rather than `ParentId`, so every comparison on PostgreSQL and
// YugabyteDB dropped the index and created it again. An expression key is the
// control: it is only ever text, and stays the text the server prints.

var indexKeyEngines = []struct {
	name   string
	engine dbtarget.Engine
	// include is the INCLUDE list the engine reports. CockroachDB stores the
	// primary key in every secondary index and does not report it as a payload
	// column.
	include []string
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL, include: []string{"Id"}},
	{name: "CockroachDB", engine: dbtarget.CockroachDB, include: nil},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB, include: []string{"Id"}},
}

const indexKeySchema = `CREATE TABLE SCHEMA."Child" ("Id" bigint PRIMARY KEY, "ParentId" bigint, "order" bigint, "my col" bigint);
CREATE INDEX "Child_Parent" ON SCHEMA."Child" ("ParentId") INCLUDE ("Id");
CREATE INDEX child_order ON SCHEMA."Child" ("order", "my col");
CREATE INDEX child_expression ON SCHEMA."Child" (("ParentId" + 1));
`

// indexKeyDatabase is a throwaway schema holding indexKeySchema, and the file
// that created it.
type indexKeyDatabase struct {
	conn   *dbschema.DatabaseConnection
	schema string
	body   string
}

// indexKeyFixture applies indexKeySchema to a throwaway schema on engine.
func indexKeyFixture(c *qt.C, engine dbtarget.Engine) indexKeyDatabase {
	c.Helper()
	ctx := c.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	schema := fmt.Sprintf("ptah_index_keys_%d", time.Now().UnixNano())
	c.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		c.Check(dropErr, qt.IsNil)
		c.Check(conn.Close(), qt.IsNil)
	})
	body := strings.ReplaceAll(indexKeySchema, "SCHEMA", schema)
	statements := []string{"CREATE SCHEMA " + pgx.Identifier{schema}.Sanitize()}
	for statement := range strings.SplitSeq(body, ";\n") {
		if strings.TrimSpace(statement) != "" {
			statements = append(statements, statement)
		}
	}
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return indexKeyDatabase{conn: conn, schema: schema, body: body}
}

// indexKeys is the part of a read index these tests are about.
type indexKeys struct {
	Columns, Include []string
	Expression       string
}

func readIndexKeys(c *qt.C, conn *dbschema.DatabaseConnection, schema string) map[string]indexKeys {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schema})
	c.Assert(err, qt.IsNil)
	keys := make(map[string]indexKeys, len(live.Indexes))
	for _, index := range live.Indexes {
		keys[index.Name] = indexKeys{Columns: index.Columns, Include: index.IncludeColumns, Expression: expressionKey(index)}
	}
	return keys
}

// expressionKey is the first key the reader marked as an expression.
func expressionKey(index catalog.Index) string {
	for _, part := range index.Parts {
		if part.Expr != "" {
			return part.Expr
		}
	}
	return ""
}

// TestIndexKeyNames_LiveReadReportsTheColumnNames reads the index keys back as
// the columns' names, without the quotes a statement needs to write them.
func TestIndexKeyNames_LiveReadReportsTheColumnNames(t *testing.T) {
	for _, engine := range indexKeyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			f := indexKeyFixture(c, engine.engine)

			keys := readIndexKeys(c, f.conn, f.schema)

			c.Assert(keys["Child_Parent"].Columns, qt.DeepEquals, []string{"ParentId"})
			c.Assert(keys["Child_Parent"].Include, qt.DeepEquals, engine.include)
			c.Assert(keys["child_order"].Columns, qt.DeepEquals, []string{"order", "my col"})
			c.Assert(keys["child_expression"].Expression, qt.Contains, `"ParentId"`)
		})
	}
}

// TestIndexKeyNames_LiveSchemaFileComparesEqual compares the file that
// created the indexes with the database. Nothing is planned for them.
func TestIndexKeyNames_LiveSchemaFileComparesEqual(t *testing.T) {
	for _, engine := range indexKeyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			f := indexKeyFixture(c, engine.engine)
			path := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(path, []byte(f.body), 0o600), qt.IsNil)
			dialect := f.conn.Info().Dialect
			desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: dialect})
			c.Assert(err, qt.IsNil)
			live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema})
			c.Assert(err, qt.IsNil)

			diff := schemadiff.CompareWithDialect(desired, live, dialect)

			c.Assert(diff.IndexesAdded, qt.HasLen, 0, qt.Commentf("%+v", diff.IndexesAdded))
			c.Assert(diff.IndexesRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.IndexesRemoved))
		})
	}
}
