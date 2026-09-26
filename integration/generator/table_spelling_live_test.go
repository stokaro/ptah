//go:build integration

package generator_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// A table declared without a schema is created in the schema the server
// resolves unqualified names to, so whether `t` and `public.t` are one table is
// the server's answer (stokaro/ptah#3721). On the default search path they are
// one, and the comparison refuses the pair before planning; on a search path
// that puts another schema first they are two, and the plan creates both.

// spellingDeclaration declares one table name twice, bare and in public, with
// different columns.
func spellingDeclaration(table string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Bare", Name: table},
			{StructName: "Public", Schema: "public", Name: table},
		},
		Fields: []schemamodel.Field{
			{StructName: "Bare", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Public", Name: "id", Type: "BIGINT", Primary: true},
		},
	}
}

// spellingConnection connects on the server's default search path.
func spellingConnection(c *qt.C, dbURL string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// spellingConnectionOnPath connects with searchPath first on the search path.
func spellingConnectionOnPath(c *qt.C, dbURL, searchPath string) *dbschema.DatabaseConnection {
	c.Helper()
	scoped, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := scoped.Query()
	query.Set("search_path", searchPath)
	scoped.RawQuery = query.Encode()
	return spellingConnection(c, scoped.String())
}

// TestPostgresLiveBareAndPublicTableAreOneTable is the default search path: the
// bare table lands in public, the pair names one table, and the comparison
// refuses it before anything is planned or created.
func TestPostgresLiveBareAndPublicTableAreOneTable(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	conn := spellingConnection(c, dbURL)
	table := fmt.Sprintf("ptah_spelling_%d", time.Now().UnixNano())

	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, spellingDeclaration(table), live, nil)

	c.Assert(err, qt.ErrorMatches, fmt.Sprintf(`(?s).*table %q is declared twice, once without a schema and once as "public\.%s".*`, table, table))
	c.Assert(diff, qt.IsNil)
}

// TestPostgresLiveBareAndPublicTableAreTwoTablesOnAnotherSearchPath puts a
// schema of the test's own first on the search path. The bare table is created
// there and the public one in public, so the comparison accepts the pair and
// applying the plan leaves two tables.
func TestPostgresLiveBareAndPublicTableAreTwoTablesOnAnotherSearchPath(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	suffix := time.Now().UnixNano()
	schemaName := fmt.Sprintf("ptah_spelling_path_%d", suffix)
	table := fmt.Sprintf("ptah_spelling_%d", suffix)

	admin := spellingConnection(c, dbURL)
	_, err := admin.ExecContext(c.Context(), `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
		c.Check(err, qt.IsNil)
		_, err = admin.ExecContext(context.Background(), `DROP TABLE IF EXISTS public."`+table+`"`)
		c.Check(err, qt.IsNil)
	})
	conn := spellingConnectionOnPath(c, dbURL, schemaName)

	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, spellingDeclaration(table), live, nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	var schemas []string
	rows, err := admin.QueryContext(c.Context(),
		`SELECT schemaname FROM pg_tables WHERE tablename = $1 ORDER BY schemaname`, table)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	for rows.Next() {
		var schema string
		c.Assert(rows.Scan(&schema), qt.IsNil)
		schemas = append(schemas, schema)
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(schemas, qt.DeepEquals, []string{schemaName, "public"})
}
