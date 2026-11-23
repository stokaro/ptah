//go:build integration

package dbschema_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// A schema name that begins with a space is a schema of its own on SQL Server,
// and the plan creates it under that name: creating the trimmed name instead
// leaves the table's schema missing, and the CREATE TABLE answers Msg 2760.
// A trailing space is another matter. The server compares names with trailing
// spaces ignored, so `[a ]` is `[a]`, and an unguarded CREATE SCHEMA [a ]
// beside [a] answers Msg 2714. The guarded creation asks the server through
// SCHEMA_ID, so the plan applies there too and adds no second schema.

// spacedSchemaPlan declares the table widget in schema, plans it against the
// live database, applies the plan, and returns the names sys.schemas holds
// that contain base, with the length in bytes of each.
func spacedSchemaPlan(c *qt.C, conn *dbschema.DatabaseConnection, schema, base string) []string {
	c.Helper()
	declared := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "dbo"}, {Name: schema}},
		Tables:  []schemamodel.Table{{StructName: "W", Name: "widget", Schema: schema}},
		Fields:  []schemamodel.Field{{StructName: "W", Name: "id", Type: "INT", Primary: true}},
	}
	for _, statement := range planSQLServerAgainstLive(c, conn, declared, schema) {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	rows, err := conn.QueryContext(c.Context(),
		"SELECT s.name, DATALENGTH(s.name), (SELECT COUNT(*) FROM sys.tables t WHERE t.schema_id = s.schema_id AND t.name = 'widget') "+
			"FROM sys.schemas s WHERE CHARINDEX(@p1, s.name) > 0 ORDER BY s.name", base)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var schemas []string
	for rows.Next() {
		var (
			name           string
			length, tables int
		)
		c.Assert(rows.Scan(&name, &length, &tables), qt.IsNil)
		schemas = append(schemas, fmt.Sprintf("%q bytes=%d widget=%d", name, length, tables))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return schemas
}

// dropSpacedSchema removes the table and the schema a row created.
func dropSpacedSchema(c *qt.C, conn *dbschema.DatabaseConnection, schema string) {
	c.Helper()
	quoted := quoteSQLServerIdentifier(schema)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS "+quoted+".[widget]")
		_, _ = conn.ExecContext(c.Context(), "DROP SCHEMA IF EXISTS "+quoted)
	})
}

func TestSQLServerLiveSpacedSchema_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		// existing is the schema the database holds before the plan, empty for
		// none, declared the one the table is declared in, and want the one
		// sys.schemas then holds, each with the base name in place of {base}.
		existing string
		declared string
		want     string
	}{
		{name: "a leading space names another schema", declared: " {base}", want: " {base}"},
		{name: "a trailing space beside the bare name is the bare name", existing: "{base}", declared: "{base} ", want: "{base}"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.SQLServer))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			base := fmt.Sprintf("ptah_spaced_%d", time.Now().UnixNano())
			named := func(spelling string) string { return strings.ReplaceAll(spelling, "{base}", base) }
			// Dropping [base ] drops [base] too, as the server compares the
			// names.
			dropSpacedSchema(c, conn, named(test.declared))
			_, err = conn.ExecContext(c.Context(),
				"DECLARE @create nvarchar(300) = N'CREATE SCHEMA ' + QUOTENAME(@p1); IF LEN(@p1) > 0 EXEC(@create)",
				named(test.existing))
			c.Assert(err, qt.IsNil)

			got := spacedSchemaPlan(c, conn, named(test.declared), base)

			// nvarchar holds two bytes per character.
			want := named(test.want)
			c.Assert(got, qt.DeepEquals, []string{fmt.Sprintf("%q bytes=%d widget=1", want, 2*len(want))})
		})
	}
}
