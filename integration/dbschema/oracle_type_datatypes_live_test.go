//go:build integration

package dbschema_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestOracleNativeTypeDimensionsLive replays datatype spellings from the public
// reader. The server must compile both objects and report the same dimensions
// for each original and copy, including DECIMAL precision and national lengths.
func TestOracleNativeTypeDimensionsLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.Oracle))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	defer func() {
		for _, statement := range []string{
			"DROP TYPE IF EXISTS ptah_datatype_copy", "DROP TYPE IF EXISTS ptah_datatype_original",
			"DROP DOMAIN IF EXISTS ptah_datatype_number_copy", "DROP DOMAIN IF EXISTS ptah_datatype_number",
			"DROP DOMAIN IF EXISTS ptah_datatype_nchar_copy", "DROP DOMAIN IF EXISTS ptah_datatype_nchar",
		} {
			c.Check(conn.SchemaWriter().ExecuteSQL(context.WithoutCancel(ctx), statement), qt.IsNil)
		}
	}()
	for _, statement := range []string{
		"CREATE DOMAIN ptah_datatype_number AS NUMBER(*,0)",
		"CREATE DOMAIN ptah_datatype_nchar AS NVARCHAR2(10)",
		`CREATE TYPE ptah_datatype_original AS OBJECT (
			d DECIMAL(5,2), neg DECIMAL(5,-2), f FLOAT, sized FLOAT(10),
			v NVARCHAR2(10), text_b VARCHAR2(20))`,
	} {
		c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("%s", statement))
	}
	original, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	fields := oracleDatatypeFields(c, original, "PTAH_DATATYPE_ORIGINAL")
	c.Assert(fields, qt.DeepEquals, []catalog.CompositeField{
		{Name: "D", Type: "DECIMAL(5,2)"}, {Name: "NEG", Type: "DECIMAL(5,-2)"},
		{Name: "F", Type: "FLOAT"}, {Name: "SIZED", Type: "FLOAT(10)"},
		{Name: "V", Type: "NVARCHAR2(10)"}, {Name: "TEXT_B", Type: "VARCHAR2(20)"},
	})
	parts := make([]string, len(fields))
	for i, field := range fields {
		parts[i] = field.Name + " " + field.Type
	}
	c.Assert(conn.SchemaWriter().ExecuteSQL(ctx,
		"CREATE TYPE ptah_datatype_copy AS OBJECT ("+strings.Join(parts, ", ")+")",
	), qt.IsNil)
	for _, test := range []struct{ name, want string }{
		{"PTAH_DATATYPE_NUMBER", "NUMBER(*,0)"}, {"PTAH_DATATYPE_NCHAR", "NVARCHAR2(10)"},
	} {
		base := oracleDatatypeDomain(c, original, test.name)
		c.Assert(base, qt.Equals, test.want)
		c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, "CREATE DOMAIN "+test.name+"_COPY AS "+base), qt.IsNil)
	}
	replayed, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleDatatypeFields(c, replayed, "PTAH_DATATYPE_COPY"), qt.DeepEquals, fields)
	for _, name := range []string{"PTAH_DATATYPE_NUMBER", "PTAH_DATATYPE_NCHAR"} {
		c.Assert(oracleDatatypeDomain(c, replayed, name+"_COPY"), qt.Equals, oracleDatatypeDomain(c, original, name))
	}
	var valid int
	c.Assert(conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM user_objects
		WHERE object_name IN ('PTAH_DATATYPE_ORIGINAL', 'PTAH_DATATYPE_COPY')
		AND object_type = 'TYPE' AND status = 'VALID'`).Scan(&valid), qt.IsNil)
	c.Assert(valid, qt.Equals, 2)
}

func oracleDatatypeFields(c *qt.C, schema *catalog.Database, name string) []catalog.CompositeField {
	c.Helper()
	index := slices.IndexFunc(schema.Composites, func(value catalog.CompositeType) bool { return value.Name == name })
	c.Assert(index >= 0, qt.IsTrue, qt.Commentf("object type %s is present", name))
	return schema.Composites[index].Fields
}

func oracleDatatypeDomain(c *qt.C, schema *catalog.Database, name string) string {
	c.Helper()
	index := slices.IndexFunc(schema.Domains, func(value catalog.Domain) bool { return value.Name == name })
	c.Assert(index >= 0, qt.IsTrue, qt.Commentf("domain %s is present", name))
	return schema.Domains[index].BaseType
}
