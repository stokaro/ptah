//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// columnCollation is what a read reports for a column: the collation it
// declares, and the one it compares with.
type columnCollation struct {
	Declared  string
	Effective string
}

// collationsOf maps each column of the named table to what the read reports
// for it.
func collationsOf(c *qt.C, schema *catalog.Database, table string) map[string]columnCollation {
	c.Helper()
	position := slices.IndexFunc(schema.Tables, func(described catalog.Table) bool { return described.Name == table })
	c.Assert(position, qt.Not(qt.Equals), -1, qt.Commentf("table %s is not in the read", table))
	columns := make(map[string]columnCollation)
	for _, column := range schema.Tables[position].Columns {
		columns[column.Name] = columnCollation{Declared: column.Collate, Effective: column.EffectiveCollate}
	}
	return columns
}

// A column reports a collation only where it declares one other than the one
// it inherits, which on PostgreSQL is its type's: the database default for
// text, C for name, and a domain's own collation for a column of that domain.
// The effective collation is what information_schema.columns names, read by
// migration lint and the inspect document. Each row is what PostgreSQL 17
// keeps in pg_attribute.attcollation beside pg_type.typcollation.
func TestPostgresLiveColumnCollation_ReportsOnlyADeclaredCollation(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	_, err := conn.ExecContext(c.Context(), fmt.Sprintf(`
CREATE DOMAIN "%[1]s".dc AS text COLLATE "C";
CREATE DOMAIN "%[1]s".dn AS text;
CREATE TABLE "%[1]s".t (
	inherited text,
	declared text COLLATE "C",
	restated text COLLATE "default",
	domain_inherited "%[1]s".dc,
	domain_default "%[1]s".dc COLLATE "default",
	domain_declared "%[1]s".dc COLLATE "POSIX",
	plain_domain "%[1]s".dn,
	sized varchar(10) COLLATE "C",
	array_declared text[] COLLATE "C",
	array_inherited text[],
	counted integer,
	named name,
	named_restated name COLLATE "C"
)`, schemaName))
	c.Assert(err, qt.IsNil)

	schema, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})

	c.Assert(err, qt.IsNil)
	c.Assert(collationsOf(c, schema, "t"), qt.DeepEquals, map[string]columnCollation{
		"inherited":        {},
		"declared":         {Declared: "C", Effective: "C"},
		"restated":         {},
		"domain_inherited": {Effective: "C"},
		"domain_default":   {Declared: "default"},
		"domain_declared":  {Declared: "POSIX", Effective: "POSIX"},
		"plain_domain":     {},
		"sized":            {Declared: "C", Effective: "C"},
		"array_declared":   {Declared: "C", Effective: "C"},
		"array_inherited":  {},
		"counted":          {},
		"named":            {Effective: "C"},
		"named_restated":   {Effective: "C"},
	})
}

// On MySQL and MariaDB a character column the author gave no COLLATE takes the
// table's default, and so does one that restates it: COLUMNS reports the
// table's collation for both, and nothing tells them apart. The effective
// collation is COLUMNS.COLLATION_NAME as it stands.
func TestMySQLFamilyLiveColumnCollation_ReportsOnlyADeclaredCollation(t *testing.T) {
	for _, engine := range []dbtarget.Engine{dbtarget.MySQL, dbtarget.MariaDB} {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, engine))
			c.Assert(err, qt.IsNil)
			table := fmt.Sprintf("ptah_column_collation_%d", time.Now().UnixNano())
			c.Cleanup(func() {
				_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
				dbschema.CloseAndWarn(conn)
			})
			_, err = conn.ExecContext(c.Context(), "CREATE TABLE `"+table+"` ("+
				"inherited varchar(10), "+
				"declared varchar(10) COLLATE utf8mb4_bin, "+
				"restated varchar(10) COLLATE utf8mb4_unicode_ci, "+
				"other_charset varchar(10) CHARACTER SET latin1, "+
				"counted int"+
				") DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
			c.Assert(err, qt.IsNil)

			schema, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(collationsOf(c, schema, table), qt.DeepEquals, map[string]columnCollation{
				"inherited":     {Effective: "utf8mb4_unicode_ci"},
				"declared":      {Declared: "utf8mb4_bin", Effective: "utf8mb4_bin"},
				"restated":      {Effective: "utf8mb4_unicode_ci"},
				"other_charset": {Declared: "latin1_swedish_ci", Effective: "latin1_swedish_ci"},
				"counted":       {},
			})
		})
	}
}

// On SQL Server a column that names no COLLATE takes the database default, and
// so does one that restates it; sys.columns keeps the same name for both. The
// database here is case-sensitive, so the case-insensitive column is the one
// that declares.
func TestSQLServerLiveColumnCollation_ReportsOnlyADeclaredCollation(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLServerCollationDatabase(c, provisionSQLServerCollationDatabase(c, sqlServerCSCollation))
	_, err := conn.ExecContext(c.Context(), `CREATE TABLE [dbo].[t] (
	[inherited] nvarchar(10),
	[declared] nvarchar(10) COLLATE `+sqlServerCICollation+`,
	[restated] nvarchar(10) COLLATE `+sqlServerCSCollation+`,
	[narrow] varchar(max),
	[counted] int
)`)
	c.Assert(err, qt.IsNil)

	schema, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"dbo"})

	c.Assert(err, qt.IsNil)
	c.Assert(collationsOf(c, schema, "t"), qt.DeepEquals, map[string]columnCollation{
		"inherited": {},
		"declared":  {Declared: sqlServerCICollation},
		"restated":  {},
		"narrow":    {},
		"counted":   {},
	})
}
