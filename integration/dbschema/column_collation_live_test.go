//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
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
// keeps in pg_attribute.attcollation beside pg_type.typcollation. Only
// COLLATE "default" restates the database default: a collation built from
// the database's own locale is another catalog row, so naming it declares.
func TestPostgresLiveColumnCollation_ReportsOnlyADeclaredCollation(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	_, err := conn.ExecContext(c.Context(), fmt.Sprintf(`
CREATE DOMAIN "%[1]s".dc AS text COLLATE "C";
CREATE DOMAIN "%[1]s".dn AS text;
DO $$ BEGIN
	EXECUTE format('CREATE COLLATION %%I.same_as_default (provider = libc, locale = %%L)',
		'%[1]s', (SELECT datcollate FROM pg_database WHERE datname = current_database()));
END $$;
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
	named_restated name COLLATE "C",
	same_as_default text COLLATE "%[1]s".same_as_default
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
		"same_as_default":  {Declared: "same_as_default", Effective: "same_as_default"},
	})
}

// CockroachDB fills pg_attribute.attcollation and pg_type.typcollation as
// PostgreSQL does, so the same comparison separates a STRING that inherits
// from one that declares a collation.
func TestCockroachDBLiveColumnCollation_ReportsOnlyADeclaredCollation(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.CockroachDB))
	c.Assert(err, qt.IsNil)
	schemaName := fmt.Sprintf("ptah_column_collation_%d", time.Now().UnixNano())
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
		dbschema.CloseAndWarn(conn)
	})
	_, err = conn.ExecContext(c.Context(), `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `CREATE TABLE "`+schemaName+`".t (
	id INT8 PRIMARY KEY,
	inherited STRING,
	declared STRING COLLATE en,
	sized VARCHAR(10) COLLATE de
)`)
	c.Assert(err, qt.IsNil)

	schema, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})

	c.Assert(err, qt.IsNil)
	c.Assert(collationsOf(c, schema, "t"), qt.DeepEquals, map[string]columnCollation{
		"id":        {},
		"inherited": {},
		"declared":  {Declared: "en", Effective: "en"},
		"sized":     {Declared: "de", Effective: "de"},
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

// Two tables whose names differ only in case each keep their own columns and
// their own collation. information_schema compares table names
// case-insensitively on MariaDB, so a read that joined COLUMNS with TABLES on
// the name gave each table the other's columns as well.
func TestMySQLFamilyLiveColumnCollation_CaseVariantTablesKeepTheirOwn(t *testing.T) {
	for _, engine := range []dbtarget.Engine{dbtarget.MySQL, dbtarget.MariaDB} {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, engine))
			c.Assert(err, qt.IsNil)
			lower := fmt.Sprintf("ptah_case_variant_%d", time.Now().UnixNano())
			upper := strings.ToUpper(lower)
			c.Cleanup(func() {
				_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+lower+"`, `"+upper+"`")
				dbschema.CloseAndWarn(conn)
			})
			_, err = conn.ExecContext(c.Context(), "CREATE TABLE `"+lower+"` (id int, a varchar(10)) "+
				"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
			c.Assert(err, qt.IsNil)
			_, err = conn.ExecContext(c.Context(), "CREATE TABLE `"+upper+"` (b varchar(10)) "+
				"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
			c.Assert(err, qt.IsNil)

			schema, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(collationsOf(c, schema, lower), qt.DeepEquals, map[string]columnCollation{
				"id": {},
				"a":  {Effective: "utf8mb4_bin"},
			})
			c.Assert(collationsOf(c, schema, upper), qt.DeepEquals, map[string]columnCollation{
				"b": {Effective: "utf8mb4_unicode_ci"},
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
		"declared":  {Declared: sqlServerCICollation, Effective: sqlServerCICollation},
		"restated":  {},
		"narrow":    {},
		"counted":   {},
	})
}

// ALTER DATABASE ... COLLATE changes the default for columns created after it
// and leaves the existing ones as they are, so a column created before the
// change keeps the old collation and now differs from what a new column
// takes. It reads as declared: the comparison is with the database's
// collation as it stands.
func TestSQLServerLiveColumnCollation_AColumnOlderThanTheDatabaseCollationDeclaresIt(t *testing.T) {
	c := qt.New(t)
	databaseURL := provisionSQLServerCollationDatabase(c, sqlServerCSCollation)
	before, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	_, err = before.ExecContext(c.Context(), `CREATE TABLE [dbo].[t] ([older] nvarchar(10))`)
	dbschema.CloseAndWarn(before)
	c.Assert(err, qt.IsNil)
	databaseName := sqlServerDatabaseNameOf(c, databaseURL)
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.SQLServer))
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(c.Context(), "ALTER DATABASE "+quoteSQLServerIdentifier(databaseName)+
		" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ALTER DATABASE "+quoteSQLServerIdentifier(databaseName)+
		" COLLATE "+sqlServerCICollation+"; ALTER DATABASE "+quoteSQLServerIdentifier(databaseName)+" SET MULTI_USER")
	dbschema.CloseAndWarn(admin)
	c.Assert(err, qt.IsNil)
	conn := connectSQLServerCollationDatabase(c, databaseURL)
	_, err = conn.ExecContext(c.Context(), `ALTER TABLE [dbo].[t] ADD [newer] nvarchar(10)`)
	c.Assert(err, qt.IsNil)

	schema, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"dbo"})

	c.Assert(err, qt.IsNil)
	c.Assert(collationsOf(c, schema, "t"), qt.DeepEquals, map[string]columnCollation{
		"older": {Declared: sqlServerCSCollation, Effective: sqlServerCSCollation},
		"newer": {},
	})
}

// sqlServerDatabaseNameOf is the database a SQL Server URL names in its
// database parameter.
func sqlServerDatabaseNameOf(c *qt.C, databaseURL string) string {
	c.Helper()
	parsed, err := url.Parse(databaseURL)
	c.Assert(err, qt.IsNil)
	name := parsed.Query().Get("database")
	c.Assert(name, qt.Not(qt.Equals), "")
	return name
}
