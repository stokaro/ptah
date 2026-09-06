package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestGenerateSchemaDiffSQLStatements_ASchemaIsCreatedWhereItIsAnObject is the
// enumeration stokaro/ptah#1996 needed, and the dialect list is the decision
// rather than a coincidence.
//
// A schema on SQL Server is an ordinary object inside the connected database,
// and `CREATE SCHEMA` is ordinary DDL. Without it a multi-schema declaration
// could not be applied at all: measured on SQL Server 2022 (16.0.4265.3),
// `CREATE TABLE [app].[widget]` against a database holding only `dbo` answers
// `Msg 2760: The specified schema name "app" either does not exist or you do
// not have permission to use it`.
//
// A schema on MySQL, MariaDB and ClickHouse IS a database, and on Oracle it is
// a USER. Creating one is `CREATE DATABASE` or `CREATE USER`, an administrative
// act outside what a schema migration owns, so those rows assert the ABSENCE --
// a fix applied to the shared planner without the dialect check would have a
// migration create databases nobody asked for.
func TestGenerateSchemaDiffSQLStatements_ASchemaIsCreatedWhereItIsAnObject(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		home    string
		want    string
	}{
		{
			name:    "sqlserver creates it",
			dialect: "sqlserver",
			home:    "dbo",
			want:    "IF SCHEMA_ID('extra') IS NULL\n    EXEC('CREATE SCHEMA [extra]')",
		},
		{
			// The control the guard needs: PostgreSQL has done this since
			// stokaro/ptah#1276, and a change to the shared MySQL-family
			// planner must not take it away.
			name:    "postgres creates it",
			dialect: "postgres",
			home:    "public",
			want:    `CREATE SCHEMA IF NOT EXISTS "extra"`,
		},
		{name: "mysql does not", dialect: "mysql", home: "testdb"},
		{name: "mariadb does not", dialect: "mariadb", home: "testdb"},
		{name: "clickhouse does not", dialect: "clickhouse", home: "default"},
		{name: "oracle does not", dialect: "oracle", home: "APP"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements := planSchemaPrecondition(c, test.dialect, test.home)

			c.Assert(schemaStatements(statements), qt.DeepEquals, wantedSchemaStatements(test.want),
				qt.Commentf("%v", statements))
		})
	}
}

// TestGenerateSchemaDiffSQLStatements_ACreatedSchemaCarriesItsComment is
// stokaro/ptah#2618: the plan reaches a schema through the qualified name of an
// object inside it, so the name arrived and the declaration's comment did not.
//
// `ptah schema render` emitted COMMENT ON SCHEMA for the same document all
// along, so a `schema apply` against a database without the schema created it
// unnamed, and every comparison afterwards saw a schema whose comment the
// declaration has and the database does not -- with nothing able to fix it.
//
// The two rows are the two spellings, not two dialects chosen for coverage:
// PostgreSQL writes the comment as DDL a later inspection reads back, and SQL
// Server has no schema comment at all and writes the author's sentence as the
// leading `--` line its renderer already wrote for a direct render.
func TestGenerateSchemaDiffSQLStatements_ACreatedSchemaCarriesItsComment(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		home    string
		want    string
	}{
		{
			name:    "postgres writes COMMENT ON SCHEMA",
			dialect: "postgres",
			home:    "public",
			want:    `COMMENT ON SCHEMA "extra" IS 'the extra schema'`,
		},
		{
			name:    "sqlserver writes the comment line",
			dialect: "sqlserver",
			home:    "dbo",
			want:    "-- the extra schema",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements := planCommentedSchema(c, test.dialect, test.home)

			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want,
				qt.Commentf("%v", statements))
		})
	}
}

// TestGenerateSchemaDiffSQLStatements_AnUndeclaredSchemaIsStillCreated is the
// control that keeps the lookup from becoming a gate.
//
// A schema reached only through an object's qualifier -- one the document never
// declares -- has been created since stokaro/ptah#1276, and it has to keep
// being created: the statement exists because the object needs the schema to
// be there, and withholding it over a missing comment would fail the migration
// on `schema "extra" does not exist`.
func TestGenerateSchemaDiffSQLStatements_AnUndeclaredSchemaIsStillCreated(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "W", Name: "widget", Schema: "extra"}},
		Fields: []schemamodel.Field{{StructName: "W", Name: "id", Type: "INT", Primary: true}},
	}
	live := &catalog.Database{Schemas: []catalog.Schema{{Name: "public"}}}

	statements := planForDialect(c, declared, live, "postgres", "public")

	c.Assert(schemaStatements(statements), qt.DeepEquals,
		[]string{`CREATE SCHEMA IF NOT EXISTS "extra"`}, qt.Commentf("%v", statements))
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "COMMENT ON SCHEMA")
}

// TestGenerateSchemaDiffSQLStatements_TheSchemaComesBeforeTheObject pins the
// order, which is the whole point of a precondition.
//
// stokaro/ptah#1276 records the shape this replaces on PostgreSQL: the
// preconditions were derived inside the table phase, so they were emitted after
// the sequences and functions and the run failed on `schema "s_misc" does not
// exist` with the CREATE SCHEMA seventeen statements further down.
func TestGenerateSchemaDiffSQLStatements_TheSchemaComesBeforeTheObject(t *testing.T) {
	c := qt.New(t)

	statements := planSchemaPrecondition(c, "sqlserver", "dbo")

	c.Assert(statements, qt.Not(qt.HasLen), 0)
	c.Assert(statements[0], qt.Contains, "IF SCHEMA_ID('extra') IS NULL")
	c.Assert(statements[0], qt.Contains, "CREATE SCHEMA [extra]")
}

// TestGenerateSchemaDiffSQLStatements_ASingleSchemaApplyCreatesNoSchema is the
// non-vacuity control, and it is what keeps an unchanged document a no-op.
//
// The schemas are read off the qualified names the comparison already produced,
// so a declaration that names none contributes none. A rule that emitted the
// connection's own schema instead would put a statement in every plan, and a
// re-apply of an inspected document would stop being the clean no-op it has to
// be.
func TestGenerateSchemaDiffSQLStatements_ASingleSchemaApplyCreatesNoSchema(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "W", Name: "widget"}},
		Fields: []schemamodel.Field{{StructName: "W", Name: "id", Type: "INT", Primary: true}},
	}

	statements := planWithSQLServer(c, declared, &catalog.Database{})

	c.Assert(schemaStatements(statements), qt.HasLen, 0, qt.Commentf("%v", statements))
	// Non-vacuity: the table really is planned, so an empty plan cannot pass.
	c.Assert(statements, qt.Not(qt.HasLen), 0)
}

// TestGenerateSchemaDiffSQLStatements_EveryAddedFamilyNamesItsSchema is the
// other half of stokaro/ptah#1276's lesson, and the reason this is a sweep.
//
// Preconditions derived from the tables alone covered none of the other objects
// planned in the same run, and each of them answers the same Msg 2760. A
// property is in the list for a reason of its own: it is the one addition that
// needs a schema without creating anything in it.
func TestGenerateSchemaDiffSQLStatements_EveryAddedFamilyNamesItsSchema(t *testing.T) {
	tests := []struct {
		name    string
		declare func(*schemamodel.Database)
	}{
		{
			name: "a table",
			declare: func(d *schemamodel.Database) {
				d.Tables = append(d.Tables, schemamodel.Table{StructName: "W", Name: "widget", Schema: "extra"})
				d.Fields = append(d.Fields, schemamodel.Field{StructName: "W", Name: "id", Type: "INT", Primary: true})
			},
		},
		{
			name: "a view",
			declare: func(d *schemamodel.Database) {
				d.Views = append(d.Views, schemamodel.View{
					StructName: "V", Name: "extra.v1", Body: "SELECT 1 AS one",
				})
			},
		},
		{
			name: "a synonym",
			declare: func(d *schemamodel.Database) {
				d.Synonyms = append(d.Synonyms, schemamodel.Synonym{
					Name: "s1", Schema: "extra", Target: "other.dbo.widget",
				})
			},
		},
		{
			name: "an extended property",
			declare: func(d *schemamodel.Database) {
				d.ExtendedProperties = append(d.ExtendedProperties, schemamodel.ExtendedProperty{
					Name: "ptah_flag", Schema: "extra", Value: "on",
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "dbo"}, {Name: "extra"}}}
			test.declare(declared)

			statements := planWithSQLServer(c, declared, &catalog.Database{
				Schemas: []catalog.Schema{{Name: "dbo"}},
			})

			c.Assert(schemaStatements(statements), qt.HasLen, 1, qt.Commentf("%v", statements))
		})
	}
}

// planSchemaPrecondition plans one table in a declared schema the database does
// not have, which is the shape the issue reported.
func planSchemaPrecondition(c *qt.C, dialect, home string) []string {
	c.Helper()
	declared := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: home}, {Name: "extra"}},
		Tables:  []schemamodel.Table{{StructName: "W", Name: "widget", Schema: "extra"}},
		Fields:  []schemamodel.Field{{StructName: "W", Name: "id", Type: "INT", Primary: true}},
	}
	live := &catalog.Database{Schemas: []catalog.Schema{{Name: home}}}
	return planForDialect(c, declared, live, dialect, home)
}

// planCommentedSchema plans one commented schema holding one table, which is
// the smallest document that reaches the precondition at all: the schema is
// needed because the table names it, and the comment has no other route.
func planCommentedSchema(c *qt.C, dialect, home string) []string {
	c.Helper()
	declared := &schemamodel.Database{
		Schemas: []schemamodel.Schema{
			{Name: home},
			{Name: "extra", Comment: "the extra schema"},
		},
		Tables: []schemamodel.Table{{StructName: "W", Name: "widget", Schema: "extra"}},
		Fields: []schemamodel.Field{{StructName: "W", Name: "id", Type: "INT", Primary: true}},
	}
	live := &catalog.Database{Schemas: []catalog.Schema{{Name: home}}}
	return planForDialect(c, declared, live, dialect, home)
}

func planWithSQLServer(c *qt.C, declared *schemamodel.Database, live *catalog.Database) []string {
	c.Helper()
	return planForDialect(c, declared, live, "sqlserver", "dbo")
}

func planForDialect(
	c *qt.C,
	declared *schemamodel.Database,
	live *catalog.Database,
	dialect, home string,
) []string {
	c.Helper()
	semantics := schemaPreconditionSemantics(dialect, home)
	opts := config.DefaultCompareOptions()
	opts.Dialect = dialect
	opts.IdentifierSemantics = &semantics
	diff := schemadiff.CompareWithOptions(declared, live, opts)
	diff.IdentifierSemantics = &semantics

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, dialect)
	c.Assert(err, qt.IsNil)
	return statements
}

// schemaPreconditionSemantics is what a live connection reports. SQL Server
// needs its catalog collation and the names it resolved, because an unresolved
// name there shares one conservative conflict key.
func schemaPreconditionSemantics(dialect, home string) identifier.Semantics {
	if dialect == "sqlserver" {
		return identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
			WithResolvedNames([]identifier.ResolvedName{
				{Name: "dbo", Key: "dbo"}, {Name: "extra", Key: "extra"},
				{Name: "widget", Key: "widget"}, {Name: "id", Key: "id"},
				{Name: "v1", Key: "v1"}, {Name: "s1", Key: "s1"},
				{Name: "ptah_flag", Key: "ptah_flag"},
			})
	}
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = home
	return semantics
}

// schemaStatements are the statements that create a schema, in order.
func schemaStatements(statements []string) []string {
	created := make([]string, 0, len(statements))
	for _, statement := range statements {
		if strings.Contains(statement, "CREATE SCHEMA") || strings.Contains(statement, "CREATE DATABASE") {
			created = append(created, statement)
		}
	}
	return created
}

// wantedSchemaStatements turns a row's expectation into the list to compare
// against, so a row that wants none and a row that wants one are one claim.
func wantedSchemaStatements(want string) []string {
	if want == "" {
		return make([]string, 0)
	}
	return []string{want}
}
