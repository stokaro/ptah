package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
)

// enumSchema builds one table whose single non-key column is typed with a
// declared enum of the given name. The two spellings differ only in the name, so
// a difference in the rendered DDL can come from nothing else.
func enumSchema(enumName string) *goschema.Database {
	return &goschema.Database{
		Enums:  []goschema.Enum{{Name: enumName, Values: []string{"active", "archived"}}},
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "User", Name: "s", Type: enumName, Nullable: true},
		},
	}
}

// TestRender_EnumIdentityIsTheDeclarationNotTheNamePrefix pins that a declared
// enum is modeled as an enum whatever it is called.
//
// The inline-enum rewrite used to be guarded by
// strings.HasPrefix(field.Type, "enum_"), an undocumented convention that
// appears nowhere in `ptah schema annotations`. On the four dialects that model
// enums on the column, an enum named "status_kind" was therefore left as the
// bare type name -- and because those dialects also skip standalone CREATE TYPE,
// its values disappeared and the DDL named a type the server never heard of
// (stokaro/ptah#931 item 1).
//
// Each row renders both spellings and asserts they agree once the name itself is
// substituted out, which is stronger than asserting a fixed string: it cannot
// pass by both sides being equally wrong in some new way.
func TestRender_EnumIdentityIsTheDeclarationNotTheNamePrefix(t *testing.T) {

	for _, dialect := range []string{"mysql", "mariadb", "sqlite", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			plain, err := renderer.GetOrderedCreateStatements(enumSchema("status_kind"), dialect)
			c.Assert(err, qt.IsNil)
			prefixed, err := renderer.GetOrderedCreateStatements(enumSchema("enum_status"), dialect)
			c.Assert(err, qt.IsNil)

			c.Assert(strings.Join(plain, "\n"), qt.Equals, strings.Join(prefixed, "\n"))
			c.Assert(strings.Join(plain, "\n"), qt.Contains, "active")
			c.Assert(strings.Join(plain, "\n"), qt.Not(qt.Contains), "status_kind")
		})
	}
}

// TestRender_PostgreSQLEmitsTheEnumTypeUnderEitherName is the control on the
// dialect that models enums as standalone types: it was already correct on the
// CREATE path under both spellings and must stay so.
func TestRender_PostgreSQLEmitsTheEnumTypeUnderEitherName(t *testing.T) {

	tests := []struct {
		name string
		want string
	}{
		{name: "status_kind", want: `CREATE TYPE "status_kind" AS ENUM ('active', 'archived');`},
		{name: "enum_status", want: `CREATE TYPE "enum_status" AS ENUM ('active', 'archived');`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(enumSchema(test.name), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// matviewSchema declares one table and one materialized view over it.
func matviewSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "T1", Name: "t1"}},
		Fields: []goschema.Field{{StructName: "T1", Name: "id", Type: "BIGINT", Primary: true}},
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "MV1", Name: "mv1", Body: "SELECT id FROM t1",
		}},
	}
}

// TestRender_MaterializedViewIsRefusedWhereApplyRefusesIt pins that `render`
// gives the same answer `apply` gives for a materialized view.
//
// The converter copied Views and Triggers for these four dialects but never
// MaterializedViews, so `ptah schema render` dropped every declared matview with
// no comment, no warning and exit 0, while `ptah schema apply` against the same
// target failed with an unsupported-feature error. Validate-with-render is the
// documented workflow, so the silent side was the one that had to move
// (stokaro/ptah#931 item 3).
func TestRender_MaterializedViewIsRefusedWhereApplyRefusesIt(t *testing.T) {

	for _, dialect := range []string{"mysql", "mariadb", "sqlite", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(matviewSchema(), dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, ".*materialized views are not supported.*")
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRender_PostgreSQLStillEmitsMaterializedViews is the non-interference
// control: the refusal above must be about targets that cannot host a matview,
// not about matviews.
func TestRender_PostgreSQLStillEmitsMaterializedViews(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(matviewSchema(), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `CREATE MATERIALIZED VIEW "mv1"`)
}

func TestRender_PostgreSQLExtensionCreatesItsInstallationSchemaFirst(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Extensions: []goschema.Extension{{
			Name:        "pgcrypto",
			Schema:      " Extension Store ",
			IfNotExists: true,
		}},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE SCHEMA IF NOT EXISTS \" Extension Store \";\n",
		"CREATE EXTENSION IF NOT EXISTS \"pgcrypto\" WITH SCHEMA \" Extension Store \";\n",
	})
}

func TestRender_PostgreSQLExtensionCreatesWhitespaceOnlyInstallationSchemaFirst(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pgcrypto", Schema: " "}},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE SCHEMA IF NOT EXISTS \" \";\n",
		"CREATE EXTENSION \"pgcrypto\" WITH SCHEMA \" \";\n",
	})
}

func TestRender_PostgreSQLExtensionKeepsDistinctWhitespaceInSchemaIdentity(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "Extension Store"}},
		Extensions: []goschema.Extension{{
			Name: "pgcrypto", Schema: " Extension Store ",
		}},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE SCHEMA IF NOT EXISTS \"Extension Store\";\n",
		"CREATE SCHEMA IF NOT EXISTS \" Extension Store \";\n",
		"CREATE EXTENSION \"pgcrypto\" WITH SCHEMA \" Extension Store \";\n",
	})
}

func TestRender_PostgreSQLExtensionDoesNotDuplicateDeclaredSchema(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Schemas:    []goschema.Schema{{Name: "extensions"}},
		Extensions: []goschema.Extension{{Name: "pgcrypto", Schema: "extensions"}},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE SCHEMA IF NOT EXISTS \"extensions\";\n",
		"CREATE EXTENSION \"pgcrypto\" WITH SCHEMA \"extensions\";\n",
	})
}

func TestRender_PostgreSQLExtensionDoesNotCreateSystemInstallationSchema(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Extensions: []goschema.Extension{{
			Name: "plpgsql", Schema: "pg_catalog", Version: "1.0", IfNotExists: true,
		}},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE EXTENSION IF NOT EXISTS \"plpgsql\" WITH SCHEMA \"pg_catalog\" VERSION '1.0';\n",
	})
}

func TestRender_ExtensionInstallationSchemaSupportedTargets(t *testing.T) {
	database := &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pgcrypto", Schema: "extensions"}},
	}

	for _, dialect := range []string{platform.Postgres, platform.YugabyteDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(database, dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"CREATE SCHEMA IF NOT EXISTS \"extensions\";\n",
				"CREATE EXTENSION \"pgcrypto\" WITH SCHEMA \"extensions\";\n",
			})
		})
	}
}

func TestRender_ExtensionInstallationSchemaUnsupportedTargetsFailBeforeSQL(t *testing.T) {
	database := &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pgcrypto", Schema: "extensions"}},
	}
	node := &ast.ExtensionNode{Name: "pgcrypto", Schema: "extensions"}

	for _, dialect := range []string{platform.CockroachDB, platform.Spanner} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(database, dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, dialect+` does not support PostgreSQL extension installation schema "extensions" for extension "pgcrypto"`)
			c.Assert(statements, qt.IsNil)

			direct, directErr := renderer.RenderSQL(dialect, node)
			c.Assert(directErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(direct, qt.Equals, "")
		})
	}
}

func TestRender_WhitespaceOnlyExtensionInstallationSchemaUnsupportedTargetsFailBeforeSQL(t *testing.T) {
	database := &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pgcrypto", Schema: " "}},
	}
	node := &ast.ExtensionNode{Name: "pgcrypto", Schema: " "}

	for _, dialect := range []string{platform.CockroachDB, platform.Spanner} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(database, dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, dialect+` does not support PostgreSQL extension installation schema " " for extension "pgcrypto"`)
			c.Assert(statements, qt.IsNil)

			direct, directErr := renderer.RenderSQL(dialect, node)
			c.Assert(directErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(direct, qt.Equals, "")
		})
	}
}

func TestRender_UnsupportedExtensionDoesNotCreateItsPostgreSQLSchema(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pgcrypto", Schema: "extensions"}},
	}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "CREATE DATABASE")
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "Extension pgcrypto not supported in MySQL")
}

// clickHouseSchema declares a plain view alongside the object kinds Ptah's
// ClickHouse model cannot express, plus a table carrying a column CHECK.
func clickHouseSchema() *goschema.Database {
	return &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pg_trgm"}},
		Sequences:  []goschema.Sequence{{Name: "chk_seq"}},
		Roles:      []goschema.Role{{Name: "chk_role"}},
		Functions:  []goschema.Function{{Name: "chk_f", Returns: "integer", Language: "sql", Body: "SELECT 1;"}},
		Tables:     []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", Name: "n", Type: "INTEGER", Nullable: true, Check: "n > 0"},
		},
		Views:             []goschema.View{{StructName: "V", Name: "chk_v", Body: "SELECT id FROM t"}},
		MaterializedViews: []goschema.MaterializedView{{StructName: "MV", Name: "chk_mv", Body: "SELECT id FROM t"}},
		Triggers: []goschema.Trigger{{
			StructName: "TR", Name: "chk_trg", Table: "t",
			Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "SELECT 1",
		}},
	}
}

// TestRender_ClickHouseRendersViewsAndNamesUnsupportedObjects pins that plain
// views are executable while the remaining unsupported object kinds stay
// visible as diagnostics.
//
// The ClickHouse renderer has implemented a notSupported() diagnostic for each
// of these kinds all along; the converter dropped the AST nodes before any of
// them ran. A schema declaring all of them rendered one CREATE TABLE and exited
// 0, against a PostgreSQL control that planned eight statements
// (stokaro/ptah#931 item 7).
func TestRender_ClickHouseRendersViewsAndNamesUnsupportedObjects(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(clickHouseSchema(), platform.ClickHouse)
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")

	tests := []struct {
		name string
		want string
	}{
		{name: "extension", want: `-- CLICKHOUSE: CREATE EXTENSION "pg_trgm" is not supported`},
		{name: "sequence", want: `-- CLICKHOUSE: CREATE SEQUENCE "chk_seq" is not supported`},
		{name: "role", want: `-- CLICKHOUSE: CREATE ROLE "chk_role" is not supported`},
		{name: "function", want: `-- CLICKHOUSE: CREATE FUNCTION "chk_f" is not supported`},
		{name: "trigger", want: `-- CLICKHOUSE: CREATE TRIGGER "chk_trg" is not supported`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rendered, qt.Contains, test.want)
		})
	}
	c.Assert(rendered, qt.Contains, "CREATE VIEW `chk_v` AS\nSELECT id FROM t")
	c.Assert(
		rendered,
		qt.Contains,
		"CREATE MATERIALIZED VIEW `chk_mv` ENGINE = MergeTree ORDER BY tuple() AS\nSELECT id FROM t",
	)
}

// TestRender_ClickHouseKeepsAColumnCheckAsANamedConstraint pins that a column
// CHECK survives into ClickHouse DDL.
//
// ClickHouse has no column-level CHECK clause but does have
// `CONSTRAINT <name> CHECK <expr>`, so the constraint is promoted rather than
// dropped. Measured on a live ClickHouse: the rendered statement executes, the
// constraint appears in SHOW CREATE TABLE, and a violating INSERT is rejected
// with `Code: 469 ... VIOLATED_CONSTRAINT` while a satisfying one succeeds, so
// it is enforced rather than decorative. (213 is what clickhouse-client exits
// with, not what the server reports.) The name is not optional -- an unnamed
// `CHECK (expr)` in a column list is a syntax error there (code 62) -- so one is
// synthesized when the author did not give one (stokaro/ptah#931 item 7).
func TestRender_ClickHouseKeepsAColumnCheckAsANamedConstraint(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(clickHouseSchema(), platform.ClickHouse)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "CONSTRAINT t_n_check CHECK (n > 0)")
}

// sequenceSchema declares one standalone sequence and one table.
func sequenceSchema() *goschema.Database {
	return &goschema.Database{
		Sequences: []goschema.Sequence{{Name: "order_number_seq", AsType: "bigint", Start: &sequenceStart}},
		Tables:    []goschema.Table{{StructName: "T1", Name: "t1"}},
		Fields:    []goschema.Field{{StructName: "T1", Name: "id", Type: "BIGINT", Primary: true}},
	}
}

// sequenceStart is addressable so sequenceSchema can point Start at it.
var sequenceStart = int64(1000)

// TestRender_SequencesCapabilityAgreesWithTheGenerator pins the capability
// registry to what the generator actually does, for EVERY registered dialect.
//
// capability.MariaDB1011 declared Sequences: true while no code path emitted,
// read or planned a sequence for the mariadb dialect -- a flag promising a
// capability that did not exist (stokaro/ptah#931 item 8). This test is the
// thing that stops that recurring: a preset can only claim Sequences if
// rendering a declared sequence for that dialect produces an actual
// CREATE SEQUENCE statement.
func TestRender_SequencesCapabilityAgreesWithTheGenerator(t *testing.T) {

	dialects := []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse,
		platform.SQLite, platform.SQLServer, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(sequenceSchema(), dialect)
			c.Assert(err, qt.IsNil)

			c.Assert(emitsExecutableCreateSequence(statements),
				qt.Equals, capability.ForDialect(dialect).Has(capability.Sequences),
				qt.Commentf("%s: the Sequences capability and the generator disagree", dialect))
		})
	}
}

// emitsExecutableCreateSequence reports whether the rendered statements contain
// a CREATE SEQUENCE the server would execute, as opposed to one named inside a
// not-supported comment. The distinction is the whole point of the test above:
// a comment mentioning CREATE SEQUENCE is exactly what a target WITHOUT the
// capability emits.
func emitsExecutableCreateSequence(statements []string) bool {
	for _, statement := range statements {
		for line := range strings.SplitSeq(statement, "\n") {
			trimmed := strings.TrimSpace(line)
			if !strings.HasPrefix(trimmed, "--") && strings.Contains(trimmed, "CREATE SEQUENCE") {
				return true
			}
		}
	}
	return false
}

// TestRender_MariaDBReportsTheSequenceItCannotGenerate is the other half of item
// 8: the flag being false must not mean the declared object vanishes.
func TestRender_MariaDBReportsTheSequenceItCannotGenerate(t *testing.T) {

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(sequenceSchema(), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, "CREATE SEQUENCE order_number_seq not supported in "+dialect)
		})
	}
}
