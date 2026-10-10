package atlasschema

// White-box testing required: baseline statement validation is private to the
// rehearsal, whose public entry point requires a live database connection.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

const ydbTable = "CREATE TABLE items (id Int64 NOT NULL, PRIMARY KEY (id));"

// TestRehearsalBaselineRefusesAPlannedBaseline drives the guard with what the
// planner writes for a target holding a routine, a trigger and a changed
// comment on an extension, on a dev database the run does not own. The whole baseline is
// refused before its first statement runs, and the refusal names the statement
// by its number and its first line.
func TestRehearsalBaselineRefusesAPlannedBaseline(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		info    catalog.ServerInfo
		wantErr string
	}{
		{
			name: "a PostgreSQL extension comment",
			sql: "CREATE EXTENSION IF NOT EXISTS pg_trgm;\n" +
				"COMMENT ON EXTENSION pg_trgm IS 'trigram matching';\n",
			info: catalog.ServerInfo{Dialect: "postgres", Capabilities: capability.Postgres18()},
			wantErr: `baseline statement 2 \(COMMENT ON EXTENSION "pg_trgm" IS 'trigram matching'\) cannot be rehearsed: ` +
				`postgres rehearsal baseline refuses COMMENT ON global metadata because its effects cannot be confined to the dev database realm; ` +
				`use a docker:// or docker\+<driver>:// dev URL, since a server declared disposable keeps this after the run`,
		},
		{
			name: "a PostgreSQL routine",
			sql: "CREATE TABLE items (id int PRIMARY KEY, n int);\n" +
				"CREATE FUNCTION item_count() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM items $$;\n",
			info: catalog.ServerInfo{Dialect: "postgres", Capabilities: capability.Postgres18()},
			wantErr: `(?s)baseline statement 2 \(CREATE .*item_count.*\) cannot be rehearsed: ` +
				`postgres rehearsal baseline refuses CREATE routine definition because .*PTAH_DEV_SERVER_DISPOSABLE=1.*`,
		},
		{
			name: "a MySQL trigger",
			sql: "CREATE TABLE items (id int PRIMARY KEY, n int);\n" +
				"CREATE TRIGGER items_touch BEFORE INSERT ON items FOR EACH ROW SET NEW.n = 1;\n",
			info: catalog.ServerInfo{Dialect: "mysql", Schema: "ptah_dev", Capabilities: capability.MySQL84()},
			wantErr: `(?s)baseline statement 2 \(CREATE TRIGGER .*items_touch.*\) cannot be rehearsed: ` +
				`mysql rehearsal baseline refuses CREATE executable stored body because .*PTAH_DEV_SERVER_DISPOSABLE=1.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired, _, err := sqlschema.Read([]byte(test.sql), test.info.Dialect)
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDialect(t.Context(), &desired, &catalog.Database{}, test.info.Dialect, runtime)
			c.Assert(err, qt.IsNil)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				t.Context(), runtime, diff, test.info.Dialect, planner.Options{Capabilities: test.info.Capabilities},
			)
			c.Assert(err, qt.IsNil)

			c.Assert(guardRehearsalBaseline(statements, test.info), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestRehearsalBaselineRefusesARealmRootGrant drives the guard with a grant on
// a YDB dev realm's root, which a baseline never writes: the rebuild leaves
// the database root's permissions out, because the realm's root survives a
// reset in the middle of a run and would carry them to the next target.
func TestRehearsalBaselineRefusesARealmRootGrant(t *testing.T) {
	c := qt.New(t)
	realm := catalog.ServerInfo{Dialect: "ydb", URL: "ydb://localhost:2136/local?dev_realm=abc"}

	err := guardRehearsalBaseline([]string{
		ydbTable,
		"GRANT 'ydb.access.grant' ON `/local/ptah_dev/abc` TO `ACCESS-ADMINS`;",
	}, realm)

	c.Assert(err, qt.ErrorMatches, `baseline statement 2 \(GRANT .*\) cannot be rehearsed: ydb rehearsal baseline refuses GRANT permission change .*`)
}

// TestStatementExcerpt pins what a refusal quotes: the first line that is not
// a comment, cut to 80 runes. A YDB translation setting is not a comment, and
// the guard refuses it, so it is quoted; on another dialect the same line is a
// comment. A statement of comments alone is quoted by its first line, cut too.
func TestStatementExcerpt(t *testing.T) {
	long := "CREATE TABLE a_table_whose_name_and_columns_run_past_the_limit_of_the_excerpt (id bigint PRIMARY KEY)"
	tests := []struct {
		name      string
		statement string
		dialect   string
		want      string
	}{
		{"a statement", "CREATE USER `reader`;", "ydb", "CREATE USER `reader`;"},
		{"a comment line", "-- Recreates the user.\nCREATE USER `reader`;", "ydb", "CREATE USER `reader`;"},
		{"a YDB translation setting", "--!syntax_v1\nCREATE USER `reader`;", "ydb", "--!syntax_v1"},
		{"the same line elsewhere", "--!syntax_v1\nCREATE ROLE reader;", "postgres", "CREATE ROLE reader;"},
		{"a long line", long, "postgres", long[:80] + "..."},
		{"comments alone", "-- " + long + "\n-- second", "postgres", ("-- " + long)[:80] + "..."},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(statementExcerpt(test.statement, test.dialect), qt.Equals, test.want)
		})
	}
}
