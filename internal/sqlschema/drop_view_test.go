package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// viewNamesOf lists the views of the database, in order.
func viewNamesOf(database schemamodel.Database) []string {
	names := make([]string, 0, len(database.Views))
	for _, view := range database.Views {
		names = append(names, view.Name)
	}
	return names
}

// materializedViewNamesOf lists the materialized views of the database, in order.
func materializedViewNamesOf(database schemamodel.Database) []string {
	names := make([]string, 0, len(database.MaterializedViews))
	for _, view := range database.MaterializedViews {
		names = append(names, view.Name)
	}
	return names
}

// providerEngine names an engine a provider serves, which no descriptor
// describes.
const providerEngine = "fixture"

const (
	viewFixture    = "CREATE TABLE t (id int PRIMARY KEY);\nCREATE VIEW v AS SELECT id FROM t;\nCREATE VIEW w AS SELECT id FROM t;\n"
	matViewFixture = "CREATE TABLE t (id int PRIMARY KEY);\nCREATE MATERIALIZED VIEW m AS SELECT id FROM t;\nCREATE MATERIALIZED VIEW n AS SELECT id FROM t;\n"
	chMatViewBody  = "CREATE TABLE t (id Int32) ENGINE = MergeTree ORDER BY id;\nCREATE MATERIALIZED VIEW m AS SELECT id FROM t;\n"
)

// A schema file is a script the server runs in order, so a view it creates and then
// drops is not in the schema, and a comparison would otherwise plan the view back.
func TestRead_DropView_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		sql       string
		wantViews []string
	}{
		{name: "PostgreSQL", dialect: "postgres", sql: viewFixture + "DROP VIEW v;", wantViews: []string{"w"}},
		{name: "SQLite", dialect: "sqlite", sql: viewFixture + "DROP VIEW v;", wantViews: []string{"w"}},
		{name: "MySQL", dialect: "mysql", sql: viewFixture + "DROP VIEW v;", wantViews: []string{"w"}},
		{name: "MariaDB", dialect: "mariadb", sql: viewFixture + "DROP VIEW v;", wantViews: []string{"w"}},
		{name: "SQL Server", dialect: "sqlserver", sql: viewFixture + "DROP VIEW v;", wantViews: []string{"w"}},
		{name: "CockroachDB", dialect: "cockroachdb", sql: viewFixture + "DROP VIEW v;", wantViews: []string{"w"}},
		{name: "YugabyteDB", dialect: "yugabytedb", sql: viewFixture + "DROP VIEW v;", wantViews: []string{"w"}},
		{name: "ClickHouse DROP TABLE removes a view", dialect: "clickhouse", sql: viewFixture + "DROP TABLE v;", wantViews: []string{"w"}},
		{name: "two views in one statement", dialect: "postgres", sql: viewFixture + "DROP VIEW v, w;", wantViews: make([]string, 0)},
		{name: "the name folds as the server folds it", dialect: "postgres", sql: viewFixture + "DROP VIEW V;", wantViews: []string{"w"}},
		{name: "the default schema is the bare name", dialect: "postgres", sql: viewFixture + "DROP VIEW public.v;", wantViews: []string{"w"}},
		{name: "IF EXISTS of a view nothing declares", dialect: "postgres", sql: viewFixture + "DROP VIEW IF EXISTS nope;", wantViews: []string{"v", "w"}},
		{
			name: "a view created again after the drop", dialect: "postgres",
			sql:       viewFixture + "DROP VIEW v;\nCREATE VIEW v AS SELECT 1 AS id;",
			wantViews: []string{"w", "v"},
		},
		{
			// The opening of a script that may run twice.
			name: "dropped before anything declares it", dialect: "sqlite",
			sql:       "DROP VIEW IF EXISTS v;\nCREATE VIEW v AS SELECT 1 AS id;",
			wantViews: []string{"v"},
		},
		{
			// MySQL keeps a view that reads a dropped view, and it errors when read.
			name: "MySQL, a view another view reads", dialect: "mysql",
			sql:       viewFixture + "CREATE VIEW x AS SELECT id FROM v;\nDROP VIEW v;",
			wantViews: []string{"w", "x"},
		},
		{
			name: "SQLite, a view another view reads", dialect: "sqlite",
			sql:       viewFixture + "CREATE VIEW x AS SELECT id FROM v;\nDROP VIEW v;",
			wantViews: []string{"w", "x"},
		},
		{
			// A read with no dialect has no server to ask, and takes the drop.
			name: "no dialect, a view another view reads", dialect: "",
			sql:       viewFixture + "CREATE VIEW x AS SELECT id FROM v;\nDROP VIEW v;",
			wantViews: []string{"w", "x"},
		},
		{name: "an engine a provider serves", dialect: providerEngine, sql: viewFixture + "DROP VIEW v;", wantViews: []string{"w"}},
		{
			// MySQL answers `Unknown table` for the view and drops nothing only
			// without IF EXISTS.
			name: "MySQL, IF EXISTS skips a table holding the name", dialect: "mysql",
			sql:       viewFixture + "DROP VIEW IF EXISTS t;",
			wantViews: []string{"v", "w"},
		},
		{name: "MariaDB, IF EXISTS skips a table holding the name", dialect: "mariadb", sql: viewFixture + "DROP VIEW IF EXISTS t;", wantViews: []string{"v", "w"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(viewNamesOf(database), qt.DeepEquals, test.wantViews)
		})
	}
}

func TestRead_DropMaterializedView_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		sql      string
		wantMats []string
	}{
		{name: "PostgreSQL", dialect: "postgres", sql: matViewFixture + "DROP MATERIALIZED VIEW m;", wantMats: []string{"n"}},
		{name: "CockroachDB", dialect: "cockroachdb", sql: matViewFixture + "DROP MATERIALIZED VIEW m;", wantMats: []string{"n"}},
		{name: "YugabyteDB", dialect: "yugabytedb", sql: matViewFixture + "DROP MATERIALIZED VIEW m;", wantMats: []string{"n"}},
		{name: "IF EXISTS of one nothing declares", dialect: "postgres", sql: matViewFixture + "DROP MATERIALIZED VIEW IF EXISTS nope;", wantMats: []string{"m", "n"}},
		{name: "ClickHouse drops one with DROP VIEW", dialect: "clickhouse", sql: chMatViewBody + "DROP VIEW m;", wantMats: make([]string, 0)},
		{name: "and with SYNC", dialect: "clickhouse", sql: chMatViewBody + "DROP VIEW IF EXISTS m SYNC;", wantMats: make([]string, 0)},
		{name: "ClickHouse DROP TABLE removes a materialized view", dialect: "clickhouse", sql: chMatViewBody + "DROP TABLE m;", wantMats: make([]string, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := readWithPolicyOwners([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(materializedViewNamesOf(database), qt.DeepEquals, test.wantMats)
		})
	}
}

// What the server drops with a view goes with it: the triggers on it, the grants on it
// and, on a materialized view, its indexes. Measured on PostgreSQL 18.
func TestRead_DropView_TakesWhatBelongsToTheView(t *testing.T) {
	c := qt.New(t)

	database, _, err := readWithPolicyOwners([]byte(
		"CREATE TABLE t (id int PRIMARY KEY);\n"+
			"CREATE VIEW v AS SELECT id FROM t;\n"+
			"CREATE VIEW keep AS SELECT id FROM t;\n"+
			"CREATE MATERIALIZED VIEW m AS SELECT id FROM t;\n"+
			"CREATE INDEX m_id ON m (id);\n"+
			"CREATE INDEX t_id ON t (id);\n"+
			"GRANT SELECT ON v TO PUBLIC;\n"+
			"GRANT SELECT ON keep TO PUBLIC;\n"+
			"CREATE FUNCTION tf() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;\n"+
			"CREATE TRIGGER tg INSTEAD OF INSERT ON v FOR EACH ROW EXECUTE FUNCTION tf();\n"+
			"CREATE TRIGGER tt BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION tf();\n"+
			"DROP VIEW v;\nDROP MATERIALIZED VIEW m;"), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(viewNamesOf(database), qt.DeepEquals, []string{"keep"})
	c.Assert(materializedViewNamesOf(database), qt.DeepEquals, make([]string, 0))
	c.Assert(indexNamesOf(database), qt.DeepEquals, []string{"t_id"})
	c.Assert(database.Grants, qt.HasLen, 1)
	c.Assert(database.Grants[0].OnTable, qt.Equals, "keep")
	c.Assert(database.Triggers, qt.HasLen, 1)
	c.Assert(database.Triggers[0].Name, qt.Equals, "tt")
}

// A later file of a schema directory drops a view an earlier file declared.
func TestReadOnto_DropView_ReachesAnEarlierFile(t *testing.T) {
	c := qt.New(t)
	var base schemamodel.Database
	document := sqlschema.NewDocument(&base)
	first, _, err := sqlschema.ReadOnto([]byte(viewFixture), "postgres", document)
	c.Assert(err, qt.IsNil)
	base = first

	second, _, err := sqlschema.ReadOnto([]byte("DROP VIEW v;"), "postgres", document)

	c.Assert(err, qt.IsNil)
	c.Assert(viewNamesOf(second), qt.DeepEquals, make([]string, 0))
	c.Assert(viewNamesOf(base), qt.DeepEquals, []string{"w"})
}

// What the servers refuse is refused, and a refused read leaves nothing half-built.
func TestRead_DropView_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{name: "a view nothing declares", dialect: "postgres", sql: viewFixture + "DROP VIEW nope;", wantErr: `line 4, column 1: DROP VIEW nope names a view this schema does not declare`},
		{name: "a view nothing declares, SQLite", dialect: "sqlite", sql: "DROP VIEW nope;", wantErr: `line 1, column 1: DROP VIEW nope names a view this schema does not declare`},
		{name: "a materialized view nothing declares", dialect: "postgres", sql: "DROP MATERIALIZED VIEW nope;", wantErr: `line 1, column 1: DROP MATERIALIZED VIEW nope names a view this schema does not declare`},
		{name: "CASCADE", dialect: "postgres", sql: viewFixture + "DROP VIEW v CASCADE;", wantErr: `line 4, column 1: DROP VIEW v CASCADE also drops the objects.*`},
		{name: "a table, PostgreSQL", dialect: "postgres", sql: viewFixture + "DROP VIEW t;", wantErr: `line 4, column 1: DROP VIEW t: the schema declares t as another kind of object`},
		{name: "a table under IF EXISTS, PostgreSQL", dialect: "postgres", sql: viewFixture + "DROP VIEW IF EXISTS t;", wantErr: `line 4, column 1: DROP VIEW t: the schema declares t as another kind of object`},
		{name: "a table under IF EXISTS, SQLite", dialect: "sqlite", sql: viewFixture + "DROP VIEW IF EXISTS t;", wantErr: `line 4, column 1: DROP VIEW t: the schema declares t as another kind of object`},
		{name: "a table under IF EXISTS, SQL Server", dialect: "sqlserver", sql: viewFixture + "DROP VIEW IF EXISTS t;", wantErr: `line 4, column 1: DROP VIEW t: the schema declares t as another kind of object`},
		{name: "a view by DROP TABLE, PostgreSQL", dialect: "postgres", sql: viewFixture + "DROP TABLE v;", wantErr: `line 4, column 1: DROP TABLE v names a table this schema does not declare`},
		{name: "a table with no IF EXISTS, MySQL", dialect: "mysql", sql: viewFixture + "DROP VIEW t;", wantErr: `line 4, column 1: DROP VIEW t: the schema declares t as another kind of object`},
		{name: "a materialized view by DROP VIEW", dialect: "postgres", sql: matViewFixture + "DROP VIEW m;", wantErr: `line 4, column 1: DROP VIEW m: the schema declares m as another kind of object`},
		{name: "a view by DROP MATERIALIZED VIEW", dialect: "postgres", sql: viewFixture + "DROP MATERIALIZED VIEW v;", wantErr: `line 4, column 1: DROP MATERIALIZED VIEW v: the schema declares v as another kind of object`},
		{
			name: "a view another view reads, PostgreSQL", dialect: "postgres",
			sql:     viewFixture + "CREATE VIEW x AS SELECT id FROM v;\nDROP VIEW v;",
			wantErr: `line 5, column 1: DROP VIEW v: view x still reads the view`,
		},
		{
			name: "a view a materialized view reads, CockroachDB", dialect: "cockroachdb",
			sql:     viewFixture + "CREATE MATERIALIZED VIEW x AS SELECT id FROM v;\nDROP VIEW v;",
			wantErr: `line 5, column 1: DROP VIEW v: materialized view x still reads the view`,
		},
		{
			// No provider states whether a view blocks the drop of what it reads,
			// so the read refuses what the Spanner emulator refuses: `Cannot drop
			// VIEW v on which there are dependent views: x`.
			name: "a view another view reads, an engine a provider serves", dialect: providerEngine,
			sql:     viewFixture + "CREATE VIEW x AS SELECT id FROM v;\nDROP VIEW v;",
			wantErr: `line 5, column 1: DROP VIEW v: view x still reads the view`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(database.Views, qt.HasLen, 0)
		})
	}
}

// A DROP the model has no answer for is refused, as a statement the parser could not
// read was: stepping over it would keep an object the script removes, and a comparison
// would plan it back.
func TestRead_ADropOfAnotherKindIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		kind    string
	}{
		{name: "a trigger", dialect: "postgres", sql: "DROP TRIGGER tg ON t;", kind: "DROP TRIGGER tg"},
		{name: "a trigger with IF EXISTS", dialect: "sqlite", sql: "DROP TRIGGER IF EXISTS tg;", kind: "DROP TRIGGER tg"},
		{name: "a function", dialect: "postgres", sql: "DROP FUNCTION f();", kind: "DROP FUNCTION f"},
		{name: "a function with IF EXISTS", dialect: "mysql", sql: "DROP FUNCTION IF EXISTS f;", kind: "DROP FUNCTION f"},
		{name: "a procedure", dialect: "mysql", sql: "DROP PROCEDURE p;", kind: "DROP PROCEDURE p"},
		{name: "T-SQL's short spelling", dialect: "sqlserver", sql: "DROP PROC p;", kind: "DROP PROCEDURE p"},
		{name: "a sequence", dialect: "postgres", sql: "DROP SEQUENCE s;", kind: "DROP SEQUENCE s"},
		{name: "a type", dialect: "postgres", sql: "DROP TYPE e;", kind: "DROP TYPE e"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte("CREATE TABLE keep (id int PRIMARY KEY);\n"+test.sql), test.dialect)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err.Error(), qt.Contains, test.kind+":")
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// A statement that changes no schema object changes nothing the read returns.
func TestRead_AStatementThatChangesNoSchemaObjectChangesNothing(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{name: "TRUNCATE", dialect: "postgres", sql: "TRUNCATE t;"},
		{name: "TRUNCATE on SQL Server", dialect: "sqlserver", sql: "TRUNCATE TABLE t;"},
		{name: "a savepoint", dialect: "sqlite", sql: "SAVEPOINT s; RELEASE s;"},
		{name: "a lock", dialect: "postgres", sql: "LOCK TABLE t IN ACCESS SHARE MODE;"},
		{name: "a lock on MySQL", dialect: "mysql", sql: "LOCK TABLES t WRITE; UNLOCK TABLES;"},
		{name: "REPLACE", dialect: "mysql", sql: "REPLACE INTO t (id) VALUES (1);"},
		{name: "UPSERT", dialect: "cockroachdb", sql: "UPSERT INTO t (id) VALUES (1);"},
		{name: "OPTIMIZE", dialect: "clickhouse", sql: "OPTIMIZE TABLE t FINAL;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declare := "CREATE TABLE t (id int PRIMARY KEY);\n"

			want, _, err := sqlschema.Read([]byte(declare), test.dialect)
			c.Assert(err, qt.IsNil)
			got, _, err := sqlschema.Read([]byte(declare+test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(tableNamesOf(got), qt.DeepEquals, tableNamesOf(want))
			c.Assert(got.Fields, qt.HasLen, len(want.Fields))
		})
	}
}

// RENAME TABLE is the other spelling of ALTER TABLE ... RENAME TO, and a schema read
// answers both alike: every other declaration names the table by its old name, so
// the read refuses rather than leave them pointing at nothing.
func TestRead_RenameTableIsRefusedAsItsOtherSpellingIs(t *testing.T) {
	const declare = "CREATE TABLE a (id int PRIMARY KEY);\n"
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{name: "MySQL", dialect: "mysql", sql: declare + "RENAME TABLE a TO b;"},
		{name: "MariaDB", dialect: "mariadb", sql: declare + "RENAME TABLE a TO b;"},
		{name: "several pairs", dialect: "mysql", sql: declare + "RENAME TABLE a TO b, b TO c;"},
		{name: "the other spelling", dialect: "mysql", sql: declare + "ALTER TABLE a RENAME TO b;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err.Error(), qt.Contains, "ALTER TABLE a RENAME TO b")
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// A name too long for the server is cut at the server, so a drop written with it
// reaches the view its creation left under the cut name.
func TestRead_DropView_ReachesACutName(t *testing.T) {
	c := qt.New(t)
	long, longer := strings.Repeat("v", 70), strings.Repeat("v", 71)

	database, _, err := sqlschema.Read([]byte("CREATE VIEW "+long+" AS SELECT 1 AS id;\nDROP VIEW "+longer+";"), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Views, qt.HasLen, 0)
}

// parseOne is the only statement of sql, in dialect.
func parseOne(c *qt.C, sql, dialect string) ast.Node {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect), parser.WithSQL(sqlext.None())).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	return statements.Statements[0]
}

// TestNameFollower_ADropForgetsTheName holds the other consumer of a drop's node: a
// name a drop frees collides with nothing.
func TestNameFollower_ADropForgetsTheName(t *testing.T) {
	c := qt.New(t)
	follower, follows := sqlschema.NewNameFollower("postgres")
	c.Assert(follows, qt.IsTrue)

	c.Assert(follower.Follow(parseOne(c, "CREATE VIEW v AS SELECT 1 AS id;", "postgres")).Collisions, qt.HasLen, 0)
	c.Assert(follower.Follow(parseOne(c, "DROP VIEW v;", "postgres")).Declares, qt.IsFalse)
	c.Assert(follower.Follow(parseOne(c, "CREATE TABLE v (id int);", "postgres")).Collisions, qt.HasLen, 0)
}

// statementOwners is a set whose one owner states the statement facts given
// for the target given.
func statementOwners(c *qt.C, target string, facts sqlext.StatementFacts) sqlext.Set {
	c.Helper()
	set, err := sqlext.NewSet(sqlext.Extension{Owner: "example.org/fixture", Targets: []string{target}, Statements: facts})
	c.Assert(err, qt.IsNil)
	return set
}

// An engine a provider serves has no descriptor, so its provider states what
// its server does with DROP VIEW IF EXISTS of a table's name. Where it states
// that the server does nothing, the read leaves the table and every view as
// they are, as the Spanner emulator does.
func TestReadWith_DropViewIfExists_FollowsTheOwnersFacts_HappyPath(t *testing.T) {
	c := qt.New(t)
	owners := statementOwners(c, providerEngine, sqlext.StatementFacts{DropViewIfExistsSkipsOtherKinds: true})

	database, _, err := sqlschema.ReadWith(c.Context(), []byte(viewFixture+"DROP VIEW IF EXISTS t;"), providerEngine, nil, sqlschema.Options{Owners: owners})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Name, qt.Equals, "t")
	c.Assert(viewNamesOf(database), qt.DeepEquals, []string{"v", "w"})
}

// The read refuses DROP VIEW IF EXISTS of a table's name where no owner of
// the document's engine states that its server skips it, the statement has
// no IF EXISTS, or a descriptor describes the dialect and says its server
// refuses it.
func TestReadWith_DropViewIfExists_FollowsTheOwnersFacts_FailurePath(t *testing.T) {
	skips := sqlext.StatementFacts{DropViewIfExistsSkipsOtherKinds: true}
	for _, test := range []struct {
		name, dialect, owner, drop string
		facts                      sqlext.StatementFacts
	}{
		{"an owner that states nothing", providerEngine, providerEngine, "DROP VIEW IF EXISTS t;", sqlext.StatementFacts{}},
		{"an owner of another engine", providerEngine, "other", "DROP VIEW IF EXISTS t;", skips},
		{"no IF EXISTS", providerEngine, providerEngine, "DROP VIEW t;", skips},
		{"a dialect a descriptor describes", "postgres", "postgres", "DROP VIEW IF EXISTS t;", skips},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owners := statementOwners(c, test.owner, test.facts)

			database, _, err := sqlschema.ReadWith(c.Context(), []byte(viewFixture+test.drop), test.dialect, nil, sqlschema.Options{Owners: owners})

			c.Assert(err, qt.ErrorMatches, `line 4, column 1: DROP VIEW t: the schema declares t as another kind of object`)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}
