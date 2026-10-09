package atlasschema

// White-box testing required: baseline statement validation is private to the
// rehearsal, whose public entry point requires a live database connection.

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

const ydbTable = "CREATE TABLE items (id Int64 NOT NULL, PRIMARY KEY (id));"

// TestRehearsalBaselineRefusesWhatEscapesTheDevRealm pins the refusals a
// baseline shares with a migration replay: what outlives the realm's cleanup,
// what the whole YDB database shares, and what reaches past the server. Each
// refusal names the statement and says it is the baseline's.
func TestRehearsalBaselineRefusesWhatEscapesTheDevRealm(t *testing.T) {
	ydb := catalog.ServerInfo{Dialect: "ydb"}
	postgres := catalog.ServerInfo{Dialect: "postgres"}
	mysql := catalog.ServerInfo{Dialect: "mysql", Schema: "ptah_dev"}
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		first     string
		statement string
		wantErr   string
	}{
		{"a YDB pool's settings", ydb, ydbTable, "ALTER RESOURCE POOL default SET (resource_weight = 50);",
			`(?s)baseline statement 2 \(ALTER RESOURCE POOL default .*\) cannot be rehearsed: ydb rehearsal baseline refuses a resource pool of the whole database .*`},
		{"a YDB pool", ydb, ydbTable, "CREATE RESOURCE POOL batch WITH (concurrent_query_limit = 10);",
			`(?s).*ydb rehearsal baseline refuses a resource pool of the whole database.*`},
		{"a YDB classifier", ydb, ydbTable, "CREATE RESOURCE POOL CLASSIFIER etl WITH (resource_pool = 'batch', rank = 1);",
			`(?s).*ydb rehearsal baseline refuses a resource pool of the whole database.*`},
		{"a YDB user", ydb, ydbTable, "CREATE USER `reader`;",
			`(?s)baseline statement 2 \(CREATE USER .reader.;\) cannot be rehearsed: ydb rehearsal baseline refuses a user of the whole database .*PTAH_DEV_SERVER_DISPOSABLE=1.*`},
		{"a YDB grant", ydb, ydbTable, "GRANT 'ydb.generic.read' ON `items` TO `reader`;",
			`(?s).*ydb rehearsal baseline refuses GRANT permission change.*`},
		{"a YDB replication", ydb, ydbTable,
			"CREATE ASYNC REPLICATION `r` FOR `items` AS `copy` WITH (CONNECTION_STRING = 'grpcs://prod.example:2135/?database=/prod');",
			`(?s).*ydb rehearsal baseline refuses async replication.*`},
		{"a YDB transfer", ydb, ydbTable, "CREATE TRANSFER `t` FROM `topic` TO `items` USING $l;",
			`(?s).*ydb rehearsal baseline refuses a transfer.*`},
		{"a YDB external data source", ydb, ydbTable,
			"CREATE EXTERNAL DATA SOURCE `s` WITH (SOURCE_TYPE = 'ObjectStorage', LOCATION = 'https://bucket.example');",
			`(?s).*ydb rehearsal baseline refuses an external data source.*`},
		{"a YDB streaming query", ydb, ydbTable, "CREATE STREAMING QUERY `q` AS DO BEGIN SELECT 1; END DO;",
			`(?s).*ydb rehearsal baseline refuses a streaming query.*`},
		{"a PostgreSQL role", postgres, "CREATE TABLE items (id int PRIMARY KEY);", `CREATE ROLE "reader" WITH NOLOGIN`,
			`(?s).*postgres rehearsal baseline refuses CREATE ROLE.*`},
		{"a comment on the database", postgres, "CREATE TABLE items (id int PRIMARY KEY);", `COMMENT ON DATABASE ptah_dev IS 'x'`,
			`(?s).*postgres rehearsal baseline refuses COMMENT ON global metadata.*`},
		{"a routine in an untrusted language", postgres, "CREATE TABLE items (id int PRIMARY KEY);",
			"CREATE FUNCTION f() RETURNS int AS $$ return 1 $$ LANGUAGE plpython3u",
			`(?s).*postgres rehearsal baseline refuses CREATE routine in untrusted language plpython3u.*`},
		{"a MySQL user", mysql, "CREATE TABLE items (id int PRIMARY KEY);", "CREATE USER 'reader'@'%'",
			`(?s).*mysql rehearsal baseline refuses CREATE USER.*`},
		{"a MySQL grant", mysql, "CREATE TABLE items (id int PRIMARY KEY);", "GRANT SELECT ON ptah_dev.items TO 'reader'@'%'",
			`(?s).*mysql rehearsal baseline refuses privilege or role mutation.*`},
		{"a MySQL event", mysql, "CREATE TABLE items (id int PRIMARY KEY);",
			"CREATE EVENT purge ON SCHEDULE EVERY 1 DAY DO DELETE FROM items",
			`(?s).*mysql rehearsal baseline refuses CREATE executable stored body.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := guardRehearsalBaseline([]string{test.first, test.statement}, test.info)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestRehearsalBaselineAcceptsWhatStaysInTheDevDatabase is the control on the
// refusals: each baseline is what the planner writes for a schema with
// objects a migration file is refused -- routines, a trigger, comments on a
// schema and an extension, a MySQL trigger and routine -- and the baseline
// guard accepts every statement, because each stays in the dev database.
func TestRehearsalBaselineAcceptsWhatStaysInTheDevDatabase(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		info catalog.ServerInfo
	}{
		{
			name: "PostgreSQL routines, a trigger and an extension comment",
			sql: "CREATE EXTENSION IF NOT EXISTS pg_trgm;\n" +
				"COMMENT ON EXTENSION pg_trgm IS 'trigram matching';\n" +

				"CREATE TABLE items (id int PRIMARY KEY, n int);\n" +
				"COMMENT ON TABLE items IS 'stock';\n" +
				"CREATE FUNCTION item_count() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM items $$;\n" +
				"CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.n := 1; RETURN NEW; END $$;\n" +
				"CREATE TRIGGER items_touch BEFORE INSERT ON items FOR EACH ROW EXECUTE FUNCTION touch();\n",
			info: catalog.ServerInfo{Dialect: "postgres", Capabilities: capability.Postgres18()},
		},
		{
			name: "a MySQL trigger and routine",
			sql: "CREATE TABLE items (id int PRIMARY KEY, n int);\n" +
				"CREATE TRIGGER items_touch BEFORE INSERT ON items FOR EACH ROW SET NEW.n = 1;\n" +
				"CREATE FUNCTION item_count() RETURNS int DETERMINISTIC READS SQL DATA RETURN (SELECT COUNT(*) FROM items);\n",
			info: catalog.ServerInfo{Dialect: "mysql", Schema: "ptah_dev", Capabilities: capability.MySQL84()},
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
			replay := devclean.NewDevReplayGuard(test.info)
			c.Assert(slices.ContainsFunc(statements, func(statement string) bool {
				return replay.ValidateStatement(statement) != nil
			}), qt.IsTrue, qt.Commentf("statements: %q", statements))

			c.Assert(guardRehearsalBaseline(statements, test.info), qt.IsNil)
		})
	}
}

// TestRehearsalBaselineAcceptsStatementsThatStayInTheDevDatabase covers what
// the planned baselines above do not: a comment on a schema of the dev
// database, and a SQL Server routine and trigger.
func TestRehearsalBaselineAcceptsStatementsThatStayInTheDevDatabase(t *testing.T) {
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		statement string
	}{
		{"a comment on a schema", catalog.ServerInfo{Dialect: "postgres"}, `COMMENT ON SCHEMA "app" IS 'application'`},
		{"a CockroachDB routine", catalog.ServerInfo{Dialect: "cockroachdb"}, "CREATE FUNCTION f() RETURNS INT8 LANGUAGE SQL AS $$ SELECT 1 $$"},
		{"a SQL Server procedure", catalog.ServerInfo{Dialect: "sqlserver"}, "CREATE PROCEDURE [dbo].[touch] AS SELECT 1"},
		{"a SQL Server table trigger", catalog.ServerInfo{Dialect: "sqlserver"}, "CREATE TRIGGER [dbo].[items_touch] ON [dbo].[items] AFTER INSERT AS SELECT 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guardRehearsalBaseline([]string{test.statement}, test.info), qt.IsNil)
		})
	}
}

func TestRehearsalBaselineAcceptsYDBDirectoryObjects(t *testing.T) {
	c := qt.New(t)
	err := guardRehearsalBaseline([]string{
		ydbTable,
		"CREATE TOPIC `events`;",
		"CREATE COORDINATION NODE `locks`;",
	}, catalog.ServerInfo{Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
}

// declaredDisposable records url as a server the operator declared
// disposable, which the run may change as a whole, for the test's lifetime.
func declaredDisposable(c *qt.C, url string) {
	c.Helper()
	_, release, err := devdocker.Resolve(c.Context(), url, devdocker.Options{DeclaredDisposable: true})
	c.Assert(err, qt.IsNil)
	c.Cleanup(release)
}

// TestRehearsalBaselineWritesWhatAnOwnedServerHolds is the acceptance control
// on the realm refusals: on a server the run owns, a user, a grant and the
// database-wide workload are the run's own, and the baseline writes them.
func TestRehearsalBaselineWritesWhatAnOwnedServerHolds(t *testing.T) {
	c := qt.New(t)
	const devURL = "ydb://localhost:2136/local"
	declaredDisposable(c, devURL)
	err := guardRehearsalBaseline([]string{
		ydbTable,
		"CREATE USER `reader`;",
		"GRANT 'ydb.generic.read' ON `items` TO `reader`;",
		"CREATE RESOURCE POOL batch WITH (concurrent_query_limit = 10);",
	}, catalog.ServerInfo{Dialect: "ydb", URL: devURL})
	c.Assert(err, qt.IsNil)
}

// TestRehearsalBaselineRefusesWhatReachesPastAnOwnedServer pins that owning
// the server lifts nothing that reaches past it: a replication pulls from
// whatever its connection string names, wherever the dev server runs.
func TestRehearsalBaselineRefusesWhatReachesPastAnOwnedServer(t *testing.T) {
	c := qt.New(t)
	const devURL = "ydb://localhost:2136/local"
	declaredDisposable(c, devURL)
	err := guardRehearsalBaseline([]string{
		ydbTable,
		"CREATE ASYNC REPLICATION `r` FOR `items` AS `copy` WITH (CONNECTION_STRING = 'grpcs://prod.example:2135/?database=/prod');",
	}, catalog.ServerInfo{Dialect: "ydb", URL: devURL})
	c.Assert(err, qt.ErrorMatches, `(?s).*ydb rehearsal baseline refuses async replication.*`)
}

// realmInfo is a YDB dev connection to the realm abc of the database /local,
// whose root is /local/ptah_dev/abc.
var realmInfo = catalog.ServerInfo{Dialect: "ydb", URL: "ydb://localhost:2136/local?dev_realm=abc"}

// TestRehearsalBaselineGrantsOnTheRealmItself pins the one grant a baseline
// writes in a YDB dev realm: on the realm's root or a path under it, named by
// its absolute path, which the realm's removal takes with it. The realm's root
// stands in for the target's database, so the permissions the target holds on
// its root are granted there.
func TestRehearsalBaselineGrantsOnTheRealmItself(t *testing.T) {
	for _, statement := range []string{
		"GRANT 'ydb.access.grant' ON `/local/ptah_dev/abc` TO `ACCESS-ADMINS`;",
		"GRANT 'ydb.generic.read', 'ydb.generic.list' ON `/local/ptah_dev/abc/items`, `/local/ptah_dev/abc/app/orders` TO `reader`;",
		"REVOKE 'ydb.generic.read' ON `/local/ptah_dev/abc/items` FROM `reader`;",
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(guardRehearsalBaseline([]string{ydbTable, statement}, realmInfo), qt.IsNil)
		})
	}
}

// TestRehearsalBaselineRefusesAGrantOutsideTheRealm is the control on the
// test above: a relative path, which YDB resolves against the database root,
// a path beside the realm, and one that climbs out of it are refused, and so
// is a grant on a connection that names no realm.
func TestRehearsalBaselineRefusesAGrantOutsideTheRealm(t *testing.T) {
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		statement string
	}{
		{"a relative path", realmInfo, "GRANT 'ydb.generic.read' ON `items` TO `reader`;"},
		{"the database root", realmInfo, "GRANT 'ydb.generic.read' ON `/local` TO `reader`;"},
		{"a realm whose name extends this one", realmInfo, "GRANT 'ydb.generic.read' ON `/local/ptah_dev/abcd` TO `reader`;"},
		{"a path that climbs out", realmInfo, "GRANT 'ydb.generic.read' ON `/local/ptah_dev/abc/../x` TO `reader`;"},
		{"one path inside and one outside", realmInfo, "GRANT 'ydb.generic.read' ON `/local/ptah_dev/abc/items`, `/local/items` TO `reader`;"},
		{"a connection naming no realm", catalog.ServerInfo{Dialect: "ydb", URL: "ydb://localhost:2136/local"},
			"GRANT 'ydb.generic.read' ON `/local/ptah_dev/abc` TO `reader`;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := guardRehearsalBaseline([]string{ydbTable, test.statement}, test.info)
			c.Assert(err, qt.ErrorMatches, `(?s).*ydb rehearsal baseline refuses GRANT permission change.*`)
		})
	}
}

// TestReplayRefusesEveryGrantInARealm pins that the realm grant is the
// baseline's alone: a migration file that grants on the realm's root is
// refused, as every grant in a realm is.
func TestReplayRefusesEveryGrantInARealm(t *testing.T) {
	c := qt.New(t)
	err := devclean.NewDevReplayGuard(realmInfo).ValidateStatement("GRANT 'ydb.access.grant' ON `/local/ptah_dev/abc` TO `ACCESS-ADMINS`;")
	c.Assert(err, qt.ErrorMatches, `(?s).*ydb migration replay rejects GRANT permission change.*`)
}
