//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// renderOmissionDatabases creates an empty scratch database on one engine of
// each built-in renderer family and returns its URL.
var renderOmissionDatabases = map[string]func(c *qt.C) string{
	"postgres": func(c *qt.C) string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
	},
	"mariadb": func(c *qt.C) string {
		_, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "omission")
		return url
	},
	"sqlserver": func(c *qt.C) string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
			"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
	},
	"oracle": func(c *qt.C) string {
		_, url, _ := oracleCaseSensitiveAccount(c, c.Context())
		return url
	},
	"clickhouse": func(c *qt.C) string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	},
	"sqlite": func(c *qt.C) string {
		return "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), "omission.db"))
	},
}

// TestPlanRenderOmissionsE2E applies, on one engine of each built-in renderer
// family, a schema that declares something the engine has no clause for, then
// applies it again. The first apply warns on stderr about what its CREATE
// statement leaves out, in the words `schema render` uses, applies the rest,
// and the object reads back without it. The second apply plans nothing and so
// warns about nothing, though the declaration still holds what the engine
// cannot carry.
func TestPlanRenderOmissionsE2E(t *testing.T) {
	tests := []struct {
		engine   string
		schema   string
		warning  string
		readBack string
		want     string
	}{
		{
			engine:   "postgres",
			schema:   "CREATE TABLE ro_t (id INT PRIMARY KEY, name VARCHAR(20)) ENGINE=InnoDB;\n",
			warning:  "warning: postgres: table public.ro_t: table option ENGINE=InnoDB was skipped (unsupported)\n",
			readBack: "SELECT table_name FROM information_schema.tables WHERE table_name = 'ro_t'",
			want:     "ro_t",
		},
		{
			engine: "mariadb",
			schema: "CREATE TABLE ro_t (id INT PRIMARY KEY, name VARCHAR(20));\n" +
				"CREATE INDEX ro_t_name ON ro_t (name) WITH (fillfactor = 70);\n",
			warning:  "warning: mariadb: index ro_t_name: index storage parameter fillfactor=70 was skipped (unsupported)\n",
			readBack: "SELECT index_type FROM information_schema.statistics WHERE table_schema = DATABASE() AND index_name = 'ro_t_name'",
			want:     "BTREE",
		},
		{
			engine:   "sqlserver",
			schema:   "CREATE TABLE ro_t (id INT PRIMARY KEY, name VARCHAR(20)) ENGINE=InnoDB;\n",
			warning:  "warning: sqlserver: table ro_t: table option ENGINE=InnoDB was skipped (unsupported)\n",
			readBack: "SELECT name FROM sys.tables WHERE name = 'ro_t'",
			want:     "ro_t",
		},
		{
			engine:   "oracle",
			schema:   "CREATE TABLE ro_t (id NUMBER(10) PRIMARY KEY, name VARCHAR2(20)) ENGINE=InnoDB;\n",
			warning:  "warning: oracle: table ro_t: table option ENGINE=InnoDB was skipped (unsupported)\n",
			readBack: "SELECT table_name FROM user_tables WHERE table_name = 'RO_T'",
			want:     "RO_T",
		},
		{
			engine: "clickhouse",
			schema: "CREATE TABLE ro_t (id Int32, name String) ENGINE = MergeTree ORDER BY id;\n" +
				"CREATE UNIQUE INDEX ro_t_name ON ro_t (name);\n",
			warning:  "warning: clickhouse: index ro_t_name: unique index was skipped (unsupported)\n",
			readBack: "SELECT type FROM system.data_skipping_indices WHERE database = currentDatabase() AND name = 'ro_t_name'",
			want:     "minmax",
		},
		{
			engine: "sqlite",
			schema: "CREATE TABLE ro_t (id INT PRIMARY KEY, name VARCHAR(20)) ENGINE=InnoDB;\n" +
				"CREATE INDEX ro_t_name ON ro_t USING HASH (name);\n",
			warning: "warning: sqlite: index ro_t_name: index type=HASH was skipped (unsupported)\n" +
				"warning: sqlite: table ro_t: table option ENGINE=InnoDB was skipped (unsupported)\n",
			readBack: "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'ro_t_name'",
			want:     "ro_t_name",
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(qt.New(t), ctx, e2eRepoRoot(t), binary)
	for _, test := range tests {
		t.Run(test.engine, func(t *testing.T) {
			c := qt.New(t)
			dbURL := renderOmissionDatabases[test.engine](c)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.schema), 0o600), qt.IsNil)
			apply := func() (string, string, error) {
				return runCLIProcess(ctx, c.TempDir(), binary, "schema", "apply", "--db-url", dbURL, "--schema-file", schema, "--auto-approve")
			}

			stdout, stderr, err := apply()
			again, againErr, err2 := apply()

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stderr, qt.Contains, test.warning)
			c.Assert(stdout, qt.Not(qt.Contains), "was skipped (")
			c.Assert(readOmissionBack(c, ctx, dbURL, test.readBack), qt.Equals, test.want)
			c.Assert(err2, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, againErr))
			c.Assert(again, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(againErr, qt.Not(qt.Contains), "was skipped (")
		})
	}
}

// readOmissionBack runs query on the database dbURL names and returns the one
// value it reads.
func readOmissionBack(c *qt.C, ctx context.Context, dbURL, query string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var value string
	c.Assert(conn.QueryRowContext(ctx, query).Scan(&value), qt.IsNil)
	return strings.TrimSpace(value)
}
