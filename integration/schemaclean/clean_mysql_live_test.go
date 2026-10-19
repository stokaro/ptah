//go:build integration

package schemaclean_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemaclean"
)

// mysqlCleanupFixture creates one object of every kind the MySQL family's
// writer destroys. The routines and the event are what the schema reader does
// not describe, so the plan names them only through the writer's catalog
// strategy, internal/dbschema/mysql's CleanupCatalog.
var mysqlCleanupFixture = []string{
	"CREATE TABLE users (id INT PRIMARY KEY)",
	"CREATE TABLE posts (id INT PRIMARY KEY, author_id INT NOT NULL, " +
		"CONSTRAINT fk_posts_author FOREIGN KEY (author_id) REFERENCES users (id))",
	"CREATE VIEW v_users AS SELECT id FROM users",
	"CREATE PROCEDURE p_touch() BEGIN SELECT 1; END",
	"CREATE FUNCTION f_one() RETURNS INT DETERMINISTIC RETURN 1",
	"CREATE EVENT e_tick ON SCHEDULE EVERY 1 DAY DO SELECT 1",
}

// mysqlCleanupCensusQuery enumerates, straight from information_schema, every
// object of the database the writer issues a statement for, in the vocabulary
// of schemaclean.Object.Type. It is written against the catalog rather than
// against the writer's strategy, so the comparison is a measurement.
const mysqlCleanupCensusQuery = `
	SELECT CONCAT(CASE table_type WHEN 'VIEW' THEN 'view' WHEN 'SEQUENCE' THEN 'sequence' ELSE 'table' END, '|', table_name)
	FROM information_schema.tables WHERE table_schema = DATABASE()
	UNION ALL
	SELECT CONCAT(LOWER(routine_type), '|', routine_name)
	FROM information_schema.routines WHERE routine_schema = DATABASE()
	UNION ALL
	SELECT CONCAT('event|', event_name)
	FROM information_schema.events WHERE event_schema = DATABASE()
	UNION ALL
	SELECT CONCAT('foreign_key|', constraint_name)
	FROM information_schema.table_constraints
	WHERE constraint_schema = DATABASE() AND constraint_type = 'FOREIGN KEY'
`

// TestInspectNamesEveryObjectApplyDestroys_MySQLFamilyLive drives the real
// Inspect and Apply path on MySQL and MariaDB: the plan names the stored
// programs, the event and, on MariaDB, the sequence the writer's catalog
// strategy lists, and every object it names is the one the cleanup destroys.
func TestInspectNamesEveryObjectApplyDestroys_MySQLFamilyLive(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		extra  []string
		want   []string
	}{
		{
			name:   "MySQL",
			engine: dbtarget.MySQLAdmin,
			want: []string{
				"event|e_tick", "foreign_key|fk_posts_author", "function|f_one", "procedure|p_touch",
				"table|posts", "table|users", "view|v_users",
			},
		},
		{
			name:   "MariaDB",
			engine: dbtarget.MariaDBAdmin,
			extra:  []string{"CREATE SEQUENCE s_counter"},
			want: []string{
				"event|e_tick", "foreign_key|fk_posts_author", "function|f_one", "procedure|p_touch",
				"sequence|s_counter", "table|posts", "table|users", "view|v_users",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := c.Context()
			conn := newMySQLCleanupLiveConnection(c, ctx, test.engine)
			for _, statement := range slices.Concat(mysqlCleanupFixture, test.extra) {
				_, err := conn.ExecContext(ctx, statement)
				c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
			}

			before := mysqlCleanupCensus(c, ctx, conn)
			plan, err := schemaclean.Inspect(ctx, conn)
			c.Assert(err, qt.IsNil)
			planned := plannedObjectKeys(plan)
			c.Assert(schemaclean.Apply(ctx, conn), qt.IsNil)
			after := mysqlCleanupCensus(c, ctx, conn)

			c.Assert(planned, qt.DeepEquals, test.want)
			c.Assert(destroyedKeys(before, after), qt.DeepEquals, planned)
			c.Assert(after, qt.HasLen, 0)
		})
	}
}

func mysqlCleanupCensus(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, mysqlCleanupCensusQuery)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()

	var census []string
	for rows.Next() {
		var key string
		c.Assert(rows.Scan(&key), qt.IsNil)
		census = append(census, key)
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(census)
	return census
}

// newMySQLCleanupLiveConnection creates a throwaway database through the
// engine's administrative account and connects to it, so a cleanup that
// destroys every object of its database cannot reach the shared fixture
// database other live tests depend on.
func newMySQLCleanupLiveConnection(c *qt.C, ctx context.Context, engine dbtarget.Engine) *dbschema.DatabaseConnection {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	name := fmt.Sprintf("ptah_schemaclean_%d", time.Now().UnixNano())

	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, withDatabase(c, adminURL, name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		_, dropErr := admin.ExecContext(context.WithoutCancel(ctx), "DROP DATABASE IF EXISTS "+name)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	return conn
}

// withDatabase replaces the database an administrative URL names, the path
// after its address, keeping any query.
func withDatabase(c *qt.C, rawURL, database string) string {
	c.Helper()
	base, query, _ := strings.Cut(rawURL, "?")
	slash := strings.LastIndex(base, "/")
	c.Assert(slash > 0, qt.IsTrue, qt.Commentf("an administrative URL names a database: %s", rawURL))
	return base[:slash+1] + database + strings.TrimSuffix("?"+query, "?")
}
