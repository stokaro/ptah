//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// clickHouseViewStorageFixture is a scratch database holding three
// materialized views over one source: plain, stored the way Ptah creates every
// view; keyed, stored with ORDER BY c; and routed, which sends its rows to
// another table with TO. It returns the database URL and its name.
func clickHouseViewStorageFixture(c *qt.C) (url, database string) {
	c.Helper()
	url = createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	conn := connectDevDialect(c, url)
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT currentDatabase()").Scan(&database), qt.IsNil)
	for _, statement := range []string{
		"CREATE TABLE src (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE dst (c UInt64) ENGINE = MergeTree ORDER BY c",
		"CREATE MATERIALIZED VIEW plain ENGINE = MergeTree ORDER BY tuple() AS SELECT c FROM " + database + ".src",
		"CREATE MATERIALIZED VIEW keyed ENGINE = MergeTree ORDER BY c AS SELECT c FROM " + database + ".src",
		"CREATE MATERIALIZED VIEW routed TO " + database + ".dst AS SELECT c FROM " + database + ".src",
	} {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return url, database
}

// A view stored any way but the one Ptah creates is not described as stored
// that way: `schema inspect --format sql` wrote `ENGINE = MergeTree ORDER BY
// tuple()` for a view sorted by c, and `schema drift` against that file
// reported no drift, so the view's real storage was lost without a word. The
// description now names the stored clause, and drift reports each such view
// undecided.
func TestClickHouseUndescribedViewStorageIsNotReadAsPtahStorageE2E(t *testing.T) {
	c := qt.New(t)
	url, database := clickHouseViewStorageFixture(c)

	inspected := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "inspect", "--db-url", url, "--format", "sql")

	c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
	c.Assert(inspected.Stdout, qt.Contains, "-- materialized view "+database+".keyed: ptah.run/clickhouse/view-storage was not captured "+
		"(the view stores its rows with ENGINE = MergeTree ORDER BY c SETTINGS index_granularity = 8192, which Ptah does not model)")
	c.Assert(inspected.Stdout, qt.Contains, "-- materialized view "+database+".routed: ptah.run/clickhouse/view-storage was not captured "+
		"(the view sends its rows to another table with TO "+database+".dst, which Ptah does not model)")
	c.Assert(inspected.Stdout, qt.Not(qt.Contains), database+".plain: ptah.run/clickhouse/view-storage")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(inspected.Stdout), 0o600), qt.IsNil)

	drift := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "drift", "--db-url", url, "--schema-file", schema)

	c.Assert(drift.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", drift.Stdout, drift.Stderr))
	c.Assert(drift.Stdout, qt.Contains, `- ptah.run/clickhouse/view-storage "`+database+`.keyed"`)
	c.Assert(drift.Stdout, qt.Contains, `- ptah.run/clickhouse/view-storage "`+database+`.routed"`)
	c.Assert(drift.Stdout, qt.Not(qt.Contains), `"`+database+`.plain"`)
}

// inspectedWithChangedQuery inspects url as SQL and returns the description
// with the query of one view changed, written to a file.
func inspectedWithChangedQuery(c *qt.C, url, database, view string) string {
	c.Helper()
	inspected := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "inspect", "--db-url", url, "--format", "sql")
	c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
	head := "`" + database + "`.`" + view + "` ENGINE = MergeTree ORDER BY tuple() AS\n"
	changed := strings.Replace(inspected.Stdout, head+"SELECT c FROM "+database+".src", head+"SELECT c + 1 AS c FROM "+database+".src", 1)
	c.Assert(changed, qt.Not(qt.Equals), inspected.Stdout)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(changed), 0o600), qt.IsNil)
	return schema
}

// A changed query replaces a materialized view, and a replacement creates it
// with Ptah's storage. For a view whose storage the read could not describe,
// that would change its storage silently, so the plan is refused.
func TestClickHouseUndescribedViewStorageRefusesAReplacementE2E(t *testing.T) {
	for _, view := range []string{"keyed", "routed"} {
		t.Run(view, func(t *testing.T) {
			c := qt.New(t)
			url, database := clickHouseViewStorageFixture(c)
			schema := inspectedWithChangedQuery(c, url, database, view)

			apply := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "apply", "--db-url", url, "--schema-file", schema, "--dry-run")

			c.Assert(apply.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", apply.Stdout, apply.Stderr))
			c.Assert(apply.Stderr, qt.Contains, "materialized view "+database+"."+view+
				" is replaced, and its \"ptah.run/clickhouse/view-storage\" could not be read")
		})
	}
}

// The control: the same change to the view stored the way Ptah creates one
// still plans its replacement, and leaves the other views alone.
func TestClickHouseDescribedViewStoragePlansAReplacementE2E(t *testing.T) {
	c := qt.New(t)
	url, database := clickHouseViewStorageFixture(c)
	schema := inspectedWithChangedQuery(c, url, database, "plain")

	apply := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "apply", "--db-url", url, "--schema-file", schema, "--dry-run")

	c.Assert(apply.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", apply.Stdout, apply.Stderr))
	c.Assert(apply.Stdout, qt.Contains, "DROP VIEW IF EXISTS `"+database+"`.`plain`;")
	c.Assert(apply.Stdout, qt.Not(qt.Contains), "DROP VIEW IF EXISTS `"+database+"`.`keyed`")
}
