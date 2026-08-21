package schema_test

import (
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `schema stats` reads a live database and answers in the OpenMetrics text
// format, so what these tests hold is the shape a scrape depends on, not just
// the numbers (stokaro/ptah#1711).

// TestSchemaStatsCountsALiveDatabase is the whole verb end to end.
func TestSchemaStatsCountsALiveDatabase(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "app.db")
	seedSQLite(c, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);"+
			"CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER);"+
			"CREATE INDEX idx_orders_user ON orders (user_id);"+
			"CREATE VIEW recent AS SELECT id FROM orders;")

	out, err := runSchema("", "stats", "--db-url", "sqlite://"+dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, `ptah_schema_tables{dialect="sqlite"} 2`)
	c.Assert(out, qt.Contains, `ptah_schema_columns{dialect="sqlite"} 5`)
	c.Assert(out, qt.Contains, `ptah_schema_indexes{dialect="sqlite"} 1`)
	c.Assert(out, qt.Contains, `ptah_schema_views{dialect="sqlite"} 1`)
	c.Assert(strings.HasSuffix(out, "# EOF\n"), qt.IsTrue)
}

// TestSchemaStatsLabelsCarryTheDialect keeps the label that makes two scrapes
// of different databases distinguishable in one collector.
func TestSchemaStatsLabelsCarryTheDialect(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "app.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runSchema("", "stats", "--db-url", "sqlite://"+dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, `dialect="sqlite"`)
	// No --schemas was passed, so no schemas label is invented for it.
	c.Assert(out, qt.Not(qt.Contains), "schemas=")
}

// TestSchemaStatsAcceptsSQLite states a deliberate divergence.
//
// The Atlas surface this mirrors refuses SQLite for this verb. Ptah does not:
// its reader handles SQLite like any other dialect, and refusing would copy a
// limitation this implementation does not have. If that ever changes, this test
// is the thing that has to be argued with first.
func TestSchemaStatsAcceptsSQLite(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "app.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runSchema("", "stats", "--db-url", "sqlite://"+dbPath)

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Not(qt.Contains), "not supported")
	c.Assert(out, qt.Contains, `ptah_schema_tables{dialect="sqlite"} 1`)
}

// TestSchemaStatsRequiresADatabase keeps the flag required rather than
// answering a scrape of nothing with a page of confident zeroes.
//
// The assertion is on the exact message, not on the flag name appearing
// somewhere in it. Without the guard the verb still fails -- it reaches the
// connector and reports `connect to --db-url: invalid database URL: missing
// scheme` -- which also contains "db-url" and would satisfy a looser check
// while telling the operator about URL parsing instead of about the flag they
// forgot.
func TestSchemaStatsRequiresADatabase(t *testing.T) {
	c := qt.New(t)

	_, err := runSchema("", "stats")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "--db-url is required")
}
