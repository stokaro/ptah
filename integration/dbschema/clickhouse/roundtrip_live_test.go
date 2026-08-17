//go:build integration

package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// The live half of stokaro/ptah#1603: a MergeTree table Ptah creates has to
// read back as itself, compare to zero difference, and re-render.
//
// Both defects the issue reported were one root cause -- the read dropped the
// key the table sorts by -- so both are asserted here against a real server
// rather than argued from the catalog query.

const (
	roundTripTable      = "ptah_1603_orders"
	roundTripSplitTable = "ptah_1603_split"
)

// TestClickHouseTableRoundTripsToZeroDifference is the idempotence half.
//
// Before the fix the second comparison planned
// `ALTER TABLE orders MODIFY COLUMN id UInt64` on a table nothing had touched,
// so a scheduled apply rewrote the column forever and `--dry-run` could never
// be used as a drift signal.
func TestClickHouseTableRoundTripsToZeroDifference(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	dropRoundTripTables(c, conn)
	defer dropRoundTripTables(c, conn)

	declared := roundTripDeclaration()
	applyStatements(c, conn, planAgainstLive(c, conn, declared))

	// The acceptance criterion: applying the same declaration twice plans
	// nothing the second time.
	c.Assert(planAgainstLive(c, conn, declared), qt.HasLen, 0)
}

// TestClickHouseReadRendersItsOwnRead is the describe half.
//
// Before the fix `ptah db read` exited 2 against any database Ptah could
// create: the description carried no key, and re-rendering it hit the refusal
// that exists for a declaration that never named one.
func TestClickHouseReadRendersItsOwnRead(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	dropRoundTripTables(c, conn)
	defer dropRoundTripTables(c, conn)

	applyStatements(c, conn, planAgainstLive(c, conn, roundTripDeclaration()))

	live := readLive(c, conn)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		schemadiff.CompareWithDialect(&goschema.Database{}, live, platform.ClickHouse),
		&goschema.Database{}, platform.ClickHouse, conn.Info().Capabilities,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.IsNotNil)
}

// TestClickHouseReadCarriesASortingKeyWiderThanThePrimaryKey covers the shape
// the primary key alone cannot describe.
//
// `PRIMARY KEY (a) ORDER BY (a, b)` is a table that sorts by two columns. A
// description built from its primary key sorts by one, and applying it creates
// a different table -- rows in a different order -- rather than failing.
func TestClickHouseReadCarriesASortingKeyWiderThanThePrimaryKey(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	dropRoundTripTables(c, conn)
	defer dropRoundTripTables(c, conn)

	c.Assert(conn.Writer().ExecuteSQL(c.Context(),
		"CREATE TABLE "+sqlident.Quote(platform.ClickHouse, roundTripSplitTable)+
			" (a UInt64, b UInt64, c String) ENGINE = MergeTree PRIMARY KEY (a) ORDER BY (a, b)",
	), qt.IsNil)

	live := readLive(c, conn)
	c.Assert(sortingKeyOf(live, roundTripSplitTable), qt.Equals, "a, b")
}

// roundTripDeclaration is the reproducer from the issue: one MergeTree table
// whose sorting key comes from a declared primary key.
func roundTripDeclaration() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Orders", Name: roundTripTable}},
		Fields: []goschema.Field{
			{StructName: "Orders", Name: "id", Type: "UInt64", Primary: true},
		},
	}
}

func planAgainstLive(c *qt.C, conn *dbschema.DatabaseConnection, declared *goschema.Database) []string {
	c.Helper()
	live := readLive(c, conn)
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, live, info, nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff, declared, info.Dialect, info.Capabilities)
	c.Assert(err, qt.IsNil)
	return statements
}

func applyStatements(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil,
			qt.Commentf("execute: %s", statement))
	}
}

func readLive(c *qt.C, conn *dbschema.DatabaseConnection) *dbschematypes.DBSchema {
	c.Helper()
	live, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	return live
}

func sortingKeyOf(live *dbschematypes.DBSchema, table string) string {
	for _, candidate := range live.Tables {
		if candidate.Name == table {
			return candidate.ClickHouseSortingKey
		}
	}
	return ""
}

func dropRoundTripTables(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	for _, table := range []string{roundTripTable, roundTripSplitTable} {
		_ = conn.Writer().ExecuteSQL(c.Context(),
			"DROP TABLE IF EXISTS "+sqlident.Quote(platform.ClickHouse, table))
	}
}
