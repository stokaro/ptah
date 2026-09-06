//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver for database/sql

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// TestMariaDBSpatialIndexIsNotSatisfiedByBTreeLive is stokaro/ptah#2721 against
// the server that reaches both states.
//
// A comparator test builds its catalog by hand, so it proves the comparison and
// says nothing about whether the reader supplies what the comparison reads.
// Measured: with the reader's `Method` assignment removed, the comparator test
// still passes and only this one fails. The two halves are one defect and only
// a live read joins them.
//
// MariaDB is the engine because MariaDB is where both states exist. `CREATE
// INDEX` over a POINT column leaves `INDEX_TYPE=BTREE` here; MySQL promotes the
// same statement to SPATIAL, so the false convergence cannot be built there.
// The reader and the comparator are shared, and the fix is in both.
//
// Measured on MariaDB 11.8.9 and on 12.3.3, which is what the integration
// workflow starts. A test whose premise holds only on the version its author
// happened to run is one that fails in CI for a reason unrelated to the change,
// so the fixture asserts the catalog's own answer before it asks Ptah anything.
func TestMariaDBSpatialIndexIsNotSatisfiedByBTreeLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, dbtarget.MariaDBAdmin))
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_spatial_%d", time.Now().UnixNano())
	createMySQLDatabase(c, ctx, adminDB, name)
	defer dropMySQLDatabase(c, context.Background(), adminDB, name)
	dbURL := replaceMySQLDatabaseName(c, dbtarget.URL(t, dbtarget.MariaDBAdmin), name)

	// Written by hand rather than by Ptah. A table Ptah created would round
	// trip through whatever the renderer writes, and a reader defect stays
	// invisible against a fixture the renderer wrote.
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`geo` (`id` BIGINT NOT NULL PRIMARY KEY, `location` POINT NOT NULL)",
		name,
	))
	c.Assert(err, qt.IsNil)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE INDEX `sx_geo_location` ON `%s`.`geo` (`location`)",
		name,
	))
	c.Assert(err, qt.IsNil)

	// What the catalog says, before Ptah is asked. Without it the assertions
	// below could agree with a server that answered differently, and the test
	// would pass while measuring nothing. Measured on MariaDB 11.8.9.
	c.Assert(mariaDBIndexType(c, ctx, adminDB, name), qt.Equals, "BTREE")

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	// The reader half: the access method reaches the structured field, not only
	// the definition string a comparison would have to parse DDL to read.
	plain, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(spatialIndexOf(c, plain).Method, qt.Equals, "BTREE")

	// The comparison half, over the schema the reader just produced.
	desired := mariaDBSpatialDesired()
	c.Assert(schemadiff.CompareWithDialect(desired, plain, "mariadb").HasChanges(), qt.IsTrue,
		qt.Commentf("a BTREE index does not satisfy a requested SPATIAL index"))

	// The repair, and the convergence after it. An access-method change is a
	// rebuild on this engine: the index goes and comes back.
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf("DROP INDEX `sx_geo_location` ON `%s`.`geo`", name))
	c.Assert(err, qt.IsNil)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE SPATIAL INDEX `sx_geo_location` ON `%s`.`geo` (`location`)",
		name,
	))
	c.Assert(err, qt.IsNil)
	c.Assert(mariaDBIndexType(c, ctx, adminDB, name), qt.Equals, "SPATIAL")

	repaired, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(spatialIndexOf(c, repaired).Method, qt.Equals, "SPATIAL")
	c.Assert(schemadiff.CompareWithDialect(desired, repaired, "mariadb").HasChanges(), qt.IsFalse,
		qt.Commentf("the requested spatial index is what the server now has"))
}

// mariaDBSpatialDesired is the schema asking for the spatial index.
func mariaDBSpatialDesired() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "geo", StructName: "Geo"}},
		Fields: []schemamodel.Field{
			{StructName: "Geo", Name: "id", Type: "BIGINT", Primary: true, Nullable: false},
			{StructName: "Geo", Name: "location", Type: "POINT", Nullable: false},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Geo", Name: "sx_geo_location", TableName: "geo",
			Fields: []string{"location"}, Type: "SPATIAL",
		}},
	}
}

// mariaDBIndexType asks the catalog what access method the index has, which is
// the fact the whole test is about and the one a client prints nowhere else.
func mariaDBIndexType(c *qt.C, ctx context.Context, adminDB *sql.DB, database string) string {
	c.Helper()
	var indexType string
	err := adminDB.QueryRowContext(ctx,
		`SELECT INDEX_TYPE FROM information_schema.STATISTICS
		  WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'geo' AND INDEX_NAME = 'sx_geo_location'`,
		database).Scan(&indexType)
	c.Assert(err, qt.IsNil)
	return indexType
}

// spatialIndexOf returns the one index this test is about, and fails when the
// read holds none or more than one.
//
// The count is asserted rather than the first match taken: a read that lost the
// index would otherwise hand back a zero value whose empty Method compares
// equal to nothing this test asks about, and the assertion would pass by
// measuring an index that was not there.
func spatialIndexOf(c *qt.C, read *catalog.Database) catalog.Index {
	c.Helper()
	matching := make([]catalog.Index, 0, 1)
	for _, index := range read.Indexes {
		matching = appendIfNamed(matching, index, "sx_geo_location")
	}
	c.Assert(matching, qt.HasLen, 1, qt.Commentf("indexes read: %+v", read.Indexes))
	return matching[0]
}

// appendIfNamed is the filter, kept out of the test so the test body stays
// declarative.
func appendIfNamed(into []catalog.Index, index catalog.Index, name string) []catalog.Index {
	if index.Name != name {
		return into
	}
	return append(into, index)
}

// TestMySQLPromotedSpatialIndexStaysSyncedLive is the control the asymmetry
// exists for, measured on the engine that causes it.
//
// MySQL answers `INDEX_TYPE=SPATIAL` to a plain `CREATE INDEX` over a `POINT`
// column -- measured on 8.4.11 and on 26.7.0, which is what the integration
// workflow starts, rather than read off the manual. A schema declaring no
// index type therefore has a database index whose method it never asked for,
// and comparing the two would plan a rebuild MySQL undoes on the spot: the
// next read reports SPATIAL again, and the plan comes back forever.
//
// So a declared-nothing index accepts any method, and this is what would break
// if that rule were dropped. Measured: with [mysqlindex.Kind.SatisfiedBy]
// narrowed to an equality, this test fails and the spatial one still passes.
func TestMySQLPromotedSpatialIndexStaysSyncedLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, dbtarget.MySQLAdmin))
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	c.Assert(adminDB.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_promoted_%d", time.Now().UnixNano())
	createMySQLDatabase(c, ctx, adminDB, name)
	defer dropMySQLDatabase(c, context.Background(), adminDB, name)
	dbURL := replaceMySQLDatabaseName(c, dbtarget.URL(t, dbtarget.MySQLAdmin), name)

	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`geo` "+
			"(`id` BIGINT NOT NULL PRIMARY KEY, `location` POINT NOT NULL SRID 0)",
		name,
	))
	c.Assert(err, qt.IsNil)
	// The plain spelling. What the server makes of it is the point.
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE INDEX `sx_geo_location` ON `%s`.`geo` (`location`)",
		name,
	))
	c.Assert(err, qt.IsNil)
	c.Assert(mariaDBIndexType(c, ctx, adminDB, name), qt.Equals, "SPATIAL",
		qt.Commentf("this control is about MySQL's promotion; without it the test measures nothing"))

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(spatialIndexOf(c, read).Method, qt.Equals, "SPATIAL")

	// The desired schema declares no index type, which is what an author writes
	// for an ordinary index.
	desired := mariaDBSpatialDesired()
	desired.Indexes[0].Type = ""

	c.Assert(schemadiff.CompareWithDialect(desired, read, "mysql").HasChanges(), qt.IsFalse,
		qt.Commentf("an index declaring no method accepts the one the engine chose"))
}
