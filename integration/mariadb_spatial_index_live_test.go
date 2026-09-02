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

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
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
// INDEX` over a POINT column leaves `INDEX_TYPE=BTREE` here; MySQL 8.4 promotes
// the same statement to SPATIAL, so the false convergence cannot be built
// there. The reader and the comparator are shared, and the fix is in both.
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
		"CREATE TABLE `%s`.`geo` (`id` BIGINT NOT NULL PRIMARY KEY, `location` POINT NOT NULL)", name))
	c.Assert(err, qt.IsNil)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE INDEX `sx_geo_location` ON `%s`.`geo` (`location`)", name))
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
		"CREATE SPATIAL INDEX `sx_geo_location` ON `%s`.`geo` (`location`)", name))
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
