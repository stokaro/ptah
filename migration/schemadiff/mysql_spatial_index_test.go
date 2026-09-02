package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// spatialDesiredSchema asks for a SPATIAL index on a POINT column.
//
//	CREATE TABLE geo (location POINT NOT NULL);
//	CREATE SPATIAL INDEX sx_geo_location ON geo (location);
func spatialDesiredSchema(indexType string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "geo", StructName: "Geo"}},
		Fields: []schemamodel.Field{{
			StructName: "Geo", Name: "location", Type: "POINT", Nullable: false,
		}},
		Indexes: []schemamodel.Index{{
			StructName: "Geo", Name: "sx_geo_location", TableName: "geo",
			Fields: []string{"location"}, Type: indexType,
		}},
	}
}

// spatialDatabaseSchema is what the MySQL-family reader reports for the same
// table, with the index's own access method as `method`.
//
// Both values are reachable on MariaDB 11.8: `CREATE INDEX` over a POINT column
// leaves `INDEX_TYPE=BTREE`, and `CREATE SPATIAL INDEX` leaves
// `INDEX_TYPE=SPATIAL`.
func spatialDatabaseSchema(method string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "geo",
			Type: "BASE TABLE",
			Columns: []catalog.Column{{
				Name: "location", DataType: "point", ColumnType: "point", IsNullable: "NO",
			}},
		}},
		Indexes: []catalog.Index{{
			Name: "sx_geo_location", TableName: "geo",
			Columns: []string{"location"}, Method: method,
		}},
	}
}

// TestCompareWithDialect_MariaDBSpatialAgainstPlainIsAChange is
// stokaro/ptah#2721 reduced to the comparison.
//
// A desired SPATIAL index and a database BTREE index of the same name over the
// same column are different schema states, and the MySQL-family comparison saw
// neither: it compared uniqueness and key columns, and the reader carried
// `INDEX_TYPE` only inside the opaque definition string. So reconciliation
// reported `InSync` for a table that did not have the requested access method,
// and the drift stayed invisible until somebody read MariaDB's own metadata.
//
// MySQL 8.4 promotes the plain spelling over a POINT column to SPATIAL, so the
// false convergence is MariaDB's; the reader and the comparator are shared, and
// the fix is in both.
func TestCompareWithDialect_MariaDBSpatialAgainstPlainIsAChange(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mariadb", dialect: "mariadb"},
		{name: "mysql", dialect: "mysql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				spatialDesiredSchema("SPATIAL"), spatialDatabaseSchema("BTREE"), test.dialect)

			c.Assert(diff.HasChanges(), qt.IsTrue,
				qt.Commentf("a BTREE index does not satisfy a requested SPATIAL index: %+v", diff))
			// A rebuild, which is what an access-method change takes on these
			// engines: the index is dropped and created again.
			c.Assert(diff.IndexesRemoved, qt.HasLen, 1)
			c.Assert(diff.IndexesRemoved[0].Name, qt.Equals, "sx_geo_location")
			c.Assert(diff.IndexesAdded, qt.HasLen, 1)
			c.Assert(diff.IndexesAdded[0].Index.Name, qt.Equals, "sx_geo_location")
		})
	}
}

// TestCompareWithDialect_MariaDBMatchingSpatialIndexesAreSynced is the control.
//
// A comparison that reported every index changed would satisfy the test above
// and plan a rebuild on every run for a key nobody touched, which is the
// oscillation the MySQL branch's own doc comment exists to prevent.
func TestCompareWithDialect_MariaDBMatchingSpatialIndexesAreSynced(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mariadb", dialect: "mariadb"},
		{name: "mysql", dialect: "mysql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				spatialDesiredSchema("SPATIAL"), spatialDatabaseSchema("SPATIAL"), test.dialect)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
		})
	}
}

// TestCompareWithDialect_MariaDBAPlainIndexIsSyncedAgainstAnyReportedMethod is
// the other control, and it is the one that keeps this change from planning a
// rebuild for every ordinary index in every existing schema.
//
// A desired index that names no type asks for the engine's default, and the
// engine answers `BTREE`, `FULLTEXT` or `SPATIAL` depending on the column and
// the DDL. Comparing an unnamed desired type against whatever the server
// reported would find every index in the world different from itself.
func TestCompareWithDialect_MariaDBAPlainIndexIsSyncedAgainstAnyReportedMethod(t *testing.T) {
	tests := []struct {
		name   string
		method string
	}{
		{name: "btree", method: "BTREE"},
		{name: "spatial", method: "SPATIAL"},
		{name: "fulltext", method: "FULLTEXT"},
		{name: "the reader reported none", method: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				spatialDesiredSchema(""), spatialDatabaseSchema(test.method), "mariadb")

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
		})
	}
}
