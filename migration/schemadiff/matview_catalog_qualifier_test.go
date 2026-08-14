package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDialect_MaterializedViewBodyMatchesCatalogReadback pins the
// round trip of a materialized view whose body names its source without a
// qualifier.
//
// A server records the definition it resolved rather than the text the author
// wrote. Measured on PostgreSQL 18.4, a body authored as `FROM users` comes back
// from pg_get_viewdef as `FROM analytics.users`; measured on ClickHouse
// 26.7.3.19, system.tables.as_select answers `FROM mvqual.users` for the same
// authored body. Both are the object the declaration named, so an unchanged
// declaration must produce no diff.
//
// The plain view standing beside the materialized one in each row is the
// control: that half normalized the readback already, so a row where only the
// materialized half moves is the whole finding.
func TestCompareWithDialect_MaterializedViewBodyMatchesCatalogReadback(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dialect string
		schema  string
	}{
		{name: "PostgreSQL", dialect: "postgres", schema: "analytics"},
		{name: "ClickHouse", dialect: "clickhouse", schema: "mvqual"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			generated, database := materializedViewReadbackFixtures(test.schema, test.schema)

			diff := schemadiff.CompareWithDialect(generated, database, test.dialect)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
		})
	}
}

// TestCompareWithDialect_MaterializedViewWrongSchemaRelationStillDiffs is the
// inverse control for the normalization above.
//
// Only the qualifier the object's own schema adds is removable. A body reading a
// relation in some other schema is a different body, and a normalization that
// stripped every qualifier instead of that one would report these two as equal.
func TestCompareWithDialect_MaterializedViewWrongSchemaRelationStillDiffs(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dialect string
		schema  string
	}{
		{name: "PostgreSQL", dialect: "postgres", schema: "analytics"},
		{name: "ClickHouse", dialect: "clickhouse", schema: "mvqual"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			generated, database := materializedViewReadbackFixtures(test.schema, "archive")

			diff := schemadiff.CompareWithDialect(generated, database, test.dialect)

			c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
			c.Assert(diff.MaterializedViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
		})
	}
}

// materializedViewReadbackFixtures builds a desired schema whose bodies name
// their source without a qualifier, and a catalog read of the same two objects
// whose bodies carry readbackSchema as the qualifier the server resolved.
func materializedViewReadbackFixtures(
	schema string,
	readbackSchema string,
) (*goschema.Database, *types.DBSchema) {
	generated := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserCounts",
			Name:       schema + ".user_counts",
			Body:       "SELECT count(*) AS c FROM users",
		}},
		Views: []goschema.View{{
			StructName: "UserCountsPlain",
			Name:       schema + ".user_counts_plain",
			Body:       "SELECT count(*) AS c FROM users",
		}},
	}
	database := &types.DBSchema{
		MatViews: []types.DBMatView{{
			Name:            "user_counts",
			Schema:          schema,
			Body:            "SELECT count(*) AS c FROM " + readbackSchema + ".users",
			RefreshStrategy: "manual",
		}},
		Views: []types.DBView{{
			Name: "user_counts_plain",
			// The plain view's readback carries the object's own schema in
			// every row: it is the control for the normalization, not a second
			// subject of it.
			Schema: schema,
			Body:   "SELECT count(*) AS c FROM " + schema + ".users",
		}},
	}
	return generated, database
}
