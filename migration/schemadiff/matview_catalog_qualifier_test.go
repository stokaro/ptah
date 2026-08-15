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
	tests := []struct {
		name    string
		dialect string
		schema  string
	}{
		{name: "PostgreSQL", dialect: "postgres", schema: "analytics"},
		{name: "ClickHouse", dialect: "clickhouse", schema: "mvqual"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
	tests := []struct {
		name    string
		dialect string
		schema  string
	}{
		{name: "PostgreSQL", dialect: "postgres", schema: "analytics"},
		{name: "ClickHouse", dialect: "clickhouse", schema: "mvqual"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated, database := materializedViewReadbackFixtures(test.schema, "archive")

			diff := schemadiff.CompareWithDialect(generated, database, test.dialect)

			c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
			c.Assert(diff.MaterializedViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
		})
	}
}

// TestCompareWithDialect_MaterializedViewAliasedBodyMatchesCatalogReadback pins
// the round trip of a materialized view whose body gives its source an alias.
//
// This is the same readback, one spelling further along. Measured on ClickHouse
// 26.7.3.19, a body authored as `SELECT u.id AS id FROM users AS u` comes back
// from system.tables.as_select as
// `SELECT u.id AS id FROM mvqual.users AS u`: the alias is untouched and only
// the relation gained the database name. Reading the alias prefix as an authored
// schema qualifier refused the normalization entirely and reported a body change
// for a declaration nobody edited -- which the ClickHouse planner answers with a
// drop and a create.
//
// Only ClickHouse is in the table, and deliberately. Measured on PostgreSQL
// 18.4, pg_get_viewdef rewrites the same authored body further than the
// qualifier: `SELECT u.id AS id FROM users AS u` comes back as
// `SELECT id FROM analytics.users u`, dropping both the alias prefix and the
// `AS`, and a body that needs the prefix comes back with a parenthesized join
// tree. Those rewrites are a separate normalization gap, they predate this
// change, and pinning a PostgreSQL row here would mean inventing a readback that
// server does not produce.
func TestCompareWithDialect_MaterializedViewAliasedBodyMatchesCatalogReadback(t *testing.T) {
	c := qt.New(t)

	generated, database := aliasedMaterializedViewReadbackFixtures("mvqual", "mvqual")

	diff := schemadiff.CompareWithDialect(generated, database, "clickhouse")

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

// TestCompareWithDialect_MaterializedViewAliasedRelationSchemaStillDiffs is the
// inverse control: with the alias present, a relation resolved into some other
// schema is still a different body.
func TestCompareWithDialect_MaterializedViewAliasedRelationSchemaStillDiffs(t *testing.T) {
	c := qt.New(t)

	generated, database := aliasedMaterializedViewReadbackFixtures("mvqual", "archive")

	diff := schemadiff.CompareWithDialect(generated, database, "clickhouse")

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

// aliasedMaterializedViewReadbackFixtures is materializedViewReadbackFixtures
// with the source aliased and the projection qualified by that alias, the
// spelling the catalog keeps and the declaration therefore has to keep too.
func aliasedMaterializedViewReadbackFixtures(
	schema string,
	readbackSchema string,
) (*goschema.Database, *types.DBSchema) {
	generated := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserIDs",
			Name:       schema + ".user_ids",
			Body:       "SELECT u.id AS id FROM users AS u",
		}},
		Views: []goschema.View{{
			StructName: "UserIDsPlain",
			Name:       schema + ".user_ids_plain",
			Body:       "SELECT u.id AS id FROM users AS u",
		}},
	}
	database := &types.DBSchema{
		MatViews: []types.DBMatView{{
			Name:            "user_ids",
			Schema:          schema,
			Body:            "SELECT u.id AS id FROM " + readbackSchema + ".users AS u",
			RefreshStrategy: "manual",
		}},
		Views: []types.DBView{{
			Name: "user_ids_plain",
			// The plain view's readback always carries the object's own schema:
			// it is the control for the normalization, not a second subject.
			Schema: schema,
			Body:   "SELECT u.id AS id FROM " + schema + ".users AS u",
		}},
	}
	return generated, database
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
