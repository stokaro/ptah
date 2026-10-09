package atlasschema

// White-box testing required: baseline statement validation is private to the
// rehearsal, whose public entry point requires a live database connection.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

func TestRehearsalBaselineAcceptsYDBDirectoryObjects(t *testing.T) {
	c := qt.New(t)
	err := guardRehearsalBaseline([]string{"CREATE TABLE items (id Int64 NOT NULL, PRIMARY KEY (id));"}, catalog.ServerInfo{Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
}

func TestRehearsalBaselineRefusesDatabaseWideWorkloadChanges(t *testing.T) {
	for _, statement := range []string{
		"ALTER RESOURCE POOL default WITH (resource_weight = 50);",
		"CREATE RESOURCE POOL batch WITH (concurrent_query_limit = 10);",
		"CREATE RESOURCE POOL CLASSIFIER etl WITH (resource_pool = 'batch', rank = 1);",
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			err := guardRehearsalBaseline([]string{"CREATE TABLE items (id Int64 NOT NULL, PRIMARY KEY (id));", statement}, catalog.ServerInfo{Dialect: "ydb"})
			c.Assert(err, qt.ErrorMatches, `(?s)baseline statement 2 cannot be rehearsed: .*`)
		})
	}
}
