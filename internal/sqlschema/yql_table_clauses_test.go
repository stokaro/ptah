package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/sqlschema"
)

func TestReadYQLTieredTTL(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE TABLE events (ts Timestamp NOT NULL, id Uint64, PRIMARY KEY (ts)) WITH (STORE = COLUMN, TTL = Interval('PT1H') TO EXTERNAL DATA SOURCE `/local/archive/cold`, Interval('P7D') DELETE ON ts);"), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables[0].YDBColumnTable.TTL, qt.DeepEquals, &ast.YDBTieredTTLSpec{Column: "ts", Tiers: []ast.YDBTTLTierSpec{
		{Interval: "PT1H", ExternalSource: "/local/archive/cold"}, {Interval: "P7D"},
	}})
	c.Assert(database.Tables[0].Facets.Len(), qt.Equals, 0)
}

func TestReadYQLColumnFamilies(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE TABLE t (id Int64 NOT NULL, `a,b` Utf8 FAMILY `pay\\x6coad`, v String FAMILY payload, PRIMARY KEY (id), FAMILY payload (COMPRESSION = 'lz4'), FAMILY `default` (CACHE_MODE = 'regular'));"), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables[0].YDBColumnFamilies, qt.DeepEquals, []ast.YDBColumnFamilySpec{
		{Name: "payload", Compression: "lz4", Columns: []string{"a,b", "v"}},
		{Name: "default", CacheMode: "regular"},
	})
}

func TestReadYQLTableClauseRefusals(t *testing.T) {
	for _, text := range []string{
		"CREATE TABLE t (id Int64 NOT NULL, v Utf8 FAMILY missing, PRIMARY KEY (id));",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id), FAMILY f (), FAMILY f ());",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id), FAMILY f (unknown = 1));",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)) WITH (TTL = Interval('PT1H') ON id AS DAYS);",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)) WITH (TTL = Interval('invalid') ON id);",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)) WITH (TTL = Interval('PT1H') DELETE ON id);",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)) WITH (STORE = COLUMN, TTL = Interval('P7D') TO EXTERNAL DATA SOURCE cold, Interval('PT1H') DELETE ON id);",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)) WITH (TTL = Interval('PT1H'), STORE = COLUMN);",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			_, statements, err := sqlschema.Read([]byte(text), "ydb")
			c.Assert(err, qt.ErrorMatches, "YQL schema at position .*")
			c.Assert(statements, qt.IsNil)
		})
	}
}
