package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/internal/sqlschema"
)

func TestReadYQLTable(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE TABLE `app/a.b` (`i\\x64` Int64 NOT NULL, `v\\`x.y` Utf8, PRIMARY KEY (`i\\x64`), INDEX `by.v` GLOBAL ASYNC ON (`v\\`x.y`) COVER (`i\\x64`) WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, UNIFORM_PARTITIONS = 4);"), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Schema, qt.Equals, "app")
	c.Assert(database.Tables[0].Name, qt.Equals, "a.b")
	c.Assert(database.Fields, qt.HasLen, 2)
	c.Assert(database.Fields[0].Name, qt.Equals, "id")
	c.Assert(database.Fields[1].Name, qt.Equals, "v`x.y")
	c.Assert(database.Indexes, qt.HasLen, 1)
	c.Assert(database.Indexes[0].Name, qt.Equals, "by.v")
	c.Assert(database.Indexes[0].Fields, qt.DeepEquals, []string{"v`x.y"})
	c.Assert(database.Indexes[0].IncludeColumns, qt.DeepEquals, []string{"id"})
	c.Assert(database.Indexes[0].Type, qt.Equals, "async")
	c.Assert(database.Indexes[0].Partitioning, qt.IsNotNil)
	c.Assert(database.Tables[0].YDBPartitioning, qt.IsNotNil)
}

func TestReadYQLColumnTable(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE TABLE events (`a,b` Uint64 NOT NULL, body Utf8, PRIMARY KEY (`a,b`), INDEX body_bloom LOCAL USING bloom_filter ON (body) WITH (false_positive_probability = 0.05)) PARTITION BY HASH (`a,b`) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8);"), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables[0].YDBColumnTable.HashColumns, qt.DeepEquals, []string{"a,b"})
	c.Assert(database.Tables[0].YDBColumnTable.Partitions, qt.Equals, uint64(8))
	c.Assert(database.Indexes[0].StorageParams, qt.DeepEquals, map[string]string{"false_positive_probability": "0.05"})
}

func TestReadYQLDefaults(t *testing.T) {
	for _, test := range []struct{ name, columnType, literal, want string }{
		{"null string", "Utf8", `"NULL"u`, "'NULL'"},
		{"null", "Utf8", "NULL", "NULL"},
		{"whitespace", "String", `" x "`, "' x '"},
		{"apostrophe", "Utf8", `"it's"u`, "'it''s'"},
		{"raw delimiter text", "String", "@@first\nDELIMITER $$\nlast@@", "'first\nDELIMITER $$\nlast'"},
		{"integer", "Int64", "42l", "'42'"},
		{"decimal", "Decimal(22,9)", "Decimal('1.25', 22, 9)", "'1.25'"},
		{"timestamp", "Timestamp", `Timestamp("2026-01-02T00:00:00Z")`, "'2026-01-02T00:00:00Z'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte("CREATE TABLE t (id Int64 NOT NULL, v "+test.columnType+" DEFAULT "+test.literal+", PRIMARY KEY (id));"), "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(database.Fields[1].Default, qt.Equals, test.want)
			c.Assert(database.Fields[1].DefaultSet, qt.IsTrue)
		})
	}
}

func TestReadYQLRefusals(t *testing.T) {
	for _, text := range []string{
		"CREATE VIEW v AS SELECT 1;", "PRAGMA TablePathPrefix = '/local';", "SELECT 1;",
		"CREATE TABLE t (id Int64, PRIMARY KEY (id));",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (missing));",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)) WITH (unknown = 1);",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)) WITH (STORE = ROW, STORE = COLUMN);",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id), INDEX i GLOBAL ASYNC USING vector_kmeans_tree ON (id));",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id), INDEX i GLOBAL ON (id) WITH (vector_dimension = 2));",
		"CREATE TABLE t (id Int64 NOT NULL, v Utf8 DEFAULT Random(), PRIMARY KEY (id));",
		"CREATE TABLE t (id Int64 NOT NULL, v Decimal(22,9) DEFAULT Decimal('1', 10, 2), PRIMARY KEY (id));",
		"CREATE TABLE t (id Int64 NOT NULL, v Int64 DEFAULT 42, PRIMARY KEY (id));",
		`CREATE TABLE t (id Int64 NOT NULL, v String DEFAULT "x"pt, PRIMARY KEY (id));`,
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)); DELETE FROM t;",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(text), "ydb")
			c.Assert(err, qt.ErrorMatches, "YQL schema at position .*")
			c.Assert(statements, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

func TestReadYQLSupportedFamilyCoverage(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read(nil, "ydb")
	c.Assert(err, qt.IsNil)
	for _, kind := range []coverage.Kind{coverage.Replication, coverage.Transfer, coverage.Role, coverage.Grant, coverage.StreamingQuery, coverage.Changefeed, coverage.Secret, coverage.ExternalDataSource, coverage.ExternalTable, coverage.ResourcePool, coverage.ResourcePoolClassifier, coverage.View, coverage.Topic, coverage.ColumnTable, coverage.TTL, coverage.ColumnFamily} {
		c.Assert(database.NotDescribed.Describes(kind), qt.IsTrue)
	}
}
