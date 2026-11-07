package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

func readWithBuiltinOwners(c *qt.C, source, target string) (schemamodel.Database, error) {
	c.Helper()
	runtime := must.Must(builtin.New())
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(source), target, nil,
		sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary(), Relations: runtime})
	return database, err
}

func memberOfTable(c *qt.C, database schemamodel.Database, name string) *pgtable.PartitionMember {
	c.Helper()
	position := slicesIndexTable(database.Tables, name)
	c.Assert(position >= 0, qt.IsTrue, qt.Commentf("table %s in %v", name, tableNames(database)))
	value, found, err := pgtable.PartitionMemberOf(database.Tables[position].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return value
}

func slicesIndexTable(tables []schemamodel.Table, name string) int {
	for i := range tables {
		if tables[i].Name == name {
			return i
		}
	}
	return -1
}

// TestReadWith_PartitionOf_HappyPath reads each form as a table with no columns of
// its own and with its parent and bound, on both targets that take the statement.
func TestReadWith_PartitionOf_HappyPath(t *testing.T) {
	for _, target := range []string{"postgres", "yugabytedb"} {
		for _, test := range []struct {
			name, source, table string
			want                pgtable.PartitionMember
		}{
			{name: "range", source: "CREATE TABLE e PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');", table: "e",
				want: pgtable.PartitionMember{Parent: "events", Bound: "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"}},
			{name: "list", source: "CREATE TABLE e PARTITION OF events FOR VALUES IN ('a', 'b');", table: "e",
				want: pgtable.PartitionMember{Parent: "events", Bound: "FOR VALUES IN ('a', 'b')"}},
			{name: "hash", source: "CREATE TABLE e PARTITION OF events FOR VALUES WITH (MODULUS 2, REMAINDER 1);", table: "e",
				want: pgtable.PartitionMember{Parent: "events", Bound: "FOR VALUES WITH (MODULUS 2, REMAINDER 1)"}},
			{name: "default", source: "CREATE TABLE e PARTITION OF events DEFAULT;", table: "e", want: pgtable.PartitionMember{Parent: "events", Bound: "DEFAULT"}},
			{name: "guarded", source: "CREATE TABLE IF NOT EXISTS e PARTITION OF events DEFAULT;", table: "e", want: pgtable.PartitionMember{Parent: "events", Bound: "DEFAULT"}},
			{name: "another schema", source: "CREATE TABLE archive.e PARTITION OF app.events DEFAULT;", table: "e",
				want: pgtable.PartitionMember{ParentSchema: "app", Parent: "events", Bound: "DEFAULT"}},
			{name: "quoted names", source: `CREATE TABLE "My E" PARTITION OF "My Events" DEFAULT;`, table: "My E",
				want: pgtable.PartitionMember{Parent: "My Events", Bound: "DEFAULT"}},
			{name: "folded names", source: "CREATE TABLE E PARTITION OF EVENTS DEFAULT;", table: "e", want: pgtable.PartitionMember{Parent: "events", Bound: "DEFAULT"}},
			{name: "a table option after the bound", source: "CREATE TABLE e PARTITION OF events DEFAULT TABLESPACE fast;", table: "e", want: pgtable.PartitionMember{Parent: "events", Bound: "DEFAULT"}},
		} {
			t.Run(target+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				database, err := readWithBuiltinOwners(c, test.source, target)

				c.Assert(err, qt.IsNil)
				c.Assert(*memberOfTable(c, database, test.table), qt.DeepEquals, test.want)
				c.Assert(database.Fields, qt.HasLen, 0)
			})
		}
	}
}

// TestReadWith_PartitionOf_KeepsItsOtherFacets reads what the shared grammar reads
// around the owner's clause: a sub-partition's own key and a table prefix.
func TestReadWith_PartitionOf_KeepsItsOtherFacets(t *testing.T) {
	c := qt.New(t)

	database, err := readWithBuiltinOwners(c, `
CREATE TABLE sales (region text, sold date) PARTITION BY LIST (region);
CREATE TABLE sales_eu PARTITION OF sales FOR VALUES IN ('eu') PARTITION BY RANGE (sold);
CREATE UNLOGGED TABLE sales_us PARTITION OF sales FOR VALUES IN ('us');
`, "postgres")

	c.Assert(err, qt.IsNil)
	key, keyed, err := pgtable.PartitionOf(database.Tables[slicesIndexTable(database.Tables, "sales_eu")].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(keyed, qt.IsTrue)
	c.Assert(key, qt.DeepEquals, &pgtable.Partition{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "sold"}}})
	persistence, persistent, err := pgtable.PersistenceOf(database.Tables[slicesIndexTable(database.Tables, "sales_us")].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(persistent, qt.IsTrue)
	c.Assert(persistence.Unlogged, qt.IsTrue)
	c.Assert(*memberOfTable(c, database, "sales_us"), qt.DeepEquals, pgtable.PartitionMember{Parent: "sales", Bound: "FOR VALUES IN ('us')"})
}

// TestReadWith_PartitionOf_OrdersThePartitionAfterItsParent pins the creation order
// a server requires, whatever order the document writes, and however it spells the
// default schema on either name.
func TestReadWith_PartitionOf_OrdersThePartitionAfterItsParent(t *testing.T) {
	for _, test := range []struct {
		name, source, partition, parent string
	}{
		{name: "the partition written first", source: "CREATE TABLE e PARTITION OF events DEFAULT;\nCREATE TABLE events (a int) PARTITION BY LIST (a);",
			partition: "e", parent: "events"},
		{name: "a parent spelled with the default schema", source: "CREATE TABLE events (a int) PARTITION BY LIST (a);\nCREATE TABLE e PARTITION OF public.events DEFAULT;",
			partition: "e", parent: "events"},
		{name: "a partition in another schema", source: "CREATE TABLE events (a int) PARTITION BY LIST (a);\nCREATE TABLE archive.e PARTITION OF events DEFAULT;",
			partition: "archive.e", parent: "events"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := readWithBuiltinOwners(c, test.source, "postgres")
			c.Assert(err, qt.IsNil)

			c.Assert(database.Dependencies[test.partition], qt.DeepEquals, []string{test.parent})
		})
	}
}

func TestReadWith_PartitionOf_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, source, target, want string
	}{
		{name: "mysql", source: "CREATE TABLE e PARTITION OF events DEFAULT;", target: "mysql",
			want: `line 1, column 16: CREATE TABLE e PARTITION OF declares a PostgreSQL partition, which Ptah does not read on mysql`},
		{name: "mariadb", source: "CREATE TABLE e PARTITION OF events DEFAULT;", target: "mariadb",
			want: `line 1, column 16: CREATE TABLE e PARTITION OF declares a PostgreSQL partition, which Ptah does not read on mariadb`},
		{name: "cockroachdb", source: "CREATE TABLE e PARTITION OF events DEFAULT;", target: "cockroachdb",
			want: `line 1, column 16: CREATE TABLE e PARTITION OF declares a PostgreSQL partition, which Ptah does not read on cockroachdb`},
		{name: "spanner", source: "CREATE TABLE e PARTITION OF events DEFAULT;", target: "spanner",
			want: `line 1, column 16: CREATE TABLE e PARTITION OF declares a PostgreSQL partition, which Ptah does not read on spanner`},
		{name: "sqlite", source: "CREATE TABLE e PARTITION OF events DEFAULT;", target: "sqlite",
			want: `line 1, column 16: CREATE TABLE e PARTITION OF declares a PostgreSQL partition, which Ptah does not read on sqlite`},
		{name: "sqlserver", source: "CREATE TABLE e PARTITION OF events DEFAULT;", target: "sqlserver",
			want: `line 1, column 16: CREATE TABLE e PARTITION OF declares a PostgreSQL partition, which Ptah does not read on sqlserver`},
		{name: "oracle", source: "CREATE TABLE e PARTITION OF events DEFAULT;", target: "oracle",
			want: `line 1, column 16: CREATE TABLE e PARTITION OF declares a PostgreSQL partition, which Ptah does not read on oracle`},
		{name: "clickhouse", source: "CREATE TABLE e PARTITION OF events DEFAULT;", target: "clickhouse",
			want: `line 1, column 16: CREATE TABLE e PARTITION OF declares a PostgreSQL partition, which Ptah does not read on clickhouse`},
		{name: "a column list", source: "CREATE TABLE e PARTITION OF events (CHECK (a > 0)) DEFAULT;", target: "postgres",
			want: `line 1, column 16: table e: .*PARTITION OF with a column or constraint list is not read: .*`},
		{name: "a storage clause", source: "CREATE TABLE e PARTITION OF events DEFAULT WITH (fillfactor=70);", target: "postgres",
			want: `line 1, column 16: table e: .*WITH \(fillfactor=70\) after the bound of PARTITION OF is not read: .*`},
		{name: "a column added to a partition", source: "CREATE TABLE e PARTITION OF events DEFAULT;\nALTER TABLE e ADD COLUMN a int;", target: "postgres",
			want: `.*ALTER TABLE e ADD COLUMN a adds a column to a table with no column list of its own; a partition takes its columns from its parent`},
		{name: "a column of a partition changed", source: "CREATE TABLE e PARTITION OF events DEFAULT;\nALTER TABLE e ALTER COLUMN a SET DEFAULT 1;", target: "postgres",
			want: `ALTER TABLE e ALTER COLUMN a names a column the table does not declare`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := readWithBuiltinOwners(c, test.source, test.target)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
