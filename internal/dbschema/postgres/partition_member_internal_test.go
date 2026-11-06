package postgres

// White-box testing required: which partition facts the reader records, and which
// catalog relations it asks, are visible only through readTableParents,
// markInheritedConstraints and readTriggersForSchema. Driving them through ReadSchema
// would need a fake answering every unrelated read of the schema walk.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/dbschema/dbtest"
)

func parentRows(rows ...[]driver.Value) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{Columns: []string{"child_schema", "child", "parent_schema", "parent", "is_partition", "bound"}, Rows: rows}, nil
	}
}

func TestReadTableParents_RecordsAPartitionsParentAndBound(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, parentRows(
		[]driver.Value{"public", "child", "public", "base", false, nil},
		[]driver.Value{"public", "e1", "public", "events", true, "FOR VALUES IN ('a')"},
		[]driver.Value{"public", "e2", "archive", "events", true, "DEFAULT"},
		[]driver.Value{"public", "unread", "public", "events", true, "DEFAULT"},
		[]driver.Value{"archive", "plain", "archive", "events", true, "DEFAULT"},
	))
	reader := &Reader{db: db.SQL, dialect: platform.Postgres, schema: "public", caps: capability.Postgres18()}
	database := &catalog.Database{Tables: []catalog.Table{{Name: "child"}, {Name: "e1"}, {Name: "e2"}, {Name: "plain"}}}

	c.Assert(reader.readTableParents(t.Context(), database), qt.IsNil)

	c.Assert(database.Tables[0].Parents, qt.DeepEquals, []string{"base"})
	c.Assert(database.Tables[0].Facets.Len(), qt.Equals, 0)
	c.Assert(database.Tables[1].Parents, qt.DeepEquals, []string{"events"})
	c.Assert(partitionMemberFacet(c, database.Tables[1]), qt.DeepEquals, &pgtable.ObservedPartitionMember{Parent: "events", Bound: "FOR VALUES IN ('a')"})
	c.Assert(partitionMemberFacet(c, database.Tables[2]), qt.DeepEquals, &pgtable.ObservedPartitionMember{ParentSchema: "archive", Parent: "events", Bound: "DEFAULT"})
	c.Assert(database.Tables[3].Parents, qt.HasLen, 0)
	c.Assert(database.FeatureCoverage.SelectKinds([]schemaext.Kind{pgtable.PartitionMemberKind}).IsZero(), qt.IsFalse)
}

func partitionMemberFacet(c *qt.C, table catalog.Table) *pgtable.ObservedPartitionMember {
	c.Helper()
	value, found, err := schemaext.FacetAs[*pgtable.ObservedPartitionMember](table.Facets, pgtable.PartitionMemberKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return value
}

// TestReadTableParents_ClaimsMembershipOnlyWhereTheStatementExists is the control for
// a server whose catalog has pg_inherits and no PARTITION OF: CockroachDB answers
// the query, and a coverage claim would be about a kind nothing serves there.
func TestReadTableParents_ClaimsMembershipOnlyWhereTheStatementExists(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, parentRows())
	reader := &Reader{db: db.SQL, dialect: platform.CockroachDB, schema: "public", caps: capability.CockroachDB26()}
	database := &catalog.Database{Tables: []catalog.Table{{Name: "t"}}}

	c.Assert(reader.readTableParents(t.Context(), database), qt.IsNil)

	c.Assert(database.FeatureCoverage.IsZero(), qt.IsTrue)
}

// TestReadTableParents_RefusesAnUnreadableBound keeps a bound the owner cannot read
// from turning into a partition that compares as nothing.
func TestReadTableParents_RefusesAnUnreadableBound(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, parentRows([]driver.Value{"public", "e1", "public", "events", true, "FOR SOMETHING NEW"}))
	reader := &Reader{db: db.SQL, dialect: platform.Postgres, schema: "public", caps: capability.Postgres18()}

	err := reader.readTableParents(t.Context(), &catalog.Database{Tables: []catalog.Table{{Name: "e1"}}})

	c.Assert(err, qt.ErrorMatches, `read partition bound of e1: .*PostgreSQL partition bound: expected FOR VALUES or DEFAULT`)
}

// TestReadTableParents_ClaimsNothingWithoutThePartitionCatalog is the control for a
// server with no pg_inherits: no query, and no coverage that says a table is ordinary.
func TestReadTableParents_ClaimsNothingWithoutThePartitionCatalog(t *testing.T) {
	c := qt.New(t)
	asked := 0
	db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		asked++
		return dbtest.QueryResult{}, nil
	})
	reader := &Reader{db: db.SQL, dialect: platform.Spanner, schema: "public", caps: capability.ForDialect(platform.Spanner)}
	database := &catalog.Database{Tables: []catalog.Table{{Name: "t"}}}

	c.Assert(reader.readTableParents(t.Context(), database), qt.IsNil)

	c.Assert(asked, qt.Equals, 0)
	c.Assert(database.FeatureCoverage.IsZero(), qt.IsTrue)
}

func TestMarkInheritedConstraints(t *testing.T) {
	c := qt.New(t)
	var query string
	db := dbtest.Open(t, func(sql string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		query = sql
		return dbtest.QueryResult{Columns: []string{"schema", "table", "name"}, Rows: [][]driver.Value{
			{"public", "e1", "e1_pkey"},
			{"public", "e1", "positive"},
			{"public", "unread", "positive"},
			{"archive", "events", "positive"},
		}}, nil
	})
	reader := &Reader{db: db.SQL, dialect: platform.Postgres, schema: "public", caps: capability.Postgres18()}
	constraints := []catalog.Constraint{
		{TableName: "events", Name: "positive"},
		{TableName: "e1", Name: "e1_pkey"},
		{TableName: "e1", Name: "positive"},
		{TableName: "e1", Name: "small"},
	}

	c.Assert(reader.markInheritedConstraints(t.Context(), constraints), qt.IsNil)

	marks := make([]bool, 0, len(constraints))
	for _, constraint := range constraints {
		marks = append(marks, constraint.Inherited)
	}
	c.Assert(marks, qt.DeepEquals, []bool{false, true, true, false})
	c.Assert(query, qt.Contains, "con.conparentid <> 0 OR NOT con.conislocal")
}

func TestMarkInheritedConstraints_AsksNothingWithoutPartitions(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "a server whose catalog has no partition relation", dialect: platform.Spanner, caps: capability.ForDialect(platform.Spanner)},
		{name: "a server with that relation and no partitions", dialect: platform.CockroachDB, caps: capability.CockroachDB26()},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			asked := 0
			db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
				asked++
				return dbtest.QueryResult{}, nil
			})
			reader := &Reader{db: db.SQL, dialect: test.dialect, schema: "public", caps: test.caps}

			c.Assert(reader.markInheritedConstraints(t.Context(), []catalog.Constraint{{TableName: "t", Name: "c"}}), qt.IsNil)

			c.Assert(asked, qt.Equals, 0)
		})
	}
}

// TestReadTriggers_LeavesOutAPartitionsCopy asks the catalog for triggers of their
// own, on a server that has copies. The query text is the assertion target: the fake
// answers by column name whatever the predicate says.
func TestReadTriggers_LeavesOutAPartitionsCopy(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		want    bool
	}{
		{name: "a server with partitions", dialect: platform.Postgres, caps: capability.Postgres18(), want: true},
		{name: "a server whose catalog has no partition relation", dialect: platform.Spanner, caps: capability.ForDialect(platform.Spanner)},
		{name: "a server with that relation and no partitions", dialect: platform.CockroachDB, caps: capability.CockroachDB26()},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var query string
			db := dbtest.Open(t, func(sql string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				query = sql
				return dbtest.QueryResult{}, nil
			})
			reader := &Reader{db: db.SQL, dialect: test.dialect, schema: "public", caps: test.caps}

			_, err := reader.readTriggersForSchema(t.Context(), "public")

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Contains(query, "tgparentid"), qt.Equals, test.want)
		})
	}
}
