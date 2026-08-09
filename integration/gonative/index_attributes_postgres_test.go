//go:build integration

// Live guard for the index half of stokaro/ptah#1242 and for the two members of
// the same family the issue's own list did not reach: an operator class's
// PARAMETERS and an index's WITH (...) storage parameters.
//
// Why a live server rather than a fake one. The reader's index query asks
// PostgreSQL four per-key questions -- pg_index.indkey, indclass, indoption, and
// the index attribute's pg_attribute.attoptions -- and one relation-level
// question, pg_class.reloptions. A fake server can be told whether the query
// mentions a catalog column, and internal/dbschema/postgres holds that guard.
// What it cannot be told is whether the query joins to the RIGHT relation.
// `attrelid = ix.indexrelid` and `attrelid = ix.indrelid` both mention
// attoptions and both parse; only one of them names the index's own attribute,
// and the other quietly reports the table column's options instead. Measured on
// PostgreSQL 17.10, a real server separates them and nothing short of one does.
//
// Each fixture is one throwaway database holding one table, so a failure names
// one attribute. The plain btree index is the control: it varies in none of
// them and must stay clean of all of them.

package gonative_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
)

// indexAttributeSeed builds the whole set of #1242 shapes in one database. They
// are separate tables rather than separate databases because each index is read
// independently and named independently in the assertion below, and one server
// round trip that reads them together also proves the per-key vectors are
// aligned per index rather than per query.
func indexAttributeSeed() []string {
	return []string{
		"CREATE TABLE t (" +
			"id integer PRIMARY KEY, doc jsonb, code text, name text, " +
			"created_at timestamptz, score integer, a integer, b integer, c integer, " +
			"tsv tsvector, ts timestamptz)",
		"CREATE INDEX i_plain ON t (name)",
		"CREATE INDEX i_gin ON t USING gin (doc)",
		"CREATE INDEX i_opclass ON t (code text_pattern_ops)",
		"CREATE INDEX i_expr ON t (lower(name))",
		"CREATE INDEX i_desc ON t (created_at DESC NULLS LAST)",
		"CREATE INDEX i_nullsfirst ON t (score NULLS FIRST)",
		"CREATE INDEX i_include ON t (a, b) INCLUDE (c)",
		"CREATE INDEX i_opclass_params ON t USING gist (tsv tsvector_ops (siglen = 64))",
		"CREATE INDEX i_storage ON t USING brin (ts) WITH (pages_per_range = 32)",
		"CREATE INDEX i_storage_unrepresentable ON t (name) WITH (fillfactor = 70)",
	}
}

// TestPostgreSQLIndexAttributes_SurviveTheRead is the enumeration #1242 asks
// for, one row per attribute the reader collects, read off a live server.
//
// The two rows the issue's list did not have are the last two. An operator
// class can be the key type's DEFAULT and still carry parameters -- measured
// here, tsvector_ops under gist with siglen=64 reports opcdefault true -- so a
// reader that names a class only when it is not the default drops the
// parameters with it, and the index it rebuilds has the 124-byte default
// signature. Neither loss is an error at replay time: psql accepts both at exit
// 0 and the wrong index is simply there.
func TestPostgreSQLIndexAttributes_SurviveTheRead(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	dbURL := newBoundaryDatabase(c, dsn, boundaryCase{
		name:  "index_attributes",
		seed:  indexAttributeSeed(),
		query: "search_path=public",
	})
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	live, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	c.Assert(err, qt.IsNil)

	tests := []struct {
		name  string
		index string
		// assert states what the reader must report for this index. Every row
		// asserts the WHOLE Parts slice rather than one field of it, so an
		// attribute that leaks onto a key it does not belong to reddens the row
		// that owns the key as well as the row that owns the attribute.
		assert func(c *qt.C, index dbschematypes.DBIndex)
	}{
		{
			name:  "plain btree control carries nothing extra",
			index: "i_plain",
			assert: func(c *qt.C, index dbschematypes.DBIndex) {
				c.Assert(index.Method, qt.Equals, "btree")
				c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{{Name: "name"}})
				c.Assert(index.IncludeColumns, qt.IsNil)
				c.Assert(index.StorageParams, qt.IsNil)
			},
		},
		{
			name:  "access method",
			index: "i_gin",
			assert: func(c *qt.C, index dbschematypes.DBIndex) {
				c.Assert(index.Method, qt.Equals, "gin")
				c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{{Name: "doc"}})
			},
		},
		{
			name:  "operator class",
			index: "i_opclass",
			assert: func(c *qt.C, index dbschematypes.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{
					{Name: "code", Operator: "text_pattern_ops"},
				})
			},
		},
		{
			name:  "expression key",
			index: "i_expr",
			assert: func(c *qt.C, index dbschematypes.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{
					{Expr: "lower(name)"},
				})
			},
		},
		{
			name:  "sort direction and nulls ordering",
			index: "i_desc",
			assert: func(c *qt.C, index dbschematypes.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{
					{Name: "created_at", Desc: true, NullsOrder: dbschematypes.NullsOrderLast},
				})
			},
		},
		{
			name:  "nulls first on an ascending key",
			index: "i_nullsfirst",
			assert: func(c *qt.C, index dbschematypes.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{
					{Name: "score", NullsOrder: dbschematypes.NullsOrderFirst},
				})
			},
		},
		{
			name:  "include payload",
			index: "i_include",
			assert: func(c *qt.C, index dbschematypes.DBIndex) {
				c.Assert(index.IncludeColumns, qt.DeepEquals, []string{"c"})
				c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{
					{Name: "a"}, {Name: "b"},
				})
			},
		},
		{
			// The row a fake server cannot hold: the parameters come from the
			// INDEX relation's pg_attribute row, not the table's.
			name:  "operator class parameters",
			index: "i_opclass_params",
			assert: func(c *qt.C, index dbschematypes.DBIndex) {
				c.Assert(index.Method, qt.Equals, "gist")
				c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{
					{Name: "tsv", Operator: "tsvector_ops(siglen=64)"},
				})
			},
		},
		{
			name:  "storage parameters",
			index: "i_storage",
			assert: func(c *qt.C, index dbschematypes.DBIndex) {
				c.Assert(index.Method, qt.Equals, "brin")
				c.Assert(index.StorageParams, qt.DeepEquals, map[string]string{
					"pages_per_range": "32",
				})
			},
		},
		{
			// fillfactor has no slot on any surface downstream, so recording it
			// would make this index differ from its own inspected document on
			// every run. Dropping it is the decision, and this row is where it
			// is written down rather than merely happening.
			name:  "a storage parameter no surface downstream can write",
			index: "i_storage_unrepresentable",
			assert: func(c *qt.C, index dbschematypes.DBIndex) {
				c.Assert(index.StorageParams, qt.IsNil)
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			test.assert(c, findLiveIndex(c, live.Indexes, test.index))
		})
	}
}

// TestPostgreSQLIndexAttributes_ApplyingItsOwnDescriptionChangesNothing closes
// the loop the read alone does not.
//
// Reading an attribute is only half of carrying it. The value has to survive
// the converter, the HCL writer, the HCL reader and the comparator as well, and
// a drop at any one of them turns a faithful `schema inspect` into a permanent
// rebuild: `schema apply` of a database's own description would drop and
// recreate the index on every single run and report success each time.
//
// This is the guard the storage-parameter comparison needs in particular. That
// comparison is what makes a dropped parameter visible, and it is also what
// would make an unrepresentable one churn forever -- which is why the reader
// records only what the whole chain can write, and why the fillfactor index
// below is part of the fixture rather than a separate one.
func TestPostgreSQLIndexAttributes_ApplyingItsOwnDescriptionChangesNothing(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	tests := []struct {
		name string
		// compatibility selects the surface: the compatibility binary omits
		// blocks the tool it stands in for refuses. Both surfaces must hold
		// this property.
		compatibility bool
	}{
		{name: "native surface", compatibility: false},
		{name: "compatibility surface", compatibility: true},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dbURL := newBoundaryDatabase(c, dsn, boundaryCase{
				name:  "index_attributes_apply",
				seed:  indexAttributeSeed(),
				query: "search_path=public",
			})
			conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

			document := boundaryInspect(c, dbURL, test.compatibility)
			c.Assert(document, qt.Contains, `ops = "tsvector_ops(siglen=64)"`)
			c.Assert(document, qt.Contains, "page_per_range = 32")

			plan := boundaryApplyBack(c, conn, document, test.compatibility)

			c.Assert(indexStatements(plan), qt.DeepEquals, []string(nil))
		})
	}
}

// indexStatements keeps the statements that touch an index, so the assertion
// records what the plan DOES to the objects this file is about. These fixtures
// also carry the owner-grant churn of #1276, which has its own guard elsewhere
// and is not this test's business.
func indexStatements(statements []string) []string {
	var out []string
	for _, statement := range statements {
		if strings.Contains(statement, "INDEX") {
			out = append(out, statement)
		}
	}
	return out
}

func findLiveIndex(c *qt.C, indexes []dbschematypes.DBIndex, name string) dbschematypes.DBIndex {
	c.Helper()

	for _, index := range indexes {
		if index.Name == name {
			return index
		}
	}
	c.Fatalf("index %q was not read at all; the reader saw %d indexes", name, len(indexes))
	return dbschematypes.DBIndex{}
}
