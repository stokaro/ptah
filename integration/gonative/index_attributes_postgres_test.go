//go:build integration

// Live guard for the index half of stokaro/ptah#1242 and for the two members of
// the same family the issue's own list did not reach: an operator class's
// PARAMETERS and an index's WITH (...) storage parameters.
//
// Why a live server rather than a fake one. The reader's index query asks
// PostgreSQL four per-key questions -- pg_index.indkey, indclass, indoption, and
// the index attribute's pg_attribute.attoptions -- and three relation-level
// ones: pg_class.reloptions, pg_am.amname and the index's own
// obj_description. The fake server in internal/dbschema/postgres models those
// joins rather than merely checking that a catalog column is named, so it does
// catch a query that reaches the table relation instead of the index one. What
// it cannot do is prove the MODEL is right: it answers what the file says
// PostgreSQL answers. This is the guard that reads the real catalog, and every
// value the fake asserts was measured here first.
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
			"tsv tsvector, tsv2 tsvector, tsv3 tsvector, ts timestamptz)",
		// The table's own comment. It is here so the index-comment test below
		// is measuring the index's object and not whichever comment the query
		// happened to reach: obj_description(t.oid, 'pg_class') is the same
		// function on the same catalog, and against a table with no comment it
		// returns the empty string and looks like a reader that simply found
		// none.
		"COMMENT ON TABLE t IS 'the table, not the index'",
		"CREATE INDEX i_plain ON t (name)",
		"CREATE INDEX i_gin ON t USING gin (doc)",
		"CREATE INDEX i_opclass ON t (code text_pattern_ops)",
		"CREATE INDEX i_expr ON t (lower(name))",
		"CREATE INDEX i_desc ON t (created_at DESC NULLS LAST)",
		"CREATE INDEX i_nullsfirst ON t (score NULLS FIRST)",
		"CREATE INDEX i_include ON t (a, b) INCLUDE (c)",
		"CREATE INDEX i_opclass_params ON t USING gist (tsv tsvector_ops (siglen = 64))",
		// Two multi-key GiST indexes. The single-key fixture above cannot tell
		// a per-key correlation from a constant one: reading the FIRST index
		// attribute's attoptions for every key answers it, and every other
		// single-key row here, correctly.
		"CREATE INDEX i_opclass_params_multikey ON t USING gist (tsv2, tsv3 tsvector_ops (siglen = 64))",
		"CREATE INDEX i_opclass_params_perkey ON t USING gist " +
			"(tsv2 tsvector_ops (siglen = 32), tsv3 tsvector_ops (siglen = 64))",
		"CREATE INDEX i_comment ON t (name)",
		"COMMENT ON INDEX i_comment IS 'keep me'",
		"CREATE INDEX i_storage ON t USING brin (ts) WITH (pages_per_range = 32)",
		"CREATE INDEX i_storage_unrepresentable ON t (name) WITH (fillfactor = 70)",
	}
}

// readIndexAttributeIndexes provisions a database holding the whole fixture and
// returns every index the reader found in it. It asserts that its own
// arrangement succeeded and nothing else, so a failure inside it is the
// arrangement and never the attribute the caller is about to measure.
func readIndexAttributeIndexes(c *qt.C, dsn string) []dbschematypes.DBIndex {
	c.Helper()

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

	return live.Indexes
}

// The tests below are the enumeration #1242 asks for, read off a live server:
// one test per shape of claim the reader has to satisfy, and one row per index
// that carries it. Each test asserts exactly the attributes its name states, so
// a failure names the attribute that moved as well as the index it moved on.

// TestPostgreSQLIndexAttributes_PlainBtreeControlCarriesNothingExtra is the
// control. An index that varies in none of the attributes the other tests
// measure has to come back clean of all of them, so an attribute that leaks off
// the index it belongs to has somewhere to be seen.
func TestPostgreSQLIndexAttributes_PlainBtreeControlCarriesNothingExtra(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	index := findLiveIndex(c, readIndexAttributeIndexes(c, dsn), "i_plain")

	c.Assert(index.Method, qt.Equals, "btree")
	c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{{Name: "name"}})
	c.Assert(index.IncludeColumns, qt.IsNil)
	c.Assert(index.StorageParams, qt.IsNil)
	c.Assert(index.Comment, qt.Equals, "",
		qt.Commentf("the table's comment must not arrive on an index that has none"))
}

// TestPostgreSQLIndexAttributes_KeyVectorSurvivesTheRead covers the attributes
// that live inside a key rather than beside it. An operator class, an
// expression, a sort direction, a nulls ordering and a per-key operator class
// parameter are all fields of one DBIndexPart, so the key vector is the whole
// claim each row makes.
func TestPostgreSQLIndexAttributes_KeyVectorSurvivesTheRead(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	indexes := readIndexAttributeIndexes(c, dsn)

	tests := []struct {
		name  string
		index string
		// wantParts is the WHOLE Parts slice rather than one field of it, so an
		// attribute that leaks onto a key it does not belong to reddens the row
		// that owns the key as well as the row that owns the attribute.
		wantParts []dbschematypes.DBIndexPart
	}{
		{
			name:  "operator class",
			index: "i_opclass",
			wantParts: []dbschematypes.DBIndexPart{
				{Name: "code", Operator: "text_pattern_ops"},
			},
		},
		{
			name:  "expression key",
			index: "i_expr",
			wantParts: []dbschematypes.DBIndexPart{
				{Expr: "lower(name)"},
			},
		},
		{
			name:  "sort direction and nulls ordering",
			index: "i_desc",
			wantParts: []dbschematypes.DBIndexPart{
				{Name: "created_at", Desc: true, NullsOrder: dbschematypes.NullsOrderLast},
			},
		},
		{
			name:  "nulls first on an ascending key",
			index: "i_nullsfirst",
			wantParts: []dbschematypes.DBIndexPart{
				{Name: "score", NullsOrder: dbschematypes.NullsOrderFirst},
			},
		},
		{
			// Both keys are parameterised and the two values differ, so no
			// constant attribute number and no reordering of the keys reports
			// this row.
			name:  "each key keeps its own operator class parameters",
			index: "i_opclass_params_perkey",
			wantParts: []dbschematypes.DBIndexPart{
				{Name: "tsv2", Operator: "tsvector_ops(siglen=32)"},
				{Name: "tsv3", Operator: "tsvector_ops(siglen=64)"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index := findLiveIndex(c, indexes, test.index)

			c.Assert(index.Parts, qt.DeepEquals, test.wantParts)
		})
	}
}

// TestPostgreSQLIndexAttributes_AccessMethodAndKeyVectorSurviveTheRead pins the
// method beside the keys. An operator class parameter exists only under a
// method that defines one, so a report that names these keys without naming the
// method they were read under has not said what is in the catalog.
//
// An operator class can be the key type's DEFAULT and still carry parameters --
// measured here, tsvector_ops under gist with siglen=64 reports opcdefault true
// -- so a reader that names a class only when it is not the default drops the
// parameters with it, and the index it rebuilds has the 124-byte default
// signature. Neither loss is an error at replay time: psql accepts both at exit
// 0 and the wrong index is simply there.
func TestPostgreSQLIndexAttributes_AccessMethodAndKeyVectorSurviveTheRead(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	indexes := readIndexAttributeIndexes(c, dsn)

	tests := []struct {
		name       string
		index      string
		wantMethod string
		// wantParts is the WHOLE Parts slice rather than one field of it, so an
		// attribute that leaks onto a key it does not belong to reddens the row
		// that owns the key as well as the row that owns the attribute.
		wantParts []dbschematypes.DBIndexPart
	}{
		{
			name:       "access method",
			index:      "i_gin",
			wantMethod: "gin",
			wantParts:  []dbschematypes.DBIndexPart{{Name: "doc"}},
		},
		{
			// The parameters come from the INDEX relation's pg_attribute row,
			// not the table's.
			name:       "operator class parameters",
			index:      "i_opclass_params",
			wantMethod: "gist",
			wantParts: []dbschematypes.DBIndexPart{
				{Name: "tsv", Operator: "tsvector_ops(siglen=64)"},
			},
		},
		{
			// The parameters are on the SECOND key. Reading the first index
			// attribute's attoptions for every key still names attoptions and
			// still joins to the index relation, so it survives both the
			// presence check and the wrong-relation check -- and reports this
			// index as USING gist ("tsv2", "tsv3"), which psql accepts at exit
			// 0 and which gives the second key the 124-byte default signature.
			name:       "operator class parameters on a key that is not the first",
			index:      "i_opclass_params_multikey",
			wantMethod: "gist",
			wantParts: []dbschematypes.DBIndexPart{
				{Name: "tsv2"},
				{Name: "tsv3", Operator: "tsvector_ops(siglen=64)"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index := findLiveIndex(c, indexes, test.index)

			c.Assert(index.Method, qt.Equals, test.wantMethod)
			c.Assert(index.Parts, qt.DeepEquals, test.wantParts)
		})
	}
}

// TestPostgreSQLIndexAttributes_IncludePayloadSurvivesTheRead reads the payload
// and the keys of one index together: an INCLUDE column is not a key, and a
// reader that reports it as one rebuilds a different index.
func TestPostgreSQLIndexAttributes_IncludePayloadSurvivesTheRead(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	index := findLiveIndex(c, readIndexAttributeIndexes(c, dsn), "i_include")

	c.Assert(index.IncludeColumns, qt.DeepEquals, []string{"c"})
	c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{
		{Name: "a"}, {Name: "b"},
	})
}

// TestPostgreSQLIndexAttributes_CommentSurvivesTheRead measures the index's own
// object comment, which the pinned community binary v1.3.0 reads and Ptah
// dropped. The table carries a different comment, so this test fails rather
// than passing by coincidence if the query reaches the table relation.
func TestPostgreSQLIndexAttributes_CommentSurvivesTheRead(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	index := findLiveIndex(c, readIndexAttributeIndexes(c, dsn), "i_comment")

	c.Assert(index.Comment, qt.Equals, "keep me")
}

// TestPostgreSQLIndexAttributes_StorageParametersSurviveTheRead reads an
// index's WITH (...) parameters, one of the two members of this family the
// issue's own list did not reach.
func TestPostgreSQLIndexAttributes_StorageParametersSurviveTheRead(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	index := findLiveIndex(c, readIndexAttributeIndexes(c, dsn), "i_storage")

	c.Assert(index.Method, qt.Equals, "brin")
	c.Assert(index.StorageParams, qt.DeepEquals, map[string]string{
		"pages_per_range": "32",
	})
}

// TestPostgreSQLIndexAttributes_UnrepresentableStorageParameterIsDropped is the
// other side of the decision. fillfactor has no slot on any surface downstream,
// so recording it would make this index differ from its own inspected document
// on every run. Dropping it is the decision, and this test is where it is
// written down rather than merely happening.
func TestPostgreSQLIndexAttributes_UnrepresentableStorageParameterIsDropped(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	index := findLiveIndex(c, readIndexAttributeIndexes(c, dsn), "i_storage_unrepresentable")

	c.Assert(index.StorageParams, qt.IsNil)
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
			// The per-key spelling. It is asserted separately from the one
			// above because a converter that forwarded the first key's class
			// to every key would satisfy that one.
			c.Assert(document, qt.Contains, `ops = "tsvector_ops(siglen=32)"`)
			c.Assert(document, qt.Contains, "page_per_range = 32")
			c.Assert(document, qt.Contains, `comment = "keep me"`)

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
