package postgres

// White-box testing required: readIndexesForSchema is unexported, and the SQL
// it issues is the only place pg_index's per-key catalog vectors enter the
// process. Everything downstream -- parsePostgresIndexParts, the conversion to
// goschema, the renderer -- can be correct while the reader simply never asks
// the server for them, which is precisely the state this package was in before
// #1242. A test of the pure parsers alone cannot see that: each is handed the
// vector it is supposed to prove was fetched.
//
// The fake server below therefore answers each projection the way a real
// PostgreSQL would. A projection that reads a catalog column is answered from
// the fixture catalog; a projection that reads nothing -- a bare literal, which
// is the shape the query had before each of these attributes was added, and the
// shape a revert restores -- is answered with that literal. Reverting any of
// them makes these tests fail where reverting them leaves the rest of the suite
// green.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// pgIndexCatalog is one row of pg_index as PostgreSQL 17.10 reports it. Every
// field records a value measured from the fixture named in its constructor
// below, read back with the probe in the #1242 investigation.
type pgIndexCatalog struct {
	schemaName string
	tableName  string
	indexName  string
	// indexDef is pg_get_indexdef(i.oid).
	indexDef string
	// keyTexts is the JSON array of per-key texts from
	// pg_get_indexdef(i.oid, ordinality, true).
	keyTexts string
	// keyAttnums is the JSON array of pg_index.indkey attribute numbers. A 0
	// marks an expression key; every real column has a positive attnum.
	keyAttnums string
	// keyOpclasses is the JSON array of per-key pg_opclass.opcname values,
	// empty where opcdefault is true.
	keyOpclasses string
	// keyOptions is the JSON array of pg_index.indoption bitmasks.
	keyOptions string
	// includeColumns is the JSON array of INCLUDE payload column texts.
	includeColumns string
	// method is pg_am.amname.
	method    string
	predicate string
	isPrimary bool
	isUnique  bool
}

// plainCatalog is CREATE INDEX i_plain ON t (name): btree, default opclass,
// ascending, no payload. It is the control every other fixture varies from.
func plainCatalog() pgIndexCatalog {
	return pgIndexCatalog{
		schemaName:     "public",
		tableName:      "t",
		indexName:      "i_plain",
		indexDef:       "CREATE INDEX i_plain ON public.t USING btree (name)",
		keyTexts:       `["name"]`,
		keyAttnums:     `[2]`,
		keyOpclasses:   `[""]`,
		keyOptions:     `[0]`,
		includeColumns: `[]`,
		method:         "btree",
	}
}

// expressionCatalog is CREATE INDEX i_expr ON t (lower(name)): indkey {0}.
func expressionCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.indexName = "i_expr"
	catalog.indexDef = "CREATE INDEX i_expr ON public.t USING btree (lower(name))"
	catalog.keyTexts = `["lower(name)"]`
	catalog.keyAttnums = `[0]`
	return catalog
}

// columnNamedLikeACallCatalog separates the expression case from its only
// plausible alternative: a column literally named "lower(name)". The key text
// is byte-identical to expressionCatalog and the attnum vector is the sole
// difference, so a reader that does not fetch the vector cannot tell them
// apart.
func columnNamedLikeACallCatalog() pgIndexCatalog {
	catalog := expressionCatalog()
	catalog.indexName = "i_quoted"
	catalog.indexDef = `CREATE INDEX i_quoted ON public.t USING btree ("lower(name)")`
	catalog.keyAttnums = `[3]`
	return catalog
}

// gistOnPointCatalog is CREATE INDEX i_gist ON t USING gist (p) over a point
// column. Dropping the access method here is not a quiet degradation: point
// has no default btree operator class, so the emitted DDL does not replay.
func gistOnPointCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.indexName = "i_gist"
	catalog.indexDef = "CREATE INDEX i_gist ON public.t USING gist (p)"
	catalog.keyTexts = `["p"]`
	catalog.method = "gist"
	return catalog
}

// opclassCatalog is CREATE INDEX i_op ON t (name text_pattern_ops).
func opclassCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.indexName = "i_op"
	catalog.indexDef = "CREATE INDEX i_op ON public.t USING btree (name text_pattern_ops)"
	catalog.keyOpclasses = `["text_pattern_ops"]`
	return catalog
}

// includeCatalog is CREATE INDEX i_inc ON t (a, b) INCLUDE (c). indclass and
// indoption cover the two key columns only; the payload column is not in them.
func includeCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.indexName = "i_inc"
	catalog.indexDef = "CREATE INDEX i_inc ON public.t USING btree (a, b) INCLUDE (c)"
	catalog.keyTexts = `["a", "b"]`
	catalog.keyAttnums = `[2, 3]`
	catalog.keyOpclasses = `["", ""]`
	catalog.keyOptions = `[0, 0]`
	catalog.includeColumns = `["c"]`
	return catalog
}

// sortOrderCatalog builds a one-key fixture with the given pg_index.indoption
// bitmask. The four values were read off PostgreSQL 17.10: (a DESC) reports 3,
// (c DESC NULLS LAST) reports 1, (b NULLS FIRST) reports 2, plain ascending
// reports 0.
func sortOrderCatalog(option string) func() pgIndexCatalog {
	return func() pgIndexCatalog {
		catalog := plainCatalog()
		catalog.indexName = "i_sorted"
		catalog.keyOptions = "[" + option + "]"
		return catalog
	}
}

// serveIndexQuery answers the reader's index query. Every value is looked up by
// the catalog expression the query asks for rather than by the alias it
// assigns, which is what makes a projection that stopped reading the catalog
// visible here instead of silently answered.
func serveIndexQuery(catalog pgIndexCatalog, query string) (dbtest.QueryResult, error) {
	answers := []struct {
		alias         string
		catalogColumn string
		value         string
	}{
		{"index_columns", "pg_get_indexdef", catalog.keyTexts},
		{"index_key_attnums", "indkey", catalog.keyAttnums},
		{"index_key_opclasses", "indclass", catalog.keyOpclasses},
		{"index_key_options", "indoption", catalog.keyOptions},
		{"index_include_columns", "indkey", catalog.includeColumns},
		{"index_method", "amname", catalog.method},
	}

	values := []driver.Value{
		catalog.schemaName, catalog.tableName, catalog.indexName, catalog.indexDef,
	}
	columns := []string{"schemaname", "tablename", "indexname", "indexdef"}
	for _, answer := range answers {
		value, err := answerProjection(query, answer.alias, answer.catalogColumn, answer.value)
		if err != nil {
			return dbtest.QueryResult{}, err
		}
		columns = append(columns, answer.alias)
		values = append(values, value)
	}
	columns = append(columns, "predicate", "indisprimary", "indisunique")
	values = append(values, catalog.predicate, catalog.isPrimary, catalog.isUnique)

	return dbtest.QueryResult{Columns: columns, Rows: [][]driver.Value{values}}, nil
}

// answerProjection returns what a PostgreSQL server would hand back for the
// SELECT-list item the query aliases to alias.
//
// It returns catalogValue only when that projection actually reads
// catalogColumn. A projection that reads nothing from the catalog cannot
// produce a catalog value on a real server either, so it is answered with the
// empty JSON array -- the same thing `'[]' as index_key_attnums` returns.
//
// A query with no projection under that alias is a hard error rather than a
// missing value: the reader scans its columns positionally, and answering an
// alias the query never asked for would let the fake supply data the server
// never sent.
func answerProjection(query, alias, catalogColumn, catalogValue string) (string, error) {
	projection, ok := selectListItem(query, alias, "FROM pg_index")
	if !ok {
		return "", fmt.Errorf("query has no projection aliased %q:\n%s", alias, query)
	}
	if strings.Contains(projection, catalogColumn) {
		return catalogValue, nil
	}
	return "[]", nil
}

// selectListItem returns the SELECT-list expression the query aliases to alias,
// splitting on commas at parenthesis depth zero so a sub-select's own commas do
// not end an item.
//
// Comments are stripped first, and that is not tidiness. Each projection in the
// reader is introduced by a comment naming the catalog column it reads and
// saying why. Matching against the raw text would let that prose stand in for
// the column: a mutant that deletes the sub-select and leaves the comment above
// it would still look like it reads the catalog. Measured -- the first version
// of the domain guard next door made exactly this mistake and stayed green
// through the revert it existed to catch.
//
// Splitting on parenthesis depth also has to happen after stripping, or a
// parenthesis inside a comment would move the depth counter.
func selectListItem(query, alias string, fromMarker string) (string, bool) {
	selectList := stripSQLLineComments(query)
	if from := strings.LastIndex(selectList, fromMarker); from >= 0 {
		selectList = selectList[:from]
	}
	for _, item := range splitTopLevel(selectList) {
		trimmed := strings.TrimSpace(item)
		// The reader spells some aliases `as` and some `AS`; neither is the
		// thing under test.
		if strings.HasSuffix(strings.ToLower(trimmed), " as "+strings.ToLower(alias)) {
			return item, true
		}
	}
	return "", false
}

func stripSQLLineComments(query string) string {
	lines := strings.Split(query, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if comment := strings.Index(line, "--"); comment >= 0 {
			line = line[:comment]
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

func splitTopLevel(value string) []string {
	var items []string
	depth := 0
	start := 0
	for position, character := range value {
		switch character {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				items = append(items, value[start:position])
				start = position + 1
			}
		}
	}
	return append(items, value[start:])
}

func readIndexThroughFakeServer(t *testing.T, catalog pgIndexCatalog) (types.DBIndex, error) {
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		return serveIndexQuery(catalog, query)
	})
	indexes, err := NewPostgreSQLReader(db.SQL, "public").readIndexesForSchema("public")
	if err != nil {
		return types.DBIndex{}, err
	}
	if len(indexes) != 1 {
		return types.DBIndex{}, fmt.Errorf("expected exactly one index, got %d", len(indexes))
	}
	return indexes[0], nil
}

// TestReadIndexesForSchema_AsksTheCatalogForEveryKeyAttribute is the guard the
// #1242 expression fix was missing and the guard its four siblings need.
//
// Removing the pg_index.indkey projection from the reader's SQL -- measured on
// PostgreSQL 17.10 to put `schema diff` back to emitting
// CREATE INDEX "i_expr" ON "t" ("lower(name)"), which psql rejects at exit 3
// with `column "lower(name)" does not exist` -- left `go test ./...` at exit 0
// before this test existed. The same was true of every other projection here.
func TestReadIndexesForSchema_AsksTheCatalogForEveryKeyAttribute(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		catalog func() pgIndexCatalog
		// assert states what the reader must report once the fake server has
		// answered only the projections the query genuinely asks for.
		assert func(c *qt.C, index types.DBIndex)
	}{
		{
			name:    "plain ascending btree key carries no extras",
			catalog: plainCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Method, qt.Equals, "btree")
				c.Assert(index.IncludeColumns, qt.IsNil)
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{{Name: "name"}})
			},
		},
		{
			// attnum 0 is PostgreSQL's marker for an expression key. Losing it
			// makes the renderer quote the expression into a column reference
			// that does not exist.
			name:    "expression key is labelled an expression",
			catalog: expressionCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{{Expr: "lower(name)"}})
				// The legacy columns-only view is populated either way, which
				// is why the loss was invisible: only Parts separates the two.
				c.Assert(index.Columns, qt.DeepEquals, []string{"lower(name)"})
			},
		},
		{
			name:    "column named like a call stays a column",
			catalog: columnNamedLikeACallCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{{Name: "lower(name)"}})
				c.Assert(index.Columns, qt.DeepEquals, []string{"lower(name)"})
			},
		},
		{
			name:    "access method is carried",
			catalog: gistOnPointCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Method, qt.Equals, "gist")
			},
		},
		{
			name:    "non-default operator class is carried",
			catalog: opclassCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{
					{Name: "name", Operator: "text_pattern_ops"},
				})
			},
		},
		{
			name:    "include payload columns are carried and stay out of the keys",
			catalog: includeCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.IncludeColumns, qt.DeepEquals, []string{"c"})
				c.Assert(index.Columns, qt.DeepEquals, []string{"a", "b"})
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{
					{Name: "a"}, {Name: "b"},
				})
			},
		},
		{
			name:    "indoption 3 is DESC with its default NULLS FIRST",
			catalog: sortOrderCatalog("3"),
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{
					{Name: "name", Desc: true},
				})
			},
		},
		{
			name:    "indoption 1 is DESC NULLS LAST",
			catalog: sortOrderCatalog("1"),
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{
					{Name: "name", Desc: true, NullsOrder: types.NullsOrderLast},
				})
			},
		},
		{
			name:    "indoption 2 is ascending NULLS FIRST",
			catalog: sortOrderCatalog("2"),
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{
					{Name: "name", NullsOrder: types.NullsOrderFirst},
				})
			},
		},
		{
			name:    "indoption 0 is ascending with its default NULLS LAST",
			catalog: sortOrderCatalog("0"),
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{
					{Name: "name"},
				})
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			index, err := readIndexThroughFakeServer(t, test.catalog())
			c.Assert(err, qt.IsNil)
			test.assert(c, index)
		})
	}
}
