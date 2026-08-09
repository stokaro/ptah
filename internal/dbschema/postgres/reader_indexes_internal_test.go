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
	// keyOpclasses is the JSON array of per-key operator classes, each an
	// object of pg_opclass.opcname, pg_opclass.opcdefault and the index
	// attribute's pg_attribute.attoptions.
	keyOpclasses string
	// keyOpclassesWithoutParams is what the same projection returns when it
	// does not read attoptions -- every params field empty. It is the answer a
	// real server gives a query that dropped that join, and it is what makes a
	// reader that stopped asking for the parameters visible here.
	keyOpclassesWithoutParams string
	// keyOptions is the JSON array of pg_index.indoption bitmasks.
	keyOptions string
	// includeColumns is the JSON array of INCLUDE payload column texts.
	includeColumns string
	// method is pg_am.amname.
	method string
	// storageParams is the JSON array of pg_class.reloptions entries.
	storageParams string
	// requiredExtensions is the JSON array of extensions the index resolves to
	// through the catalog, and requiredExtensionsFrom is the catalog column the
	// resolution goes through -- indclass for an operator class, relam for an
	// access method. The two arms are separated so a fixture answers only the
	// one it is about, and dropping either arm from the reader's SQL reddens the
	// fixture that rests on it.
	requiredExtensions     string
	requiredExtensionsFrom string
	predicate              string
	isPrimary              bool
	isUnique               bool
}

// opclassJSON spells one key's operator class the way the reader's
// json_build_object projection returns it.
func opclassJSON(name string, isDefault bool, params string) string {
	return fmt.Sprintf(`{"name": %q, "is_default": %t, "params": %q}`, name, isDefault, params)
}

// defaultOpclassJSON is a one-key list holding the key type's default class
// with no parameters -- what every fixture that is not about operator classes
// reports.
func defaultOpclassJSON() string {
	return "[" + opclassJSON("text_ops", true, "") + "]"
}

// plainCatalog is CREATE INDEX i_plain ON t (name): btree, default opclass,
// ascending, no payload, no storage parameters. It is the control every other
// fixture varies from.
func plainCatalog() pgIndexCatalog {
	return pgIndexCatalog{
		schemaName:                "public",
		tableName:                 "t",
		indexName:                 "i_plain",
		indexDef:                  "CREATE INDEX i_plain ON public.t USING btree (name)",
		keyTexts:                  `["name"]`,
		keyAttnums:                `[2]`,
		keyOpclasses:              defaultOpclassJSON(),
		keyOpclassesWithoutParams: defaultOpclassJSON(),
		keyOptions:                `[0]`,
		includeColumns:            `[]`,
		method:                    "btree",
		storageParams:             `[]`,
		requiredExtensions:        `[]`,
		requiredExtensionsFrom:    "indclass",
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
	catalog.keyOpclasses = "[" + opclassJSON("text_pattern_ops", false, "") + "]"
	catalog.keyOpclassesWithoutParams = catalog.keyOpclasses
	return catalog
}

// parameterisedOpclassCatalog is
// CREATE INDEX i_sig ON t USING gist (tsv tsvector_ops (siglen = 64)).
//
// Measured on PostgreSQL 17.10, that stores opcname tsvector_ops with
// opcdefault TRUE and the index attribute's attoptions {siglen=64}: the class
// is the type's default under gist, and its parameters are not. It is the
// fixture that separates "name a class only when it is not the default" from
// the rule the reader needs, and the one the whole suite was blind to before
// #1242 -- an index rebuilt without the parameters gets the 124-byte default
// signature, which psql accepts at exit 0.
func parameterisedOpclassCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.indexName = "i_sig"
	catalog.indexDef = "CREATE INDEX i_sig ON public.t USING gist (tsv tsvector_ops (siglen='64'))"
	catalog.keyTexts = `["tsv"]`
	catalog.method = "gist"
	catalog.keyOpclasses = "[" + opclassJSON("tsvector_ops", true, "siglen=64") + "]"
	catalog.keyOpclassesWithoutParams = "[" + opclassJSON("tsvector_ops", true, "") + "]"
	return catalog
}

// storageParamsCatalog is
// CREATE INDEX i_brin ON t USING brin (ts) WITH (pages_per_range = 32),
// whose pg_class.reloptions PostgreSQL 17.10 reports as {pages_per_range=32}.
func storageParamsCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.indexName = "i_brin"
	catalog.indexDef = "CREATE INDEX i_brin ON public.t USING brin (ts) WITH (pages_per_range='32')"
	catalog.keyTexts = `["ts"]`
	catalog.method = "brin"
	catalog.storageParams = `["pages_per_range=32"]`
	return catalog
}

// unrepresentableStorageParamsCatalog is the same index with fillfactor as
// well. fillfactor has no slot on the Atlas-compatible HCL surface -- neither
// Ptah's writer nor the pinned community binary v1.3.0 emits one -- so
// recording it would make the index differ from its own inspected document on
// every run. The reader keeps the parameter the chain can carry and drops the
// one it cannot, rather than keeping both or neither.
func unrepresentableStorageParamsCatalog() pgIndexCatalog {
	catalog := storageParamsCatalog()
	catalog.indexName = "i_brin_ff"
	catalog.storageParams = `["pages_per_range=32", "fillfactor=70", "autosummarize=on"]`
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
	catalog.keyOpclasses = "[" + opclassJSON("int4_ops", true, "") + ", " +
		opclassJSON("int4_ops", true, "") + "]"
	catalog.keyOpclassesWithoutParams = catalog.keyOpclasses
	catalog.keyOptions = `[0, 0]`
	catalog.includeColumns = `["c"]`
	return catalog
}

// implicitOpclassCatalog is CREATE INDEX t_gin ON t USING gin (n int4_ops) over
// an integer column with btree_gin installed. PostgreSQL stores it as
// USING gin (n) -- the class is the default for integer under gin, so it is not
// printed, and keyOpclasses reports the empty string exactly as it does for a
// plain btree key. The extension is therefore invisible in every text this row
// carries, and only pg_index.indclass answers for it (stokaro/ptah#1286).
func implicitOpclassCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.indexName = "t_gin"
	catalog.indexDef = "CREATE INDEX t_gin ON public.t USING gin (n)"
	catalog.keyTexts = `["n"]`
	catalog.method = "gin"
	catalog.requiredExtensions = `["btree_gin"]`
	return catalog
}

// coreOpclassCatalog is the control: the same gin index over a jsonb column,
// whose GIN operator class core supplies. Everything the reader can read as
// text is identical to implicitOpclassCatalog -- including `USING gin` -- and
// the catalog answer is the only difference, so a rule that matched the access
// method rather than the resolved class would keep an extension this index does
// not need.
func coreOpclassCatalog() pgIndexCatalog {
	catalog := implicitOpclassCatalog()
	catalog.indexName = "doc_body_gin"
	catalog.indexDef = "CREATE INDEX doc_body_gin ON public.t USING gin (body)"
	catalog.keyTexts = `["body"]`
	catalog.requiredExtensions = `[]`
	return catalog
}

// extensionAccessMethodCatalog is CREATE INDEX i_bloom ON t USING bloom (name),
// where the access method itself is an extension member. It reaches the same
// field through pg_class.relam rather than through pg_index.indclass, so it is
// the fixture that fails if that arm is dropped.
func extensionAccessMethodCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.indexName = "i_bloom"
	catalog.indexDef = "CREATE INDEX i_bloom ON public.t USING bloom (name)"
	catalog.method = "bloom"
	catalog.requiredExtensions = `["bloom"]`
	catalog.requiredExtensionsFrom = "relam"
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
	// An operator class's parameters live in pg_attribute.attoptions, not in
	// pg_opclass, so a projection that reads indclass and not attoptions gets
	// the classes back with no parameters on them -- which is what a reader
	// that dropped that join would see, and what it must not be able to hide.
	opclasses := catalog.keyOpclassesWithoutParams
	if projection, ok := selectListItem(query, "index_key_opclasses", "FROM pg_index"); ok &&
		strings.Contains(projection, "attoptions") {
		opclasses = catalog.keyOpclasses
	}

	answers := []struct {
		alias         string
		catalogColumn string
		value         string
	}{
		{"index_columns", "pg_get_indexdef", catalog.keyTexts},
		{"index_key_attnums", "indkey", catalog.keyAttnums},
		{"index_key_opclasses", "indclass", opclasses},
		{"index_key_options", "indoption", catalog.keyOptions},
		{"index_include_columns", "indkey", catalog.includeColumns},
		{"index_method", "amname", catalog.method},
		{"index_storage_params", "reloptions", catalog.storageParams},
		{"index_required_extensions", catalog.requiredExtensionsFrom, catalog.requiredExtensions},
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
			// The class is the key type's default, so a reader that stops at
			// opcdefault reports nothing at all -- and the index it rebuilds
			// has the 124-byte default signature instead of the 64-byte one.
			name:    "a default operator class with parameters is carried whole",
			catalog: parameterisedOpclassCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{
					{Name: "tsv", Operator: "tsvector_ops(siglen=64)"},
				})
			},
		},
		{
			name:    "storage parameters the chain can carry are kept",
			catalog: storageParamsCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.StorageParams, qt.DeepEquals, map[string]string{
					"pages_per_range": "32",
				})
			},
		},
		{
			name:    "storage parameters no surface can write are dropped",
			catalog: unrepresentableStorageParamsCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.StorageParams, qt.DeepEquals, map[string]string{
					"pages_per_range": "32",
				}, qt.Commentf("fillfactor and autosummarize have no slot downstream"))
			},
		},
		{
			name:    "an index with no WITH clause carries no storage parameters",
			catalog: plainCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.StorageParams, qt.IsNil)
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
			// The dependency #1286 is about: nothing in the index's own text
			// names btree_gin, so a reader that does not resolve indclass
			// against pg_depend reports an index that cannot be built.
			name:    "an implicit operator class reports the extension behind it",
			catalog: implicitOpclassCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.RequiresExtensions, qt.DeepEquals, []string{"btree_gin"})
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{{Name: "n"}},
					qt.Commentf("the class is the default, so it is not carried as a printed one"))
			},
		},
		{
			name:    "a core operator class reports no extension",
			catalog: coreOpclassCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.RequiresExtensions, qt.IsNil)
				c.Assert(index.Method, qt.Equals, "gin",
					qt.Commentf("the control has to keep the access method the other row has"))
			},
		},
		{
			name:    "an extension-supplied access method reports its extension",
			catalog: extensionAccessMethodCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.RequiresExtensions, qt.DeepEquals, []string{"bloom"})
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

// TestPostgresOperatorClassSpelling covers all four combinations of the two
// catalog facts that decide how a key's operator class is spelled, because the
// rule is an ordering and an ordering is only pinned by the row that separates
// it from the other one.
//
// Testing IsDefault before Params passes three of these four rows. The row it
// fails is the parameterised default class, which is the case that exists on
// every GiST index over tsvector with a signature length -- and the failure it
// produces is not an error but a quietly different index.
func TestPostgresOperatorClassSpelling(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		class postgresKeyOperatorClass
		want  string
	}{
		{
			name:  "a default class with no parameters needs no spelling",
			class: postgresKeyOperatorClass{Name: "text_ops", IsDefault: true},
			want:  "",
		},
		{
			name:  "a chosen class is named",
			class: postgresKeyOperatorClass{Name: "text_pattern_ops"},
			want:  "text_pattern_ops",
		},
		{
			name: "a default class with parameters is named for its parameters",
			class: postgresKeyOperatorClass{
				Name: "tsvector_ops", IsDefault: true, Params: "siglen=64",
			},
			want: "tsvector_ops(siglen=64)",
		},
		{
			name: "a chosen class with parameters carries both",
			class: postgresKeyOperatorClass{
				Name: "gist_trgm_ops", Params: "siglen=32",
			},
			want: "gist_trgm_ops(siglen=32)",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(postgresOperatorClassSpelling(test.class), qt.Equals, test.want)
		})
	}
}

// TestPostgresIndexStorageParams pins which pg_class.reloptions entries reach
// the model.
//
// The allowlist is the point. A reader that recorded every reloption would look
// more complete and be worse: `fillfactor` has no slot on any surface the model
// passes through, so an index carrying it would differ from its own inspected
// document on every run, and the rebuild that difference plans would drop the
// parameter it was meant to protect.
func TestPostgresIndexStorageParams(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name       string
		reloptions string
		want       map[string]string
	}{
		{
			name:       "no WITH clause",
			reloptions: `[]`,
			want:       nil,
		},
		{
			name:       "a parameter the whole chain can carry",
			reloptions: `["pages_per_range=32"]`,
			want:       map[string]string{"pages_per_range": "32"},
		},
		{
			name:       "a parameter no surface downstream can write",
			reloptions: `["fillfactor=70"]`,
			want:       nil,
		},
		{
			name:       "a mixture keeps only what survives",
			reloptions: `["fillfactor=70", "pages_per_range=8", "deduplicate_items=off"]`,
			want:       map[string]string{"pages_per_range": "8"},
		},
		{
			name:       "an entry with no value is not a parameter",
			reloptions: `["pages_per_range"]`,
			want:       nil,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			params, err := postgresIndexStorageParams(test.reloptions)
			c.Assert(err, qt.IsNil)
			c.Assert(params, qt.DeepEquals, test.want)
		})
	}
}
