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
//
// Two projections need more than presence.
//
// The operator-class one: a class's PARAMETERS are not in pg_opclass, because
// PostgreSQL keeps them in the attoptions of the INDEX relation's own
// pg_attribute row for the key's position, so the projection has to reach a
// particular relation at a particular attribute number. Both halves of that
// reach can be wrong while the word attoptions is still in the query --
// `keyatt.attnum = 1::smallint` reports the first key's parameters for every
// key, `keyatt.attrelid = ix.indrelid` reports the table column's options
// instead of the index attribute's -- so the fake evaluates the join the
// projection wrote rather than asking whether a word appears in it. Evaluating
// it also means accepting every spelling the query's own joins prove equal; see
// [namesIndexRelation].
//
// The comment one: obj_description takes an object AND the catalog its
// description is filed under, and only the pair answers. See
// [answerCommentProjection].
//
// What the fake cannot prove is that the model matches PostgreSQL; the live
// guard in integration/gonative/index_attributes_postgres_test.go is what does
// that.

import (
	"database/sql/driver"
	"fmt"
	"strconv"
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
	// keyOpclasses is the index's per-key operator classes, one entry per key
	// in key order. It is kept as catalog facts rather than as the JSON the
	// reader receives so the fake can assemble the answer the projection's own
	// join earns; see [answerOpclassProjection].
	keyOpclasses []pgIndexKeyOpclass
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
	// comment is obj_description of the INDEX relation -- the object COMMENT
	// ON INDEX addresses -- and tableComment is obj_description of the TABLE
	// relation the index is on. Both are here because the index row already
	// joins both relations, so a projection can reach the wrong one and still
	// read obj_description; see [answerCommentProjection].
	comment      string
	tableComment string
	predicate    string
	isPrimary    bool
	isUnique     bool
	// partitionAttached is whether pg_inherits records this index relation as
	// attached to an index on a partitioned parent.
	partitionAttached bool
}

// pgIndexKeyOpclass is one key's operator class as three separate catalog
// facts rather than as the JSON the reader receives.
//
// They do not come from one catalog row, and that is the reason for the split.
// name and isDefault are pg_opclass columns reached through pg_index.indclass,
// which is a per-key vector; attoptions belongs to the INDEX relation's
// pg_attribute row at the key's position, which is a different relation reached
// by a different correlation. A fixture that carried the finished JSON could
// only say whether the reader asked for parameters at all, never whether it
// asked for the right key's.
type pgIndexKeyOpclass struct {
	// name is pg_opclass.opcname.
	name string
	// isDefault is pg_opclass.opcdefault.
	isDefault bool
	// attoptions is the index attribute's pg_attribute.attoptions at this key
	// position, empty for the overwhelmingly common class that takes none.
	attoptions string
}

// defaultOpclasses is a one-key list holding the key type's default class with
// no parameters -- what every fixture that is not about operator classes
// reports.
func defaultOpclasses() []pgIndexKeyOpclass {
	return []pgIndexKeyOpclass{{name: "text_ops", isDefault: true}}
}

// plainCatalog is CREATE INDEX i_plain ON t (name): btree, default opclass,
// ascending, no payload, no storage parameters. It is the control every other
// fixture varies from.
func plainCatalog() pgIndexCatalog {
	return pgIndexCatalog{
		schemaName:             "public",
		tableName:              "t",
		indexName:              "i_plain",
		indexDef:               "CREATE INDEX i_plain ON public.t USING btree (name)",
		keyTexts:               `["name"]`,
		keyAttnums:             `[2]`,
		keyOpclasses:           defaultOpclasses(),
		keyOptions:             `[0]`,
		includeColumns:         `[]`,
		method:                 "btree",
		storageParams:          `[]`,
		requiredExtensions:     `[]`,
		requiredExtensionsFrom: "indclass",
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
	catalog.keyOpclasses = []pgIndexKeyOpclass{{name: "text_pattern_ops"}}
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
	catalog.keyOpclasses = []pgIndexKeyOpclass{
		{name: "tsvector_ops", isDefault: true, attoptions: "siglen=64"},
	}
	return catalog
}

// multiKeyParameterisedOpclassCatalog is
// CREATE INDEX i_sig_multi ON t USING gist (a, b tsvector_ops (siglen = 64)),
// where the parameters are on a key that is NOT the first one.
//
// Measured on PostgreSQL 17.10 for exactly that statement: the index's own
// pg_attribute rows report no attoptions for attnum 1 and {siglen=64} for
// attnum 2, and the server prints
// USING gist (a, b tsvector_ops (siglen='64')).
//
// This fixture exists because every other operator-class fixture in this file
// has one key, and one key cannot tell a per-key correlation from a constant.
// Reading `keyatt.attnum = 1::smallint` -- which still names attoptions and
// still joins to the index relation, so neither the presence check nor a join
// to the wrong relation describes it -- answers every single-key fixture here
// correctly and reports NO parameters at all for this one. The index it then
// rebuilds is USING gist ("a", "b"), which psql accepts at exit 0 and which
// gives the second key the 124-byte default signature.
func multiKeyParameterisedOpclassCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.indexName = "i_sig_multi"
	catalog.indexDef = "CREATE INDEX i_sig_multi ON public.t USING gist (a, b tsvector_ops (siglen='64'))"
	catalog.keyTexts = `["a", "b"]`
	catalog.keyAttnums = `[2, 3]`
	catalog.keyOptions = `[0, 0]`
	catalog.method = "gist"
	catalog.keyOpclasses = []pgIndexKeyOpclass{
		{name: "tsvector_ops", isDefault: true},
		{name: "tsvector_ops", isDefault: true, attoptions: "siglen=64"},
	}
	return catalog
}

// perKeyParameterisedOpclassCatalog is
// CREATE INDEX i_sig_perkey ON t USING gist
// (a tsvector_ops (siglen = 32), b tsvector_ops (siglen = 64)),
// measured on PostgreSQL 17.10 to store {siglen=32} on attnum 1 and
// {siglen=64} on attnum 2.
//
// Both keys carry parameters and the two differ, which is what separates the
// correlation from every constant at once: attnum 1 puts siglen=32 on both
// keys, attnum 2 puts siglen=64 on both, and swapping the two keys reports each
// key the other's signature length. Its sibling above separates the correlation
// from "no parameters at all"; neither row alone pins both.
func perKeyParameterisedOpclassCatalog() pgIndexCatalog {
	catalog := multiKeyParameterisedOpclassCatalog()
	catalog.indexName = "i_sig_perkey"
	catalog.indexDef = "CREATE INDEX i_sig_perkey ON public.t USING gist " +
		"(a tsvector_ops (siglen='32'), b tsvector_ops (siglen='64'))"
	catalog.keyOpclasses = []pgIndexKeyOpclass{
		{name: "tsvector_ops", isDefault: true, attoptions: "siglen=32"},
		{name: "tsvector_ops", isDefault: true, attoptions: "siglen=64"},
	}
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

// commentedCatalog is CREATE INDEX i_note ON t (name) followed by
// COMMENT ON INDEX i_note IS 'keep me', on a table that carries a comment of
// its own.
//
// The comment hangs off the INDEX relation, so it is reachable only through
// obj_description of that relation; nothing in the index's definition text
// mentions it, which is why a reader that never asked for it lost it in
// silence. Measured on PostgreSQL 17.10, the pinned Atlas community binary
// v1.3.0 reads it and plans COMMENT ON INDEX for it, so this is one of the few
// index attributes where the community binary was ahead. See #1242.
//
// The table's comment is set, and set to something else, because the index row
// joins the table as well: obj_description(t.oid, 'pg_class') is the same
// function on the same catalog reaching the wrong object, and a fixture whose
// table had no comment would report the empty string for it and let that pass.
func commentedCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.indexName = "i_note"
	catalog.indexDef = "CREATE INDEX i_note ON public.t USING btree (name)"
	catalog.comment = "keep me"
	catalog.tableComment = "the table, not the index"
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
	catalog.keyOpclasses = []pgIndexKeyOpclass{
		{name: "int4_ops", isDefault: true},
		{name: "int4_ops", isDefault: true},
	}
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

// partitionAttachedCatalog is the copy PostgreSQL creates on a partition when
// an index is created on its partitioned parent: an ordinary relkind 'i' index
// that pg_inherits records as attached to the parent's index.
func partitionAttachedCatalog() pgIndexCatalog {
	catalog := plainCatalog()
	catalog.tableName = "events_2026"
	catalog.indexName = "events_2026_tenant_idx"
	catalog.indexDef = "CREATE INDEX events_2026_tenant_idx ON public.events_2026 USING btree (tenant)"
	catalog.keyTexts = `["tenant"]`
	catalog.partitionAttached = true
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
	opclasses, err := answerOpclassProjection(catalog, query)
	if err != nil {
		return dbtest.QueryResult{}, err
	}
	comment, err := answerCommentProjection(catalog, query)
	if err != nil {
		return dbtest.QueryResult{}, err
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
		{"index_comment", "obj_description", comment},
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
	attached, err := answerPartitionAttached(query, catalog.partitionAttached)
	if err != nil {
		return dbtest.QueryResult{}, err
	}
	columns = append(columns, "predicate", "indisprimary", "indisunique", "partition_attached")
	values = append(values, catalog.predicate, catalog.isPrimary, catalog.isUnique, attached)

	return dbtest.QueryResult{Columns: columns, Rows: [][]driver.Value{values}}, nil
}

// answerPartitionAttached answers the partition_attached projection the way a
// server would: with the catalog's answer when the projection reads
// pg_inherits, and with false when it reads something else.
//
// pg_inherits is where the attachment of a partition's index copy to its parent
// index is recorded, and it is the only place. A projection reading relkind
// instead is not a weaker version of this question, it is a different one --
// relkind marks the parent, which is the one index of the pair a DROP INDEX may
// name -- so a mutant that swaps them gets the answer a server would give it,
// false, rather than the catalog's.
func answerPartitionAttached(query string, attached bool) (driver.Value, error) {
	projection, ok := selectListItem(query, "partition_attached", "FROM pg_index")
	if !ok {
		return nil, fmt.Errorf("query has no projection aliased %q:\n%s", "partition_attached", query)
	}
	return attached && strings.Contains(projection, "pg_inherits"), nil
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

// answerOpclassProjection assembles the JSON the index_key_opclasses projection
// returns, by evaluating the joins the projection wrote.
//
// pg_opclass answers a key's class name and whether it is the key type's
// default. It does not answer the class's PARAMETERS: PostgreSQL keeps those in
// the attoptions of the INDEX relation's pg_attribute row for that key's
// position. Reaching them takes a relation and an attribute number, and a query
// can be wrong about either while still mentioning attoptions -- which is why
// this is evaluated rather than pattern-matched. See [attoptionsAsProjected].
func answerOpclassProjection(catalog pgIndexCatalog, query string) (string, error) {
	projection, ok := selectListItem(query, "index_key_opclasses", "FROM pg_index")
	if !ok {
		return "", fmt.Errorf("query has no projection aliased %q:\n%s", "index_key_opclasses", query)
	}
	entries := make([]string, 0, len(catalog.keyOpclasses))
	for position, class := range catalog.keyOpclasses {
		params, err := attoptionsAsProjected(catalog, query, projection, position+1)
		if err != nil {
			return "", err
		}
		entries = append(entries, fmt.Sprintf(
			`{"name": %q, "is_default": %t, "params": %q}`,
			class.name, class.isDefault, params,
		))
	}
	return "[" + strings.Join(entries, ", ") + "]", nil
}

// answerCommentProjection returns the comment the index_comment projection asks
// for, decided by BOTH arguments it passes to obj_description: which object,
// and which catalog that object's description is filed under.
//
// The index row joins pg_class twice -- once as the index, once as the table --
// so obj_description(t.oid, 'pg_class') is the same function on the same
// catalog reaching a different object, and it reads a comment.
//
// The second argument is load-bearing in its own right, and it is the cheaper
// wrong implementation of the two: obj_description(i.oid, 'pg_index') is the
// same length, names the catalog the row plainly comes from, and returns NULL,
// because a comment on any relation -- an index included -- is filed in
// pg_description under classoid pg_class, never pg_index. A fake that read only
// the object would answer it with the index's comment and let the whole unit
// suite pass at exit 0 while the comment was silently dropped on every real
// server.
func answerCommentProjection(catalog pgIndexCatalog, query string) (string, error) {
	projection, ok := selectListItem(query, "index_comment", "FROM pg_index")
	if !ok {
		return "", fmt.Errorf("query has no projection aliased %q:\n%s", "index_comment", query)
	}
	object, descriptionCatalog, called := objDescriptionArguments(projection)
	if !called {
		return "", nil
	}
	// Any other catalog name finds no pg_description row and returns NULL,
	// which the projection's COALESCE turns into the empty string. That is a
	// real server's answer, not this fake declining to model something.
	if descriptionCatalog != "'pg_class'" {
		return "", nil
	}
	switch object {
	case "i.oid":
		return catalog.comment, nil
	case "t.oid":
		return catalog.tableComment, nil
	default:
		return "", fmt.Errorf(
			"the index_comment projection describes an object this fake does not model:\n%s",
			projection,
		)
	}
}

// objDescriptionArguments returns the two arguments of the projection's
// obj_description call, and whether it calls it at all.
func objDescriptionArguments(projection string) (object, descriptionCatalog string, ok bool) {
	_, rest, called := strings.Cut(projection, "obj_description(")
	if !called {
		return "", "", false
	}
	arguments, _, closed := strings.Cut(rest, ")")
	if !closed {
		return "", "", false
	}
	fields := splitTopLevel(arguments)
	if len(fields) != 2 {
		return "", "", false
	}
	return strings.TrimSpace(fields[0]), strings.TrimSpace(fields[1]), true
}

// attoptionsAsProjected returns the attoptions the projection's own join lands
// on for the key at this ordinality.
//
// Empty is not a fallback here, it is what a real server returns for each of
// the three ways the join can miss:
//
//   - no join to pg_attribute at all, which is the shape the query had before
//     #1242 and the shape a revert restores;
//   - a join to ix.indrelid, which lands on the TABLE's column rather than the
//     index's attribute. Table columns carry attoptions only where someone set
//     a per-column planner option, which no fixture here and no live fixture in
//     integration/gonative/index_attributes_postgres_test.go does;
//   - a correlation that names an attribute number the index does not have,
//     which a LEFT JOIN answers with NULL and the projection's COALESCE turns
//     into the empty string.
func attoptionsAsProjected(catalog pgIndexCatalog, query, projection string, ordinality int) (string, error) {
	relation, joined := joinOperand(projection, "keyatt.attrelid")
	if !joined || !namesIndexRelation(query, relation) {
		return "", nil
	}
	correlation, correlated := joinOperand(projection, "keyatt.attnum")
	if !correlated {
		return "", fmt.Errorf(
			"the index_key_opclasses projection joins pg_attribute without correlating attnum:\n%s",
			projection,
		)
	}
	attnum, err := resolveProjectedAttnum(correlation, ordinality)
	if err != nil {
		return "", err
	}
	if attnum < 1 || attnum > len(catalog.keyOpclasses) {
		return "", nil
	}
	return catalog.keyOpclasses[attnum-1].attoptions, nil
}

// namesIndexRelation reports whether an expression is the index relation's OID,
// as the query's OWN joins prove it rather than as one preferred spelling.
//
// `keyatt.attrelid = i.oid` and `keyatt.attrelid = ix.indexrelid` are the same
// join, because the query itself says `JOIN pg_class i ON i.oid =
// ix.indexrelid`. Comparing the operand to one fixed string made the first of
// those a red test and the second a green one for a difference no server can
// observe, which is a guard blocking a correct refactor rather than a defect.
// Equality is therefore resolved through the outer FROM clause: every `a = b`
// there is an edge, and any expression in the connected component of
// `ix.indexrelid` names the index relation.
//
// It stays exactly as discriminating as before on the mutants it exists for.
// `ix.indrelid` and `t.oid` are each other's component and are not connected to
// `ix.indexrelid` by any join in this query, so both still read the TABLE's
// attribute and still report no parameters.
func namesIndexRelation(query, expression string) bool {
	from := stripSQLLineComments(query)
	if marker := strings.LastIndex(from, "FROM pg_index"); marker >= 0 {
		from = from[marker:]
	}

	equal := map[string][]string{}
	for line := range strings.SplitSeq(from, "\n") {
		left, right, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		leftFields, rightFields := strings.Fields(left), strings.Fields(right)
		if len(leftFields) == 0 || len(rightFields) == 0 {
			continue
		}
		a, b := leftFields[len(leftFields)-1], rightFields[0]
		equal[a] = append(equal[a], b)
		equal[b] = append(equal[b], a)
	}

	seen := map[string]bool{"ix.indexrelid": true}
	frontier := []string{"ix.indexrelid"}
	for len(frontier) > 0 {
		current := frontier[len(frontier)-1]
		frontier = frontier[:len(frontier)-1]
		if current == expression {
			return true
		}
		for _, next := range equal[current] {
			if seen[next] {
				continue
			}
			seen[next] = true
			frontier = append(frontier, next)
		}
	}
	return false
}

// joinOperand returns the expression the projection equates with the named
// column, read to the end of that line. Comments are already gone by the time a
// projection reaches here; see [selectListItem].
func joinOperand(projection, column string) (string, bool) {
	_, rest, named := strings.Cut(projection, column)
	if !named {
		return "", false
	}
	if line, _, multiline := strings.Cut(rest, "\n"); multiline {
		rest = line
	}
	operand, equated := strings.CutPrefix(strings.TrimSpace(rest), "=")
	if !equated {
		return "", false
	}
	return strings.TrimSpace(operand), true
}

// resolveProjectedAttnum evaluates what the projection correlates
// pg_attribute.attnum with, for one key position.
//
// Two shapes are answerable and they are the two that matter: the ordinality of
// the key being read, which is the correlation an index attribute needs, and a
// constant, which is the cheaper wrong implementation that reports one key's
// parameters for every key. Anything else is an error rather than a guess,
// because a fake that guessed would be answering a query no server answers that
// way, and the reader would look pinned when it is not.
func resolveProjectedAttnum(correlation string, ordinality int) (int, error) {
	expression := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(correlation), "::smallint"))
	if expression == "keys.ordinality" {
		return ordinality, nil
	}
	constant, err := strconv.Atoi(expression)
	if err != nil {
		return 0, fmt.Errorf("cannot answer an attnum correlated with %q", correlation)
	}
	return constant, nil
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
				c.Assert(index.Comment, qt.Equals, "")
			},
		},
		{
			// Nothing in the index's definition text carries it, so a reader
			// that does not ask obj_description reports an index with no
			// comment and the object's comment is gone at exit 0.
			name:    "an index comment is carried",
			catalog: commentedCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Comment, qt.Equals, "keep me")
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
			// One key cannot tell a per-key correlation from a constant, and
			// every operator-class row above has one key. Reading the first
			// attribute's options for every key answers all of them and drops
			// the parameters here.
			name:    "parameters land on the key that carries them",
			catalog: multiKeyParameterisedOpclassCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{
					{Name: "a"},
					{Name: "b", Operator: "tsvector_ops(siglen=64)"},
				})
			},
		},
		{
			// Both keys are parameterised and the two differ, so no constant
			// attribute number and no reordering of the keys reports this row.
			name:    "each key keeps its own parameters",
			catalog: perKeyParameterisedOpclassCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.Parts, qt.DeepEquals, []types.DBIndexPart{
					{Name: "a", Operator: "tsvector_ops(siglen=32)"},
					{Name: "b", Operator: "tsvector_ops(siglen=64)"},
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
		{
			// The attachment of a partition's index copy to its parent index
			// lives in pg_inherits and nowhere else, so a projection that reads
			// anything else answers false here exactly as a server would.
			name:    "a partition's copy of a parent index is reported as attached",
			catalog: partitionAttachedCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.PartitionAttached, qt.IsTrue)
			},
		},
		{
			name:    "an ordinary index is not reported as attached",
			catalog: plainCatalog,
			assert: func(c *qt.C, index types.DBIndex) {
				c.Assert(index.PartitionAttached, qt.IsFalse)
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

// indexQueryForFake returns the SQL the reader actually issues, captured
// through the fake server, so the tests below reason about the real query
// rather than about a copy of it that can drift away from the reader.
func indexQueryForFake(t *testing.T) string {
	t.Helper()
	captured := ""
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		captured = query
		return serveIndexQuery(plainCatalog(), query)
	})
	_, err := NewPostgreSQLReader(db.SQL, "public").readIndexesForSchema("public")
	qt.Assert(t, err, qt.IsNil)
	return captured
}

// TestNamesIndexRelation states the equivalence the fake evaluates instead of
// pinning one spelling.
//
// `keyatt.attrelid = i.oid` is the same join as `keyatt.attrelid =
// ix.indexrelid` -- the query says so itself, in `JOIN pg_class i ON i.oid =
// ix.indexrelid` -- and a rewrite between them is correct against a real
// server. Answering "no parameters" for one of the two would fail a refactor
// over a difference no server can observe. The rows below are the whole point
// of the pair: both spellings of the INDEX relation are accepted, and both
// spellings of the TABLE relation are still refused, which is what keeps the
// wrong-relation mutant red.
func TestNamesIndexRelation(t *testing.T) {
	c := qt.New(t)
	query := indexQueryForFake(t)

	tests := []struct {
		name       string
		expression string
		want       bool
	}{
		{
			name:       "the index relation as pg_index spells it",
			expression: "ix.indexrelid",
			want:       true,
		},
		{
			name:       "the index relation as the joined pg_class row spells it",
			expression: "i.oid",
			want:       true,
		},
		{
			name:       "the table relation as pg_index spells it",
			expression: "ix.indrelid",
			want:       false,
		},
		{
			name:       "the table relation as the joined pg_class row spells it",
			expression: "t.oid",
			want:       false,
		},
		{
			name:       "a relation the query joins for something else entirely",
			expression: "am.oid",
			want:       false,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(namesIndexRelation(query, test.expression), qt.Equals, test.want)
		})
	}
}

// TestAnswerCommentProjectionReadsTheCatalogArgument pins the second argument
// of obj_description in the unit layer.
//
// A relation's comment is filed in pg_description under classoid pg_class, and
// an index is a relation. obj_description(i.oid, 'pg_index') is therefore the
// cheaper wrong implementation with everything going for it -- the same length,
// and it names the catalog the row visibly comes from -- and it returns NULL.
// Until this fake read that argument it answered such a projection with the
// index's comment, so the whole unit suite passed at exit 0 while every real
// server dropped the comment; only the live guard caught it, and the live guard
// runs only in integration-tests.
func TestAnswerCommentProjectionReadsTheCatalogArgument(t *testing.T) {
	c := qt.New(t)

	catalog := commentedCatalog()

	tests := []struct {
		name       string
		projection string
		want       string
	}{
		{
			name:       "the index relation, filed under pg_class",
			projection: `COALESCE(obj_description(i.oid, 'pg_class'), '') as index_comment`,
			want:       catalog.comment,
		},
		{
			name:       "the table relation, filed under pg_class",
			projection: `COALESCE(obj_description(t.oid, 'pg_class'), '') as index_comment`,
			want:       catalog.tableComment,
		},
		{
			name:       "the index relation, filed under a catalog that holds no descriptions",
			projection: `COALESCE(obj_description(i.oid, 'pg_index'), '') as index_comment`,
			want:       "",
		},
		{
			name:       "a projection that asks for no description at all",
			projection: `'' as index_comment`,
			want:       "",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			query := "SELECT\n\t" + test.projection + "\nFROM pg_index ix"
			got, err := answerCommentProjection(catalog, query)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
