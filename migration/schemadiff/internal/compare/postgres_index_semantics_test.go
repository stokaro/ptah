package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// The fixtures below are transcribed from live PostgreSQL 17.10 (container
// ptah1074-dev). Each pair was created with psql, introspected by both
// binaries, and diffed; the "want" column of every changed row is what the
// pinned community binary v1.3.0 planned for that pair, and the "want" of
// every control row is the "Schemas are synced, no changes to be made." it
// printed instead.
//
// Before issue #1272 the PostgreSQL branch of indexDefinitionsChanged returned
// false before comparing anything but the partial predicate and NULLS
// DISTINCT, so every changed row here reported "synced" -- including the plain
// key-column change, which nothing else in the suite covered either.

// postgresIndexTable is the table every fixture in this file indexes. It exists
// so a row can name a column without also having to describe a schema.
const postgresIndexTable = "t"

// databaseIndex is the introspected side of one fixture: one index named "i" on
// table "t", spelled the way internal/dbschema/postgres/reader.go reports it
// after #1246 and #1271 -- an access method from pg_am, one part per key with
// the operator class recorded only when it is not the type default, and the
// NULLS ordering recorded only when it deviates from the direction's default.
func databaseIndex(parts []types.DBIndexPart) types.DBIndex {
	return types.DBIndex{
		Name:      "i",
		TableName: postgresIndexTable,
		Method:    "btree",
		Parts:     parts,
		Columns:   databaseIndexColumns(parts),
	}
}

func databaseIndexColumns(parts []types.DBIndexPart) []string {
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		columns = append(columns, part.Name)
	}
	return columns
}

// generatedIndex is the desired side: one index named "i" on table "t" as a
// schema source describes it.
func generatedIndex(parts []goschema.IndexPart) goschema.Index {
	return goschema.Index{
		Name:      "i",
		TableName: postgresIndexTable,
		Parts:     parts,
		Fields:    generatedIndexFields(parts),
	}
}

func generatedIndexFields(parts []goschema.IndexPart) []string {
	fields := make([]string, 0, len(parts))
	for _, part := range parts {
		fields = append(fields, part.Name)
	}
	return fields
}

// TestPostgresIndexSemantics_AxisIsDetected walks the axes issue #1272 lists.
// Every row differs from its database side in exactly one semantic attribute,
// and every row must plan the same DROP INDEX + CREATE INDEX pair PostgreSQL
// requires: there is no ALTER INDEX form for any of these properties.
//
// These rows are the mutation detectors. Deleting the corresponding comparison
// from postgresIndexDefinitionChanged turns exactly the rows for that attribute
// red and leaves the controls green.
func TestPostgresIndexSemantics_AxisIsDetected(t *testing.T) {
	tests := []struct {
		name      string
		generated goschema.Index
		database  types.DBIndex
	}{
		{
			// CREATE INDEX i ON t USING btree (value)
			//   -> CREATE INDEX i ON t USING hash (value)
			name: "access method btree to hash",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "value"}})
				index.Type = "hash"
				return index
			}(),
			database: databaseIndex([]types.DBIndexPart{{Name: "value"}}),
		},
		{
			// CREATE INDEX i ON t USING gin (tsv)
			//   -> CREATE INDEX i ON t USING gist (tsv)
			name: "access method gin to gist",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "tsv"}})
				index.Type = "gist"
				return index
			}(),
			database: func() types.DBIndex {
				index := databaseIndex([]types.DBIndexPart{{Name: "tsv"}})
				index.Method = "gin"
				return index
			}(),
		},
		{
			// (value) -> (value text_pattern_ops)
			name:      "operator class added",
			generated: generatedIndex([]goschema.IndexPart{{Name: "value", Operator: "text_pattern_ops"}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "value"}}),
		},
		{
			// (value text_pattern_ops) -> (value)
			name:      "operator class removed",
			generated: generatedIndex([]goschema.IndexPart{{Name: "value"}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "value", Operator: "text_pattern_ops"}}),
		},
		{
			// USING gist (tsv tsvector_ops(siglen=64))
			//   -> USING gist (tsv tsvector_ops(siglen=32))
			//
			// The class name is identical on both sides and only its parameter
			// differs, so this is the row that separates comparing the operator
			// class from comparing the operator class name.
			name: "operator class parameter changed",
			generated: generatedIndex([]goschema.IndexPart{
				{Name: "tsv", Operator: "tsvector_ops(siglen=32)"},
			}),
			database: databaseIndex([]types.DBIndexPart{
				{Name: "tsv", Operator: "tsvector_ops(siglen=64)"},
			}),
		},
		{
			// WITH (pages_per_range = 32) -> WITH (pages_per_range = 8)
			name: "storage parameter value changed",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "ts"}})
				index.Type = "brin"
				index.StorageParams = map[string]string{"pages_per_range": "8"}
				return index
			}(),
			database: func() types.DBIndex {
				index := databaseIndex([]types.DBIndexPart{{Name: "ts"}})
				index.Method = "brin"
				index.StorageParams = map[string]string{"pages_per_range": "32"}
				return index
			}(),
		},
		{
			// USING brin (ts) -> USING brin (ts) WITH (pages_per_range = 32)
			name: "storage parameter added",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "ts"}})
				index.Type = "brin"
				index.StorageParams = map[string]string{"pages_per_range": "32"}
				return index
			}(),
			database: func() types.DBIndex {
				index := databaseIndex([]types.DBIndexPart{{Name: "ts"}})
				index.Method = "brin"
				return index
			}(),
		},
		{
			// WITH (pages_per_range = 32) -> USING brin (ts)
			name: "storage parameter removed",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "ts"}})
				index.Type = "brin"
				return index
			}(),
			database: func() types.DBIndex {
				index := databaseIndex([]types.DBIndexPart{{Name: "ts"}})
				index.Method = "brin"
				index.StorageParams = map[string]string{"pages_per_range": "32"}
				return index
			}(),
		},
		{
			// The index-level operator class an annotation may set once for
			// every key. The renderer applies it to each key that has none, so
			// the comparison has to resolve it the same way or a
			// `ops="gin_trgm_ops"` index would compare equal to a default one.
			name: "index level operator class added",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "value"}})
				index.Operator = "text_pattern_ops"
				return index
			}(),
			database: databaseIndex([]types.DBIndexPart{{Name: "value"}}),
		},
		{
			// (value) -> (value DESC)
			name:      "sort direction",
			generated: generatedIndex([]goschema.IndexPart{{Name: "value", Desc: true}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "value"}}),
		},
		{
			// (value NULLS FIRST) -> (value DESC).
			//
			// This row exists because the one above cannot prove the direction
			// comparison is load-bearing: `(value)` resolves to NULLS LAST and
			// `(value DESC)` to NULLS FIRST, so deleting the direction
			// comparison left it passing on the NULLS ordering alone. Here both
			// sides resolve to NULLS FIRST -- PostgreSQL 17.10 reports the
			// first as `btree (value NULLS FIRST)` and the second as
			// `btree (value DESC)` -- so direction is the only difference left,
			// and the pinned community binary v1.3.0 planned a rebuild for it.
			name:      "sort direction with the same resolved nulls ordering",
			generated: generatedIndex([]goschema.IndexPart{{Name: "value", Desc: true}}),
			database: databaseIndex([]types.DBIndexPart{
				{Name: "value", NullsOrder: types.NullsOrderFirst},
			}),
		},
		{
			// (value) -> (value NULLS FIRST): NULLS LAST is the ASC default,
			// so FIRST deviates and the reader records it.
			name:      "nulls first on an ascending key",
			generated: generatedIndex([]goschema.IndexPart{{Name: "value", NullsOrder: goschema.NullsOrderFirst}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "value"}}),
		},
		{
			// (value DESC) -> (value DESC NULLS LAST): NULLS FIRST is the DESC
			// default, so LAST deviates.
			name: "nulls last on a descending key",
			generated: generatedIndex([]goschema.IndexPart{
				{Name: "value", Desc: true, NullsOrder: goschema.NullsOrderLast},
			}),
			database: databaseIndex([]types.DBIndexPart{{Name: "value", Desc: true}}),
		},
		{
			// (a) INCLUDE (b) -> (a) INCLUDE (c)
			name: "include payload changed",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "a"}})
				index.IncludeColumns = []string{"c"}
				return index
			}(),
			database: func() types.DBIndex {
				index := databaseIndex([]types.DBIndexPart{{Name: "a"}})
				index.IncludeColumns = []string{"b"}
				return index
			}(),
		},
		{
			// (a) -> (a) INCLUDE (b)
			name: "include payload added",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "a"}})
				index.IncludeColumns = []string{"b"}
				return index
			}(),
			database: databaseIndex([]types.DBIndexPart{{Name: "a"}}),
		},
		{
			// (a) INCLUDE (b) -> (a)
			name:      "include payload removed",
			generated: generatedIndex([]goschema.IndexPart{{Name: "a"}}),
			database: func() types.DBIndex {
				index := databaseIndex([]types.DBIndexPart{{Name: "a"}})
				index.IncludeColumns = []string{"b"}
				return index
			}(),
		},
		{
			// INCLUDE (b, c) -> INCLUDE (c, b). PostgreSQL stores the payload
			// in the written order and the pinned binary planned a rebuild for
			// this pair, so the comparison is positional.
			name: "include payload reordered",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "a"}})
				index.IncludeColumns = []string{"c", "b"}
				return index
			}(),
			database: func() types.DBIndex {
				index := databaseIndex([]types.DBIndexPart{{Name: "a"}})
				index.IncludeColumns = []string{"b", "c"}
				return index
			}(),
		},
		{
			// (a) -> (lower(a)). The #1246 distinction: an expression key is
			// not a column key spelling the same text.
			name:      "column key becomes an expression",
			generated: generatedIndex([]goschema.IndexPart{{Expr: "lower(a)"}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "a"}}),
		},
		{
			// (lower(a)) -> (a)
			name:      "expression key becomes a column",
			generated: generatedIndex([]goschema.IndexPart{{Name: "a"}}),
			database:  databaseIndex([]types.DBIndexPart{{Expr: "lower(a)"}}),
		},
		{
			// (lower(a)) -> (upper(a))
			name:      "expression key changed",
			generated: generatedIndex([]goschema.IndexPart{{Expr: "upper(a)"}}),
			database:  databaseIndex([]types.DBIndexPart{{Expr: "lower(a)"}}),
		},
		{
			// An expression and a column that spell the same text stay
			// distinct. Collapsing them is what made the reader emit
			// CREATE INDEX ... ("lower(name)"), which psql rejects.
			name:      "expression and identically spelled column stay distinct",
			generated: generatedIndex([]goschema.IndexPart{{Expr: "lower(a)"}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "lower(a)"}}),
		},
		{
			// (value) -> UNIQUE (value)
			name: "uniqueness added",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "value"}})
				index.Unique = true
				return index
			}(),
			database: databaseIndex([]types.DBIndexPart{{Name: "value"}}),
		},
		{
			// (a) -> (b). The plainest change of all, and it reported "synced"
			// too: the PostgreSQL branch never reached the key comparison.
			name:      "key column changed",
			generated: generatedIndex([]goschema.IndexPart{{Name: "b"}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "a"}}),
		},
		{
			// (a) -> (a, b)
			name:      "key column added",
			generated: generatedIndex([]goschema.IndexPart{{Name: "a"}, {Name: "b"}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "a"}}),
		},
		{
			// The mixed multi-key fixture issue #1272 asks for, mutated one
			// position at a time. pg_index exposes keys, operator classes and
			// sort options as three parallel arrays, so a comparison that
			// matched keys by name instead of by position would miss an
			// operator class moving from the first key to the second.
			name: "mixed multi-key index, operator class moves to the second key",
			generated: generatedIndex([]goschema.IndexPart{
				{Name: "a", Desc: true, NullsOrder: goschema.NullsOrderLast},
				{Name: "b", Operator: "text_pattern_ops", NullsOrder: goschema.NullsOrderFirst},
			}),
			database: mixedDatabaseIndex(),
		},
		{
			name: "mixed multi-key index, key order swapped",
			generated: generatedIndex([]goschema.IndexPart{
				{Name: "a", Operator: "text_pattern_ops", NullsOrder: goschema.NullsOrderFirst},
				{Name: "b", Desc: true, NullsOrder: goschema.NullsOrderLast},
			}),
			database: mixedDatabaseIndex(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{Indexes: []goschema.Index{test.generated}}
			database := &types.DBSchema{Indexes: []types.DBIndex{test.database}}
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(generated, database, diff, platform.Postgres)

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
				{Name: "i", TableName: postgresIndexTable},
			})
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
				{Name: "i", TableName: postgresIndexTable},
			})
		})
	}
}

// mixedDatabaseIndex is the combined fixture:
//
//	CREATE INDEX i ON t USING btree (
//	    a text_pattern_ops DESC NULLS LAST,
//	    b ASC NULLS FIRST
//	) INCLUDE (c);
//
// as PostgreSQL 17.10 reports it.
func mixedDatabaseIndex() types.DBIndex {
	index := databaseIndex([]types.DBIndexPart{
		{Name: "a", Operator: "text_pattern_ops", Desc: true, NullsOrder: types.NullsOrderLast},
		{Name: "b", NullsOrder: types.NullsOrderFirst},
	})
	index.IncludeColumns = []string{"c"}
	return index
}

// TestPostgresIndexSemantics_EquivalentDefinitionsDoNotChurn carries the fix.
//
// Buying detection by rebuilding every index on every apply would be worse than
// the bug it fixes, so each row here is a pair the pinned community binary
// v1.3.0 reported as "Schemas are synced, no changes to be made." on live
// PostgreSQL 17.10 and Ptah must agree.
func TestPostgresIndexSemantics_EquivalentDefinitionsDoNotChurn(t *testing.T) {
	tests := []struct {
		name      string
		generated goschema.Index
		database  types.DBIndex
	}{
		{
			name:      "identical single-key index",
			generated: generatedIndex([]goschema.IndexPart{{Name: "value"}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "value"}}),
		},
		{
			name: "identical mixed multi-key index",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{
					{Name: "a", Operator: "text_pattern_ops", Desc: true, NullsOrder: goschema.NullsOrderLast},
					{Name: "b", NullsOrder: goschema.NullsOrderFirst},
				})
				index.IncludeColumns = []string{"c"}
				index.Type = "btree"
				return index
			}(),
			database: mixedDatabaseIndex(),
		},
		{
			// The control the storage-parameter rows above need: a comparison
			// that reported any difference at all would redden this, and the
			// index would be dropped and recreated on every single run.
			name: "identical storage parameters",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "ts"}})
				index.Type = "brin"
				index.StorageParams = map[string]string{"pages_per_range": "32"}
				return index
			}(),
			database: func() types.DBIndex {
				index := databaseIndex([]types.DBIndexPart{{Name: "ts"}})
				index.Method = "brin"
				index.StorageParams = map[string]string{"pages_per_range": "32"}
				return index
			}(),
		},
		{
			// Neither side carries a WITH clause, which is the overwhelmingly
			// common case and must stay free of it.
			name: "no storage parameters on either side",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "ts"}})
				index.StorageParams = map[string]string{}
				return index
			}(),
			database: databaseIndex([]types.DBIndexPart{{Name: "ts"}}),
		},
		{
			// The same class and the same parameter, differing only in case and
			// space, is the same operator class.
			name: "parameterised operator class differs only in case",
			generated: generatedIndex([]goschema.IndexPart{
				{Name: "tsv", Operator: "TSVector_Ops(siglen=64)"},
			}),
			database: databaseIndex([]types.DBIndexPart{
				{Name: "tsv", Operator: "tsvector_ops(siglen=64)"},
			}),
		},
		{
			// An annotation or HCL source spells the access method BTREE; the
			// reader reports pg_am.amname, which PostgreSQL spells btree.
			name: "access method differs only in case",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "value"}})
				index.Type = "BTREE"
				return index
			}(),
			database: databaseIndex([]types.DBIndexPart{{Name: "value"}}),
		},
		{
			// A source that says nothing about the access method means the
			// default one, which is what the reader reports.
			name:      "unspecified access method is the default one",
			generated: generatedIndex([]goschema.IndexPart{{Name: "value"}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "value"}}),
		},
		{
			// GIN and gin are the same access method.
			name: "non-default access method differs only in case",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "tsv"}})
				index.Type = "GIN"
				return index
			}(),
			database: func() types.DBIndex {
				index := databaseIndex([]types.DBIndexPart{{Name: "tsv"}})
				index.Method = "gin"
				return index
			}(),
		},
		{
			// ASC and ASC NULLS LAST are the same key: NULLS LAST is the
			// ascending default, so #1271's reader does not record it and a
			// source that spells it out must still compare equal.
			name:      "explicit nulls last on an ascending key is the default",
			generated: generatedIndex([]goschema.IndexPart{{Name: "value", NullsOrder: goschema.NullsOrderLast}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "value"}}),
		},
		{
			// DESC and DESC NULLS FIRST are the same key.
			name: "explicit nulls first on a descending key is the default",
			generated: generatedIndex([]goschema.IndexPart{
				{Name: "value", Desc: true, NullsOrder: goschema.NullsOrderFirst},
			}),
			database: databaseIndex([]types.DBIndexPart{{Name: "value", Desc: true}}),
		},
		{
			// The reverse direction: the database side spells the default and
			// the desired side omits it.
			name:      "database side spells the ascending default",
			generated: generatedIndex([]goschema.IndexPart{{Name: "value"}}),
			database: databaseIndex([]types.DBIndexPart{
				{Name: "value", NullsOrder: types.NullsOrderLast},
			}),
		},
		{
			// Operator classes are compared case-insensitively, like every
			// other identifier PostgreSQL folds.
			name:      "operator class differs only in case",
			generated: generatedIndex([]goschema.IndexPart{{Name: "value", Operator: "TEXT_PATTERN_OPS"}}),
			database:  databaseIndex([]types.DBIndexPart{{Name: "value", Operator: "text_pattern_ops"}}),
		},
		{
			// An index-level operator class and the same class written on the
			// key are the same index; the renderer emits one clause either way.
			name: "index level and per-key operator class agree",
			generated: func() goschema.Index {
				index := generatedIndex([]goschema.IndexPart{{Name: "value"}})
				index.Operator = "text_pattern_ops"
				return index
			}(),
			database: databaseIndex([]types.DBIndexPart{{Name: "value", Operator: "text_pattern_ops"}}),
		},
		{
			// Expression spelling is normalized the same way check-constraint
			// expressions are: whitespace and keyword case do not make a new
			// index.
			name:      "expression differs only in spacing and case",
			generated: generatedIndex([]goschema.IndexPart{{Expr: "LOWER( a )"}}),
			database:  databaseIndex([]types.DBIndexPart{{Expr: "lower(a)"}}),
		},
		{
			// A source that lists plain column names without structured parts
			// still matches an introspected index whose parts are all plain.
			name: "unstructured field list matches plain introspected parts",
			generated: goschema.Index{
				Name: "i", TableName: postgresIndexTable, Fields: []string{"a", "b"},
			},
			database: databaseIndex([]types.DBIndexPart{{Name: "a"}, {Name: "b"}}),
		},
		{
			// A reader that reported only Columns (no structured parts) still
			// matches a plain desired index, so the legacy shape does not churn.
			name:      "introspected index without structured parts",
			generated: generatedIndex([]goschema.IndexPart{{Name: "a"}, {Name: "b"}}),
			database: types.DBIndex{
				Name: "i", TableName: postgresIndexTable, Method: "btree",
				Columns: []string{"a", "b"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{Indexes: []goschema.Index{test.generated}}
			database := &types.DBSchema{Indexes: []types.DBIndex{test.database}}
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(generated, database, diff, platform.Postgres)

			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

// TestPostgresIndexSemantics_ChangeStaysInItsOwnSchema pins the fixture issue
// #1272 asks for outside the default schema: two tables named "t" in two
// schemas, each carrying an index named "i", with only the app-schema one
// changing access method.
//
// Measured against the same pair on PostgreSQL 17.10, the pinned community
// binary v1.3.0 planned exactly one rebuild, on "app"."i".
func TestPostgresIndexSemantics_ChangeStaysInItsOwnSchema(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "i", TableName: "public.t", Fields: []string{"value"}},
			{Name: "i", TableName: "app.t", Fields: []string{"value"}, Type: "hash"},
		},
	}
	database := &types.DBSchema{
		Indexes: []types.DBIndex{
			{
				Name: "i", TableName: "t", Schema: "public", Method: "btree",
				Columns: []string{"value"}, Parts: []types.DBIndexPart{{Name: "value"}},
			},
			{
				Name: "i", TableName: "t", Schema: "app", Method: "btree",
				Columns: []string{"value"}, Parts: []types.DBIndexPart{{Name: "value"}},
			},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.IndexesWithDialect(generated, database, diff, platform.Postgres)

	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "i", TableName: "app.t"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "i", TableName: "app.t"},
	})
}

// TestPostgresIndexSemantics_OtherDialectsAreUnaffected is the isolation
// control issue #1272 requires. The comparison lives in shared infrastructure,
// so every dialect routes through the same function; only PostgreSQL was
// measured, and a dialect whose catalog semantics have not been measured keeps
// the name-only comparison it had rather than getting guessed ones.
//
// Each row is the access-method fixture -- the axis with the widest reach,
// since goschema.Index.Type means something different on ClickHouse -- under a
// dialect that must not react to it.
func TestPostgresIndexSemantics_OtherDialectsAreUnaffected(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "clickhouse", dialect: platform.ClickHouse},
		{name: "cockroachdb", dialect: platform.CockroachDB},
		{name: "yugabytedb", dialect: platform.YugabyteDB},
		{name: "spanner", dialect: platform.Spanner},
		{name: "unset dialect", dialect: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{Indexes: []goschema.Index{
				func() goschema.Index {
					index := generatedIndex([]goschema.IndexPart{{Name: "value"}})
					index.Type = "hash"
					return index
				}(),
			}}
			database := &types.DBSchema{Indexes: []types.DBIndex{
				databaseIndex([]types.DBIndexPart{{Name: "value"}}),
			}}
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(generated, database, diff, test.dialect)

			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}
