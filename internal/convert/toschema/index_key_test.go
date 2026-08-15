package toschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/convert/toschema"
	"go.5x5.cz/ptah/internal/parser"
)

// parseOneIndex parses a document holding exactly one CREATE INDEX and converts
// it the way every SQL schema source does.
func parseOneIndex(tb testing.TB, sql string) goschema.Index {
	c := qt.New(tb)
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.Postgres)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	node, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	return toschema.ToIndex(node)
}

// TestToIndex_KeySuffixes pins the index key suffixes the SQL surface has to
// read back out of DDL that Ptah's own PostgreSQL renderer wrote.
//
// Before #1242 a key list element was one opaque string, and every element that
// was not a bare identifier became an EXPRESSION -- suffix and all. Measured on
// live PostgreSQL 17.10, that turned `schema diff --from <db> --to file://<the
// SQL inspect just wrote for that db>` into a DROP plus a CREATE that psql
// refuses with `syntax error`, so applying the plan left the table with no
// index at all.
func TestToIndex_KeySuffixes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sql    string
		parts  []goschema.IndexPart
		fields []string
	}{
		{
			name:   "a parameterised operator class keeps its key and its parameters",
			sql:    `CREATE INDEX "i" ON "t" USING gist ("tsv" tsvector_ops(siglen=64));`,
			parts:  []goschema.IndexPart{{Name: "tsv", Operator: "tsvector_ops(siglen=64)"}},
			fields: []string{"tsv"},
		},
		{
			name:   "operator class parameters are re-spelled the way the catalog reports them",
			sql:    `CREATE INDEX "i" ON "t" USING gist ("tsv" tsvector_ops (siglen = '64'));`,
			parts:  []goschema.IndexPart{{Name: "tsv", Operator: "tsvector_ops(siglen=64)"}},
			fields: []string{"tsv"},
		},
		{
			name:   "several operator class parameters keep the catalog's separator",
			sql:    `CREATE INDEX "i" ON "t" USING gist ("tsv" tsvector_ops (siglen = 64, other = 2));`,
			parts:  []goschema.IndexPart{{Name: "tsv", Operator: "tsvector_ops(siglen=64, other=2)"}},
			fields: []string{"tsv"},
		},
		{
			name:   "a bare operator class keeps its key",
			sql:    `CREATE INDEX "i" ON "t" ("code" text_pattern_ops);`,
			parts:  []goschema.IndexPart{{Name: "code", Operator: "text_pattern_ops"}},
			fields: []string{"code"},
		},
		{
			name:   "DESC and NULLS LAST are read off the key, not into it",
			sql:    `CREATE INDEX "i" ON "t" ("created_at" DESC NULLS LAST);`,
			parts:  []goschema.IndexPart{{Name: "created_at", Desc: true, NullsOrder: goschema.NullsOrderLast}},
			fields: []string{"created_at"},
		},
		{
			name:   "NULLS FIRST without a direction is read off the key",
			sql:    `CREATE INDEX "i" ON "t" ("score" NULLS FIRST);`,
			parts:  []goschema.IndexPart{{Name: "score", NullsOrder: goschema.NullsOrderFirst}},
			fields: []string{"score"},
		},
		{
			name:   "ASC is consumed, because the catalog reports an ascending key by saying nothing",
			sql:    `CREATE INDEX "i" ON "t" ("score" ASC);`,
			parts:  []goschema.IndexPart{{Name: "score"}},
			fields: []string{"score"},
		},
		{
			name: "every suffix at once, on the key that carries it",
			sql:  `CREATE INDEX "i" ON "t" ("a", "b" text_pattern_ops DESC NULLS FIRST, ("lower"("c")) text_pattern_ops);`,
			parts: []goschema.IndexPart{
				{Name: "a"},
				{Name: "b", Operator: "text_pattern_ops", Desc: true, NullsOrder: goschema.NullsOrderFirst},
				{Expr: `"lower"("c")`, Operator: "text_pattern_ops"},
			},
			fields: []string{"a", "b", `"lower"("c")`},
		},
		{
			name:   "a plain key list is left on the legacy path",
			sql:    `CREATE INDEX "i" ON "t" ("name");`,
			parts:  nil,
			fields: []string{"name"},
		},
		{
			name:   "a bare expression key list is left on the legacy path",
			sql:    `CREATE INDEX "i" ON "t" ((lower(name)));`,
			parts:  []goschema.IndexPart{{Expr: "(lower(name))"}},
			fields: []string{"(lower(name))"},
		},
		{
			name:   "a per-key COLLATE leaves the whole list alone rather than dropping it",
			sql:    `CREATE INDEX "i" ON "t" ("name" COLLATE "C" DESC);`,
			parts:  []goschema.IndexPart{{Expr: `"name" COLLATE "C" DESC`}},
			fields: []string{`"name" COLLATE "C" DESC`},
		},
		{
			name:   "a MySQL prefix length is not an operator class",
			sql:    "CREATE INDEX `i` ON `t` (`name`(10) DESC);",
			parts:  []goschema.IndexPart{{Expr: "`name`(10) DESC"}},
			fields: []string{"`name`(10) DESC"},
		},
		{
			name:   "a backtick-quoted key grows no operator class",
			sql:    "CREATE INDEX `i` ON `t` (`code` text_pattern_ops);",
			parts:  []goschema.IndexPart{{Expr: "`code` text_pattern_ops"}},
			fields: []string{"`code` text_pattern_ops"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			index := parseOneIndex(c.TB, tt.sql)
			c.Assert(index.Parts, qt.DeepEquals, tt.parts)
			c.Assert(index.Fields, qt.DeepEquals, tt.fields)
		})
	}
}

// TestToIndex_StructuredPartsKeepTheirNullsOrdering guards the other hop into
// the same field.
//
// A source that already hands this converter structured parts -- anything that
// builds an [ast.IndexNode] rather than going through the SQL frontend -- had
// its NULLS ordering dropped here, silently, while the direction it belongs to
// came through. A sort direction and its NULLS ordering are one property in two
// fields, and an index that differs from its own description on a property
// neither side can show is a rebuild the comparator plans forever.
func TestToIndex_StructuredPartsKeepTheirNullsOrdering(t *testing.T) {
	t.Parallel()
	c := qt.New(t)

	node := ast.NewIndex("i", "t").SetParts([]ast.IndexPart{
		{Name: "created_at", Desc: true, NullsOrder: ast.NullsOrderLast},
		{Name: "score", NullsOrder: ast.NullsOrderFirst},
	})

	c.Assert(toschema.ToIndex(node).Parts, qt.DeepEquals, []goschema.IndexPart{
		{Name: "created_at", Desc: true, NullsOrder: goschema.NullsOrderLast},
		{Name: "score", NullsOrder: goschema.NullsOrderFirst},
	})
}

// TestPostgresIndexDDLSurvivesItsOwnSQLSurface is the property the blocker was
// about, stated directly: what the PostgreSQL renderer writes for an index, the
// SQL surface reads back into a model the renderer writes identically.
//
// A regression here is not cosmetic. The second rendering IS the DDL a plan
// replays when a `.sql` document is the desired state, and the shapes below
// were all measured being planned as `(("tsv" tsvector_ops(siglen=64)))`, which
// PostgreSQL 17.10 refuses at exit 3.
func TestPostgresIndexDDLSurvivesItsOwnSQLSurface(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ddl  string
	}{
		{
			name: "parameterised default operator class",
			ddl:  `CREATE INDEX IF NOT EXISTS "i" ON "t" USING gist ("tsv" tsvector_ops(siglen=64));`,
		},
		{
			name: "non-default operator class",
			ddl:  `CREATE INDEX IF NOT EXISTS "i" ON "t" ("code" text_pattern_ops);`,
		},
		{
			name: "descending key with an explicit NULLS ordering",
			ddl:  `CREATE INDEX IF NOT EXISTS "i" ON "t" ("created_at" DESC NULLS LAST);`,
		},
		{
			name: "ascending key with an explicit NULLS ordering",
			ddl:  `CREATE INDEX IF NOT EXISTS "i" ON "t" ("score" NULLS FIRST);`,
		},
		{
			name: "plain key",
			ddl:  `CREATE INDEX IF NOT EXISTS "i" ON "t" ("name");`,
		},
		{
			name: "storage parameters",
			ddl:  `CREATE INDEX IF NOT EXISTS "i" ON "t" USING brin ("ts") WITH (pages_per_range='32');`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			index := parseOneIndex(c.TB, tt.ddl)
			rendered, err := renderer.RenderSQL(platform.Postgres, fromschema.FromIndex(index))
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, tt.ddl+"\n")
		})
	}
}
