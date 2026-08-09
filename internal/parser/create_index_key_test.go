package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/parser"
)

// parseOneIndex parses a document holding exactly one CREATE INDEX and returns
// the node.
func parseOneIndex(c *qt.C, dialect, sql string) *ast.IndexNode {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	index, ok := statements.Statements[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	return index
}

// TestParseCreateIndexKeySuffixes pins the index key suffixes the SQL surface
// has to read back out of DDL that Ptah's own PostgreSQL renderer wrote.
//
// Before #1242 the key list was one opaque string per element, so every suffix
// -- operator class, operator class parameters, DESC, NULLS FIRST/LAST -- came
// back glued to the column name and was classified as an expression. Measured
// on live PostgreSQL 17.10, that turned `schema diff --from <db> --to
// file://<the SQL inspect just wrote for that db>` into a DROP plus a CREATE
// that psql refuses with `syntax error`, so applying the plan left the table
// with no index at all.
func TestParseCreateIndexKeySuffixes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		sql   string
		parts []ast.IndexPart
	}{
		{
			name:  "a parameterised operator class keeps its key and its parameters",
			sql:   `CREATE INDEX "i" ON "t" USING gist ("tsv" tsvector_ops(siglen=64));`,
			parts: []ast.IndexPart{{Name: `"tsv"`, Operator: "tsvector_ops(siglen=64)"}},
		},
		{
			name:  "operator class parameters are re-spelled the way the catalog reports them",
			sql:   `CREATE INDEX "i" ON "t" USING gist ("tsv" tsvector_ops (siglen = '64'));`,
			parts: []ast.IndexPart{{Name: `"tsv"`, Operator: "tsvector_ops(siglen=64)"}},
		},
		{
			name: "several operator class parameters keep the catalog's separator",
			sql:  `CREATE INDEX "i" ON "t" USING gist ("tsv" tsvector_ops (siglen = 64, other = 2));`,
			parts: []ast.IndexPart{
				{Name: `"tsv"`, Operator: "tsvector_ops(siglen=64, other=2)"},
			},
		},
		{
			name:  "a bare operator class keeps its key",
			sql:   `CREATE INDEX "i" ON "t" ("code" text_pattern_ops);`,
			parts: []ast.IndexPart{{Name: `"code"`, Operator: "text_pattern_ops"}},
		},
		{
			name:  "DESC and NULLS LAST are read off the key, not into it",
			sql:   `CREATE INDEX "i" ON "t" ("created_at" DESC NULLS LAST);`,
			parts: []ast.IndexPart{{Name: `"created_at"`, Desc: true, NullsOrder: ast.NullsOrderLast}},
		},
		{
			name:  "NULLS FIRST without a direction is read off the key",
			sql:   `CREATE INDEX "i" ON "t" ("score" NULLS FIRST);`,
			parts: []ast.IndexPart{{Name: `"score"`, NullsOrder: ast.NullsOrderFirst}},
		},
		{
			name:  "ASC is consumed, because the catalog reports an ascending key by saying nothing",
			sql:   `CREATE INDEX "i" ON "t" ("score" ASC);`,
			parts: []ast.IndexPart{{Name: `"score"`}},
		},
		{
			name: "every suffix at once, on the key that carries it",
			sql:  `CREATE INDEX "i" ON "t" ("a", "b" text_pattern_ops DESC NULLS FIRST, ("lower"("c")) text_pattern_ops);`,
			parts: []ast.IndexPart{
				{Name: `"a"`},
				{Name: `"b"`, Operator: "text_pattern_ops", Desc: true, NullsOrder: ast.NullsOrderFirst},
				{Expr: `"lower"("c")`, Operator: "text_pattern_ops"},
			},
		},
		{
			name:  "a plain key list is left on the legacy path",
			sql:   `CREATE INDEX "i" ON "t" ("name");`,
			parts: nil,
		},
		{
			name:  "a bare expression key list is left on the legacy path",
			sql:   `CREATE INDEX "i" ON "t" ((lower(name)));`,
			parts: nil,
		},
		{
			name:  "a per-key COLLATE leaves the whole list alone rather than dropping it",
			sql:   `CREATE INDEX "i" ON "t" ("name" COLLATE "C" DESC);`,
			parts: nil,
		},
		{
			name:  "a MySQL prefix length is not an operator class",
			sql:   "CREATE INDEX `i` ON `t` (`name`(10) DESC);",
			parts: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			index := parseOneIndex(c, platform.Postgres, tt.sql)
			c.Assert(index.Parts, qt.DeepEquals, tt.parts)
		})
	}
}

// TestParseCreateIndexKeySuffixesOffPostgres pins the dialect gate: an operator
// class is a PostgreSQL-family suffix, and a dialect that has no such thing
// must not read a trailing identifier as one.
func TestParseCreateIndexKeySuffixesOffPostgres(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dialect string
		sql     string
		parts   []ast.IndexPart
	}{
		{
			name:    "MySQL keeps a direction but grows no operator class",
			dialect: platform.MySQL,
			sql:     "CREATE INDEX `i` ON `t` (`name` DESC);",
			parts:   []ast.IndexPart{{Name: "`name`", Desc: true}},
		},
		{
			name:    "MySQL leaves a two-identifier element opaque",
			dialect: platform.MySQL,
			sql:     "CREATE INDEX `i` ON `t` (`code` text_pattern_ops);",
			parts:   nil,
		},
		{
			name:    "the compatibility parser reads PostgreSQL DDL when no dialect was named",
			dialect: "",
			sql:     `CREATE INDEX "i" ON "t" ("code" text_pattern_ops);`,
			parts:   []ast.IndexPart{{Name: `"code"`, Operator: "text_pattern_ops"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			index := parseOneIndex(c, tt.dialect, tt.sql)
			c.Assert(index.Parts, qt.DeepEquals, tt.parts)
		})
	}
}

// TestPostgresIndexDDLSurvivesItsOwnParser is the property the blocker was
// about, stated directly: what the PostgreSQL renderer writes for an index, the
// SQL parser reads back into a node the renderer writes identically.
//
// A regression here is not a cosmetic one. The second rendering IS the DDL a
// plan replays when a `.sql` document is the desired state, and the shapes below
// were all measured being planned as `(("tsv" tsvector_ops(siglen=64)))`, which
// PostgreSQL 17.10 refuses.
func TestPostgresIndexDDLSurvivesItsOwnParser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ddl  string
	}{
		{
			name: "parameterised default operator class",
			ddl:  `CREATE INDEX "i" ON "t" USING gist ("tsv" tsvector_ops(siglen=64));`,
		},
		{
			name: "non-default operator class",
			ddl:  `CREATE INDEX "i" ON "t" ("code" text_pattern_ops);`,
		},
		{
			name: "descending key with an explicit NULLS ordering",
			ddl:  `CREATE INDEX "i" ON "t" ("created_at" DESC NULLS LAST);`,
		},
		{
			name: "ascending key with an explicit NULLS ordering",
			ddl:  `CREATE INDEX "i" ON "t" ("score" NULLS FIRST);`,
		},
		{
			name: "plain key",
			ddl:  `CREATE INDEX "i" ON "t" ("name");`,
		},
		{
			name: "storage parameters",
			ddl:  `CREATE INDEX "i" ON "t" USING brin ("ts") WITH (pages_per_range='32');`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			index := parseOneIndex(c, platform.Postgres, tt.ddl)
			rendered, err := renderer.RenderSQL(platform.Postgres, index)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, tt.ddl+"\n")
		})
	}
}
