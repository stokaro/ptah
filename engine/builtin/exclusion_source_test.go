package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// The rendered source preserves names determined by declaration order, while
// equal CREATE declarations remain PostgreSQL's own folding decision.
func TestOwnedExclusion_SourceRendering(t *testing.T) {
	tests := []struct {
		name, sql string
		names     []string
	}{
		{"unnamed", `CREATE TABLE t (a int, EXCLUDE (a WITH =));`, []string{"t_a_excl"}},
		{"named", `CREATE TABLE t (a int, CONSTRAINT named EXCLUDE (a WITH =));`, []string{"named"}},
		{"constraint before column", `CREATE TABLE t (EXCLUDE (a WITH =), a int);`, []string{"t_a_excl"}},
		{"unique before exclusion", `CREATE TABLE t (a int, b int, CONSTRAINT t_a_excl UNIQUE (b), EXCLUDE (a WITH =));`, []string{"t_a_excl1"}},
		{"column after exclusion", `CREATE TABLE t (a int, EXCLUDE (a WITH =), b int UNIQUE);`, []string{"t_a_excl"}},
		{"fold keeps later name", `CREATE TABLE t (a int, EXCLUDE (a WITH =), CONSTRAINT named EXCLUDE (a WITH =));`, []string{"named"}},
		{"distinct timing", `CREATE TABLE t (a int, EXCLUDE (a WITH =), EXCLUDE (a WITH =) INITIALLY DEFERRED);`, []string{"t_a_excl", "t_a_excl1"}},
		{"alter addition", `CREATE TABLE t (a int); ALTER TABLE t ADD EXCLUDE (a WITH =);`, []string{"t_a_excl"}},
		{"quoted identity", `CREATE TABLE "T.dot" (a int, CONSTRAINT "E.dot" EXCLUDE (a WITH =));`, []string{"E.dot"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			parsed, err := parser.NewParser(test.sql, parser.WithDialect("postgres"), parser.WithSQL(runtime.SQL())).ParseContext(t.Context())
			c.Assert(err, qt.IsNil)
			rendered, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres18(), parsed)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Contains, "EXCLUDE USING btree")
			repeated, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres18(), parsed)
			c.Assert(err, qt.IsNil)
			c.Assert(repeated, qt.Equals, rendered)
			model, _, err := sqlschema.ReadWith(t.Context(), []byte(rendered), "postgres", nil, sqlschema.Options{Owners: runtime.SQL()})
			c.Assert(err, qt.IsNil)
			var names []string
			for _, ref := range model.FeatureObjects.Refs() {
				names = append(names, ref.Name.Source)
			}
			c.Assert(names, qt.DeepEquals, test.names)
		})
	}
}

func TestOwnedExclusion_SourceBodyOrder(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	parsed, err := parser.NewParser(`CREATE TABLE t (EXCLUDE (a WITH =), a int, CONSTRAINT c CHECK (a > 0), b int);`, parser.WithDialect("postgres"), parser.WithSQL(runtime.SQL())).ParseContext(t.Context())
	c.Assert(err, qt.IsNil)
	node, ok := parsed.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	rendered, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres18(), parsed)
	c.Assert(err, qt.IsNil)
	for _, token := range []string{"EXCLUDE", `"a"`, `CONSTRAINT "c"`, `"b"`} {
		c.Assert(rendered, qt.Contains, token)
	}
	c.Assert(strings.Index(rendered, "EXCLUDE") < strings.Index(rendered, `"a"`), qt.IsTrue)
	c.Assert(strings.Index(rendered, `CONSTRAINT "c"`) < strings.Index(rendered, `"b"`), qt.IsTrue)
	c.Assert(node.OwnedConstraints[0].Name, qt.Equals, "")
	c.Assert(node.Elements[0].OwnedConstraint, qt.Equals, node.OwnedConstraints[0])
	c.Assert(node.Elements[1].Column, qt.Equals, node.Columns[0])
}

func TestOwnedExclusion_SourceRenderingKeepsNameCollision(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	source := `CREATE TABLE t (a int, EXCLUDE (a WITH =), b int CONSTRAINT t_a_excl UNIQUE);`
	parsed, err := parser.NewParser(source, parser.WithDialect("postgres"), parser.WithSQL(runtime.SQL())).ParseContext(t.Context())
	c.Assert(err, qt.IsNil)
	rendered, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres18(), parsed)
	c.Assert(err, qt.IsNil)
	_, _, err = sqlschema.ReadWith(t.Context(), []byte(rendered), "postgres", nil, sqlschema.Options{Owners: runtime.SQL()})
	c.Assert(err, qt.ErrorMatches, ".*two constraints of one CREATE TABLE claim the same name.*")
}

func TestOwnedExclusion_SourceRenderingRefusesOtherTargets(t *testing.T) {
	for _, target := range []string{"mysql", "clickhouse", "cockroachdb", "yugabytedb", "spanner", "sqlite", "sqlserver", "oracle", "ydb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			parsed, err := parser.NewParser(`CREATE TABLE t (a int, EXCLUDE (a WITH =));`, parser.WithDialect("postgres"), parser.WithSQL(runtime.SQL())).ParseContext(t.Context())
			c.Assert(err, qt.IsNil)
			rendered, err := builtin.RenderSQLWithCapabilities(target, capability.ForDialect(target), parsed)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, "(?s).*EXCLUDE.*")
			c.Assert(rendered, qt.Equals, "")
		})
	}
}
