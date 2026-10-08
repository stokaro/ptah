package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/engine/builtin/internal/dialects/clickhouse"
	"ptah.run/migration/safety"
)

func TestClickHouseTTLRenderingRetainsOwnerRisk(t *testing.T) {
	for _, test := range []struct {
		name  string
		after string
		sql   string
	}{
		{name: "modify", after: "created_at + INTERVAL 30 DAY", sql: "ALTER TABLE events MODIFY TTL created_at + INTERVAL 30 DAY;\n"},
		{name: "remove", sql: "ALTER TABLE events REMOVE TTL;\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := &chschema.ObservedTable{Engine: "MergeTree", TTL: "created_at + INTERVAL 7 DAY"}
			after := before.Desired()
			after.TTL.Value = test.after
			op := &chast.AlterTTL{Change: chdiff.Table{Before: before, After: after}}
			fragment := &ast.ExtensionAlterOperation{Payload: op}
			parent := &ast.AlterTableNode{Name: "events", Operations: []ast.AlterOperation{fragment}}
			for _, visitor := range []renderer.RenderVisitor{clickhouse.New(), must.Must(builtin.NewRenderer("clickhouse"))} {
				sql, err := visitor.Render(parent)
				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Equals, test.sql)
				sql, err = visitor.Render(fragment)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
				c.Assert(sql, qt.Equals, "")
			}
			assessments, err := safety.AssessRendered(t.Context(), must.Must(builtin.New()), []ast.Node{parent}, "clickhouse")
			c.Assert(err, qt.IsNil)
			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, safety.Warning)
			c.Assert(assessments[0].Reason, qt.Contains, "cannot recover")
		})
	}
}

func TestClickHouseTTLRefusesMalformedPayloadWithoutPartialSQL(t *testing.T) {
	before := &chschema.ObservedTable{Engine: "MergeTree", TTL: "created_at + INTERVAL 7 DAY"}
	blank := before.Desired()
	blank.TTL.Value = " \t\n"
	for _, payload := range []*chast.AlterTTL{
		nil, {}, {Change: chdiff.Table{Before: before, After: blank}},
	} {
		c := qt.New(t)
		parent := &ast.AlterTableNode{Name: "events", Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: ast.NewColumn("extra", "Int64")},
			&ast.ExtensionAlterOperation{Payload: payload},
		}}
		for _, visitor := range []renderer.RenderVisitor{clickhouse.New(), must.Must(builtin.NewRenderer("clickhouse"))} {
			sql, err := visitor.Render(parent)
			c.Assert(err, qt.IsNotNil)
			c.Assert(sql, qt.Equals, "")
		}
	}
}

func TestClickHouseTTLNonownersRefuseWithoutPartialSQL(t *testing.T) {
	payload := clickhouseTTLFixture().payload
	for _, dialect := range []string{"postgres", "cockroachdb", "mysql", "mariadb", "sqlite", "sqlserver", "oracle", "spanner", "ydb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			parent := &ast.AlterTableNode{Name: "events", Operations: []ast.AlterOperation{
				&ast.AddColumnOperation{Column: ast.NewColumn("extra", "INTEGER")},
				&ast.ExtensionAlterOperation{Payload: payload},
			}}
			sql, err := builtin.RenderSQL(dialect, parent)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
