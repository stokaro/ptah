package clickhouse_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
)

func typedTable(value *chschema.DesiredTable) *ast.CreateTableNode {
	return &ast.CreateTableNode{Name: "events", Facets: must.Must(schemaext.NewFacets(value)),
		Columns: []*ast.ColumnNode{{Name: "id", Type: "UInt64", Primary: true}, {Name: "tenant", Type: "UInt64"},
			{Name: "created_at", Type: "DateTime"}, {Name: "version", Type: "UInt64"}},
	}
}

func TestTypedTableFacetsRenderEverySetting(t *testing.T) {
	c := qt.New(t)
	value := (&chschema.ObservedTable{
		Engine: "ReplacingMergeTree(version)", OrderBy: "tenant, id", PrimaryKey: "tenant", SampleBy: "tenant",
		PartitionBy: "toYYYYMM(created_at)", TTL: "created_at + toIntervalDay(30)", Settings: "index_granularity = 4096",
	}).Desired()
	node := typedTable(value)
	sql, err := builtin.RenderSQL("clickhouse", node)
	c.Assert(err, qt.IsNil)
	for _, clause := range []string{
		"ENGINE = ReplacingMergeTree(version)", "ORDER BY (tenant, id)", "PRIMARY KEY (tenant)", "SAMPLE BY tenant",
		"PARTITION BY toYYYYMM(created_at)", "TTL created_at + toIntervalDay(30)", "SETTINGS index_granularity = 4096",
	} {
		c.Assert(sql, qt.Contains, clause)
	}
	database := &schemamodel.Database{Tables: []schemamodel.Table{{Name: node.Name, StructName: "Event", Facets: node.Facets}}}
	for _, column := range node.Columns {
		database.Fields = append(database.Fields, schemamodel.Field{Name: column.Name, Type: column.Type, StructName: "Event", Primary: column.Primary})
	}
	statements, err := builtin.GetOrderedCreateStatements(database, "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Equals, sql)
	c.Assert(node.Options, qt.IsNil)
	c.Assert(database.Tables[0].Overrides, qt.IsNil)
	c.Assert(node.Facets, qt.DeepEquals, must.Must(schemaext.NewFacets(value)))
}

func TestTypedPrimaryKeyIntentControlsInheritance(t *testing.T) {
	for _, test := range []struct {
		name     string
		primary  chschema.Setting
		want     string
		declared bool
	}{
		{"omitted", chschema.Setting{}, "ORDER BY (tenant, id)", false},
		{"default", chschema.Setting{State: chschema.Default}, "ORDER BY (tenant, id)", false},
		{"explicit empty", chschema.Setting{State: chschema.Explicit}, "PRIMARY KEY (tuple())", true},
		{"explicit prefix", chschema.Setting{State: chschema.Explicit, Value: "tenant"}, "PRIMARY KEY (tenant)", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := typedTable(&chschema.DesiredTable{OrderBy: chschema.Setting{State: chschema.Explicit, Value: "tenant, id"}, PrimaryKey: test.primary})
			sql, err := builtin.RenderSQL("clickhouse", node)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
			c.Assert(strings.Contains(sql, "PRIMARY KEY"), qt.Equals, test.declared)
		})
	}
}

func TestTypedSortKeyIntentControlsCommonKeyFallback(t *testing.T) {
	for _, test := range []struct {
		name  string
		order chschema.Setting
		want  string
	}{
		{"omitted", chschema.Setting{}, "ORDER BY (id)"},
		{"default", chschema.Setting{State: chschema.Default}, "ORDER BY (id)"},
		{"explicit empty", chschema.Setting{State: chschema.Explicit}, "ORDER BY (tuple())"},
		{"explicit order", chschema.Setting{State: chschema.Explicit, Value: "tenant, id"}, "ORDER BY (tenant, id)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQL("clickhouse", typedTable(&chschema.DesiredTable{OrderBy: test.order}))
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

func TestTypedTableFacetsRefuseDuplicateWritableOptions(t *testing.T) {
	for _, key := range []string{"ENGINE", "ORDER_BY", "PRIMARY_KEY", "PARTITION_BY", "SAMPLE_BY", "TTL", "SETTINGS"} {
		t.Run(key, func(t *testing.T) {
			c := qt.New(t)
			node := typedTable(&chschema.DesiredTable{})
			node.Options = map[string]string{key: ""}
			sql, err := builtin.RenderSQL("clickhouse", node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `(?s).*both a typed facet and table options.*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func TestTypedTableFacetsRefuseWrongTargetOrPlacement(t *testing.T) {
	for _, target := range []string{"postgres", "mysql", "sqlite"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQL(target, typedTable(&chschema.DesiredTable{}))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
	t.Run("column", func(t *testing.T) {
		c := qt.New(t)
		node := typedTable(&chschema.DesiredTable{})
		node.Columns[0].Facets, node.Facets = node.Facets, schemaext.Facets{}
		sql, err := builtin.RenderSQL("clickhouse", node)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(sql, qt.Equals, "")
	})
	t.Run("observation", func(t *testing.T) {
		c := qt.New(t)
		node := typedTable(&chschema.DesiredTable{})
		node.Facets = must.Must(schemaext.NewFacets(&chschema.ObservedTable{Engine: "Memory"}))
		sql, err := builtin.RenderSQL("clickhouse", node)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(sql, qt.Equals, "")
	})
}
