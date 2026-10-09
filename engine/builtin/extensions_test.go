package builtin_test

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"ptah.run/dialect/ydb/ydbworkload"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chrender"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine/builtin"
	"ptah.run/engine/builtin/internal/dialects/clickhouse"
	"ptah.run/engine/builtin/internal/dialects/mssql"
	"ptah.run/engine/builtin/internal/dialects/mysql"
	"ptah.run/engine/builtin/internal/dialects/oracle"
	"ptah.run/engine/builtin/internal/dialects/postgres"
	"ptah.run/engine/builtin/internal/dialects/sqlite"
	"ptah.run/engine/builtin/internal/dialects/ydb"
	"ptah.run/internal/astrouteguard"
	"ptah.run/internal/ydbextensions"
)

type extensionFixture struct {
	payload ast.ExtensionPayload
	wantSQL string
}

func extensionFixtures() []extensionFixture {
	feed := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	grown := feed.Clone()
	grown.RetentionPeriod = "PT2H"
	return []extensionFixture{
		{payload: &ydbast.AddChangefeed{Changefeed: feed}, wantSQL: "ALTER TABLE `items` ADD CHANGEFEED `updates` WITH (MODE = 'UPDATES', FORMAT = 'JSON');\n"},
		{payload: &ydbast.DropChangefeed{Name: "updates"}, wantSQL: "ALTER TABLE `items` DROP CHANGEFEED `updates`;\n"},
		{payload: &ydbast.AlterChangefeedTopic{Changefeed: grown, Previous: feed}, wantSQL: "ALTER TOPIC `items/updates` SET (retention_period = Interval('PT2H'));\n"},
	}
}

func clickhouseTTLFixture() extensionFixture {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()", TTL: "created_at + INTERVAL 1 DAY"}
	after := before.Desired()
	after.TTL.Value = ""
	return extensionFixture{payload: &chast.AlterTTL{Change: chdiff.Table{Before: before, After: after}}, wantSQL: "ALTER TABLE items REMOVE TTL;\n"}
}

func clickhouseIndexFixture() extensionFixture {
	return extensionFixture{payload: &chast.AddSkippingIndex{Name: "idx_c", Expression: "c"}, wantSQL: "ALTER TABLE `items` ADD INDEX `idx_c` c TYPE minmax GRANULARITY 1;\n"}
}

func coordinationFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.CoordinationNode{Schema: "app", Name: "locks", Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}}, wantSQL: "CREATE COORDINATION NODE `app/locks`;\n"}
}

func streamingFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.StreamingQuery{Operation: ydbast.StreamingCreate, Schema: "jobs.daily", Name: "copy.events",
		Spec: ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}},
		wantSQL: "CREATE STREAMING QUERY `jobs.daily/copy.events` WITH (RUN = FALSE, RESOURCE_POOL = `default`) AS DO BEGIN\nSELECT 1;\nEND DO;\n"}
}

func allExtensionFixtures() []extensionFixture {
	return append(extensionFixtures(), clickhouseTTLFixture(), clickhouseIndexFixture(), coordinationFixture(), streamingFixture(), poolFixture(), classifierFixture(), defaultPoolFixture())
}

func defaultPoolFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.DefaultPoolSettings{Spec: ydbworkload.PoolSpec{ResourceWeight: new(20.0)}},
		wantSQL: "ALTER RESOURCE POOL `default` SET (RESOURCE_WEIGHT = 20);\n"}
}

// The source inventory is independent of both owner registration and fixtures.
// Moving a concrete node out of core cannot remove its routing evidence.
func TestExtensionPayloads_CoverSourceTypesAndHandlers(t *testing.T) {
	c := qt.New(t)
	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	kinds, err := astrouteguard.ExtensionKinds(root)
	c.Assert(err, qt.IsNil)
	c.Assert(len(kinds) >= astrouteguard.ExtensionKindFloor, qt.IsTrue)
	var source []string
	for _, kind := range kinds {
		source = append(source, kind.Package+"."+kind.Name)
	}
	registry, err := ydbextensions.Registry()
	c.Assert(err, qt.IsNil)
	clickhouseRegistry, err := chrender.Registry()
	c.Assert(err, qt.IsNil)
	var registered, fixtures []string
	for _, payloadType := range append(registry.PayloadTypes(), clickhouseRegistry.PayloadTypes()...) {
		registered = append(registered, payloadType.Elem().PkgPath()+"."+payloadType.Elem().Name())
	}
	for _, fixture := range allExtensionFixtures() {
		payloadType := reflect.TypeOf(fixture.payload).Elem()
		fixtures = append(fixtures, payloadType.PkgPath()+"."+payloadType.Name())
	}
	slices.Sort(registered)
	slices.Sort(fixtures)
	c.Assert(registered, qt.DeepEquals, source)
	c.Assert(fixtures, qt.DeepEquals, source)
}

func TestExtensionPayloads_AllEntryPoints(t *testing.T) {
	for _, fixture := range allExtensionFixtures() {
		for _, dialect := range renderedDialects() {
			t.Run(dialect+"/"+string(fixture.payload.Kind()), func(t *testing.T) {
				c := qt.New(t)
				fragment := &ast.ExtensionAlterOperation{Payload: fixture.payload}
				parent := &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{fragment}}
				nodes := []ast.Node{fragment, parent, &ast.ExtensionStatement{Payload: fixture.payload}}
				for _, node := range nodes {
					visited := visitAnswer(c, dialect, node)
					c.Assert(renderAnswer(c, dialect, node), qt.DeepEquals, visited)
					c.Assert(renderSQLAnswer(dialect, node), qt.DeepEquals, visited)
				}
			})
		}
	}
}

func TestExtensionPayloads_OwnerRendersAndRequiresParent(t *testing.T) {
	for _, fixture := range extensionFixtures() {
		t.Run(string(fixture.payload.Kind()), func(t *testing.T) {
			c := qt.New(t)
			fragment := &ast.ExtensionAlterOperation{Payload: fixture.payload}
			parent := &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{fragment}}
			for _, visitor := range []renderer.RenderVisitor{ydb.NewWithCapabilities(capability.YDB262()), mustYDBRenderer(c)} {
				sql, err := visitor.Render(parent)
				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Equals, fixture.wantSQL)
				sql, err = visitor.Render(fragment)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
				c.Assert(err, qt.ErrorMatches, `extension .* requires an ALTER TABLE parent`)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

func mustYDBRenderer(c *qt.C) renderer.RenderVisitor {
	c.Helper()
	r, err := builtin.NewRendererWithCapabilities("ydb", capability.YDB262())
	c.Assert(err, qt.IsNil)
	return r
}

func TestClickHouseExtensionOwnerRendersAndNonownersRefuse(t *testing.T) {
	for _, fixture := range []extensionFixture{clickhouseTTLFixture(), clickhouseIndexFixture()} {
		for _, dialect := range renderedDialects() {
			t.Run(dialect+"/"+string(fixture.payload.Kind()), func(t *testing.T) {
				c := qt.New(t)
				parent := &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: fixture.payload}}}
				answer := renderSQLAnswer(dialect, parent)
				c.Assert(answer, qt.DeepEquals, visitAnswer(c, dialect, parent))
				c.Assert(answer, qt.DeepEquals, renderAnswer(c, dialect, parent))
			})
		}
		c := qt.New(t)
		sql, err := builtin.RenderSQL("clickhouse", &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: fixture.payload}}})
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, fixture.wantSQL)
		for _, dialect := range []string{"postgres", "mysql", "sqlite", "sqlserver", "oracle", "ydb"} {
			sql, err := builtin.RenderSQL(dialect, &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: fixture.payload}}})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		}
	}
}

func TestExtensionPayloads_RefuseMalformedDrop(t *testing.T) {
	for _, name := range []string{"", " ", "table/feed"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			op := &ast.ExtensionAlterOperation{Payload: &ydbast.DropChangefeed{Name: name}}
			for _, visitor := range []renderer.RenderVisitor{ydb.NewWithCapabilities(capability.YDB262()), mustYDBRenderer(c)} {
				for _, node := range []ast.Node{op, &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{op}}} {
					sql, err := visitor.Render(node)
					c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
					c.Assert(err, qt.ErrorMatches, "a changefeed needs a name without a slash")
					c.Assert(sql, qt.Equals, "")
				}
			}
		})
	}
}

func TestExtensionPayloads_NonownersRefuseClaimedCapabilities(t *testing.T) {
	caps := capability.YDB262()
	visitors := []renderer.RenderVisitor{
		clickhouse.NewWithCapabilities(caps), mssql.NewWithCapabilities(caps), mysql.NewWithCapabilities(caps),
		oracle.NewWithCapabilities(caps), postgres.NewWithCapabilities(caps, "postgres"), sqlite.NewWithCapabilities(caps),
	}
	for _, visitor := range visitors {
		for _, fixture := range extensionFixtures() {
			t.Run(visitor.Dialect()+"/"+string(fixture.payload.Kind()), func(t *testing.T) {
				c := qt.New(t)
				for _, node := range []ast.Node{
					&ast.ExtensionStatement{Payload: fixture.payload},
					&ast.ExtensionAlterOperation{Payload: fixture.payload},
					&ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("extra", "INTEGER")}, &ast.ExtensionAlterOperation{Payload: fixture.payload}}},
				} {
					sql, err := visitor.Render(node)
					c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
					c.Assert(fmt.Sprint(err), qt.Contains, string(fixture.payload.Kind()))
					c.Assert(sql, qt.Equals, "")
				}
			})
		}
	}
}

func TestClickHouseIndexExtensionRefusesMissingOperandsWithoutPartialSQL(t *testing.T) {
	for _, payload := range []ast.ExtensionPayload{(*chast.AddSkippingIndex)(nil), &chast.AddSkippingIndex{Name: "idx"}} {
		c := qt.New(t)
		parent := &ast.AlterTableNode{Name: "events", Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: ast.NewColumn("valid", "UInt64")},
			&ast.ExtensionAlterOperation{Payload: payload},
		}}
		for _, visitor := range []renderer.RenderVisitor{clickhouse.New(), mustClickHouseRenderer(c)} {
			sql, err := visitor.Render(parent)
			c.Assert(err, qt.IsNotNil)
			c.Assert(sql, qt.Equals, "")
			c.Assert(visitor.Output(), qt.Equals, "")
		}
	}
	c := qt.New(t)
	fragment := &ast.ExtensionAlterOperation{Payload: clickhouseIndexFixture().payload}
	for _, visitor := range []renderer.RenderVisitor{clickhouse.New(), mustClickHouseRenderer(c)} {
		sql, err := visitor.Render(fragment)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(sql, qt.Equals, "")
	}
}

func mustClickHouseRenderer(c *qt.C) renderer.RenderVisitor {
	c.Helper()
	r, err := builtin.NewRenderer("clickhouse")
	c.Assert(err, qt.IsNil)
	return r
}

func TestStreamingExtensionRendersAfterSelectedCodecRoundTrip(t *testing.T) {
	c := qt.New(t)
	fixture := streamingFixture()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	data, err := runtime.Codecs().Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{fixture.payload})
	c.Assert(err, qt.IsNil)
	values, err := runtime.Codecs().Unmarshal(c.Context(), data)
	c.Assert(err, qt.IsNil)
	node := &ast.ExtensionStatement{Payload: values[0].(ast.ExtensionPayload)}
	caps := capability.YDB262().With(capability.StreamingQueries, true)
	visitor, err := builtin.NewRendererWithCapabilities("ydb", caps)
	c.Assert(err, qt.IsNil)
	visited, err := visitor.Render(node)
	c.Assert(err, qt.IsNil)
	c.Assert(visited, qt.Equals, fixture.wantSQL)
	rendered, err := builtin.RenderSQLWithCapabilities("ydb", caps, node)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Equals, fixture.wantSQL)
	result, err := runtime.Render(c.Context(), renderer.Request{Target: "ydb", Capabilities: caps, Nodes: []ast.Node{node}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Fragments, qt.DeepEquals, []string{fixture.wantSQL})
}

func poolFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.ResourcePool{Operation: ydbast.PoolCreate, Name: "batch", Spec: &ydbworkload.PoolSpec{}}, wantSQL: "CREATE RESOURCE POOL `batch` WITH (CONCURRENT_QUERY_LIMIT = \"-1\");\n"}
}

func classifierFixture() extensionFixture {
	return extensionFixture{payload: &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolCreate, Name: "route", Spec: &ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 0}}, wantSQL: "CREATE RESOURCE POOL CLASSIFIER `route` WITH (RESOURCE_POOL = 'default', RANK = 0);\n"}
}
