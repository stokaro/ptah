package builtin_test

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
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
	feed := ast.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	grown := feed.Clone()
	grown.RetentionPeriod = "PT2H"
	return []extensionFixture{
		{payload: &ydbast.AddChangefeed{Changefeed: feed}, wantSQL: "ALTER TABLE `items` ADD CHANGEFEED `updates` WITH (MODE = 'UPDATES', FORMAT = 'JSON');\n"},
		{payload: &ydbast.DropChangefeed{Name: "updates"}, wantSQL: "ALTER TABLE `items` DROP CHANGEFEED `updates`;\n"},
		{payload: &ydbast.AlterChangefeedTopic{Changefeed: grown, Previous: feed}, wantSQL: "ALTER TOPIC `items/updates` SET (retention_period = Interval('PT2H'));\n"},
	}
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
	var registered, fixtures []string
	for _, payloadType := range registry.PayloadTypes() {
		registered = append(registered, payloadType.Elem().PkgPath()+"."+payloadType.Elem().Name())
	}
	for _, fixture := range extensionFixtures() {
		payloadType := reflect.TypeOf(fixture.payload).Elem()
		fixtures = append(fixtures, payloadType.PkgPath()+"."+payloadType.Name())
	}
	slices.Sort(registered)
	slices.Sort(fixtures)
	c.Assert(registered, qt.DeepEquals, source)
	c.Assert(fixtures, qt.DeepEquals, source)
}

func TestExtensionPayloads_AllEntryPoints(t *testing.T) {
	for _, fixture := range extensionFixtures() {
		for _, dialect := range renderedDialects() {
			t.Run(dialect+"/"+string(fixture.payload.Kind()), func(t *testing.T) {
				c := qt.New(t)
				fragment := &ast.ExtensionAlterOperation{Payload: fixture.payload}
				parent := &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{fragment}}
				for _, node := range []ast.Node{fragment, parent} {
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
