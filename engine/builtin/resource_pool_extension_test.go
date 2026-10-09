package builtin_test

import (
	"testing"

	"ptah.run/dialect/ydb/ydbworkload"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/engine/builtin"
	"ptah.run/engine/builtin/internal/dialects/ydb"
)

func TestWorkloadOperationsUseRegisteredCodecsAndOwnerRendering(t *testing.T) {
	for _, fixture := range []extensionFixture{poolFixture(), classifierFixture()} {
		t.Run(string(fixture.payload.Kind()), func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			data, err := runtime.Codecs().Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{fixture.payload})
			c.Assert(err, qt.IsNil)
			values, err := runtime.Codecs().Unmarshal(c.Context(), data)
			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.DeepEquals, []schemaext.Payload{fixture.payload})
			caps := capability.YDB262().With(capability.ResourcePools, true)
			wrapper, err := builtin.NewRendererWithCapabilities("ydb", caps)
			c.Assert(err, qt.IsNil)
			node := &ast.ExtensionStatement{Payload: values[0].(ast.ExtensionPayload)}
			for _, visitor := range []renderer.RenderVisitor{ydb.NewWithCapabilities(caps), wrapper} {
				sql, err := visitor.Render(node)
				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Equals, fixture.wantSQL)
			}
			sql, err := builtin.RenderSQLWithCapabilities("ydb", caps.With(capability.ResourcePools, false), node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func TestWorkloadOperationsRejectInvalidLaterStatementWithoutPartialSQL(t *testing.T) {
	for _, payload := range []ast.ExtensionPayload{
		&ydbast.ResourcePool{Operation: ydbast.PoolDrop, Name: "default"},
		&ydbast.ResourcePool{Operation: ydbast.PoolAlter, Name: "batch", Spec: &ydbworkload.PoolSpec{}},
		&ydbast.ResourcePoolClassifier{Operation: ydbast.PoolCreate, Name: "route", Spec: &ydbworkload.ClassifierSpec{}},
	} {
		t.Run(string(payload.Kind()), func(t *testing.T) {
			c := qt.New(t)
			caps := capability.YDB262().With(capability.ResourcePools, true)
			wrapper, err := builtin.NewRendererWithCapabilities("ydb", caps)
			c.Assert(err, qt.IsNil)
			statements := &ast.StatementList{Statements: []ast.Node{
				&ast.ExtensionStatement{Payload: poolFixture().payload},
				&ast.ExtensionStatement{Payload: payload},
			}}
			for _, visitor := range []renderer.RenderVisitor{ydb.NewWithCapabilities(caps), wrapper} {
				sql, err := visitor.Render(statements)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
				c.Assert(sql, qt.Equals, "")
				c.Assert(visitor.Output(), qt.Equals, "")
			}
		})
	}
}
