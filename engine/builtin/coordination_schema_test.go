package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/atlascompat"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/engine/builtin"
)

func coordinationFeatureSchema(c *qt.C, name string) *schemamodel.Database {
	c.Helper()
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: ydbcoordination.Ref("app", name),
		Value: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}})
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{FeatureObjects: objects,
		Tables: []schemamodel.Table{{Schema: "app", Name: "items", StructName: "Item", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{{StructName: "Item", FieldName: "ID", Name: "id", Type: "Uint64", Primary: true}}}
}

func TestCoordinationFeatureSchemaUsesSelectedOwnerOnEveryEntryPoint(t *testing.T) {
	c := qt.New(t)
	assertOwnedSchemaEntryPoints(c, coordinationFeatureSchema(c, "node.with.dot"), capability.YDB262(), "CREATE COORDINATION NODE `app/node.with.dot` WITH (read_consistency_mode = 'strict');")
}

func assertOwnedSchemaEntryPoints(c *qt.C, database *schemamodel.Database, caps capability.Capabilities, sql string) {
	c.Helper()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	request := renderer.SchemaRequest{Target: "ydb", Capabilities: caps, Schema: database}
	rendered, err := runtime.RenderSchema(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Statements, qt.HasLen, 2)
	c.Assert(strings.Join(rendered.Statements, "\n"), qt.Contains, sql)
	validated, err := runtime.ValidateSchema(c.Context(), schemavalidation.Request{Target: "ydb", Capabilities: caps, Schema: database, NoSkipped: true})
	c.Assert(err, qt.IsNil)
	c.Assert(validated.Err("ydb"), qt.IsNil)
	list, err := atlascompat.SchemaToAST(c.Context(), runtime, *database, "ydb", caps)
	c.Assert(err, qt.IsNil)
	direct, err := runtime.Render(c.Context(), renderer.Request{Target: "ydb", Capabilities: caps, Nodes: list.Statements})
	c.Assert(err, qt.IsNil)
	c.Assert(strings.TrimSpace(direct.SQL()), qt.Equals, strings.TrimSpace(strings.Join(rendered.Statements, "")))
	c.Assert(database.FeatureObjects.Len(), qt.Equals, 1)
	c.Assert(database.Tables, qt.HasLen, 1)
}

func TestCoordinationFeatureSchemaRefusesConflictsAndCapabilitiesAtomically(t *testing.T) {
	for _, test := range []struct {
		name    string
		node    string
		caps    capability.Capabilities
		want    error
		message string
	}{
		{"common path conflict", "items", capability.YDB262(), ptaherr.ErrInvalidSchemaDiff, "conflicts with create at scheme path"},
		{"missing capability", "locks", capability.YDB262().With(capability.CoordinationNodes, false), ptaherr.ErrUnsupportedFeature, "coordination_nodes"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			database := coordinationFeatureSchema(c, test.node)
			rendered, err := runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "ydb", Capabilities: test.caps, Schema: database})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(err.Error(), qt.Contains, test.message)
			c.Assert(rendered, qt.DeepEquals, renderer.SchemaResult{})
			validated, err := runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "ydb", Capabilities: test.caps, Schema: database})
			c.Assert(err, qt.IsNil)
			c.Assert(validated.Err("ydb"), qt.ErrorIs, test.want)
			list, err := atlascompat.SchemaToAST(t.Context(), runtime, *database, "ydb", test.caps)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(list, qt.IsNil)
		})
	}
}
