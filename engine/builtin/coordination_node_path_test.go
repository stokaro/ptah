package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// nodeAt declares the coordination node app.locks beside what more declares.
func nodeAt(more schemamodel.Database) *schemamodel.Database {
	more.FeatureObjects = must.Must(schemaext.NewObjects(
		ydbcoordination.DesiredObject("app", "locks", "", ydbcoordination.Spec{})))

	schemamodel.Finalize(&more)
	return &more
}

// nodeBesideTable declares app.locks and the table name in the directory
// schema.
func nodeBesideTable(schema, name string) *schemamodel.Database {
	return nodeAt(schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Schema: schema, Name: name}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
	})
}

// nodeBesideView declares app.locks and the view name.
func nodeBesideView(name string) *schemamodel.Database {
	return nodeAt(schemamodel.Database{
		Views: []schemamodel.View{{StructName: "V", Name: name, Body: "SELECT 1 AS a"}},
	})
}

// nodeBesideTopic declares app.locks and the topic name in the directory
// schema.
func nodeBesideTopic(schema, name string) *schemamodel.Database {
	declared := nodeAt(schemamodel.Database{})
	declared.FeatureObjects = must.Must(declared.FeatureObjects.With(ydbtopic.DesiredObject(schema, name, "P", ydbtopic.Spec{})))
	return declared
}

// A coordination node whose path a declared table, view or topic holds is
// refused before anything is written, on the render surface and on the plan
// surface, which share the validation: YDB keeps one object at a path and
// answers `unexpected path type` for the later one. The refusal does not
// depend on which of the two the database holds already, since the
// declaration alone cannot be applied.
func TestValidateSchema_YDBRefusesACoordinationNodeOnAnotherObjectsPath(t *testing.T) {
	heldNode := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbcoordination.ObservedObject("app", "locks", ydbcoordination.Spec{})),
	),
	}
	tests := []struct {
		name     string
		kind     string
		declared *schemamodel.Database
		current  *catalog.Database
	}{
		{name: "a table, neither held", kind: "table", declared: nodeBesideTable("app", "locks"), current: &catalog.Database{}},
		{
			name: "a table added beside the node the database holds", kind: "table",
			declared: nodeBesideTable("app", "locks"), current: heldNode,
		},
		{
			name: "a node added beside the table the database holds", kind: "table",
			declared: nodeBesideTable("app", "locks"),
			current:  &catalog.Database{Tables: []catalog.Table{{Schema: "app", Name: "locks"}}},
		},
		{name: "a view, neither held", kind: "view", declared: nodeBesideView("app.locks"), current: &catalog.Database{}},
		{
			name: "a view added beside the node the database holds", kind: "view",
			declared: nodeBesideView("app.locks"), current: heldNode,
		},
		{
			name: "a view whose name quotes its parts", kind: "view",
			declared: nodeBesideView(`"app"."locks"`), current: &catalog.Database{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			want := `coordination create conflicts with create at scheme path ptah.run/ydb/scheme-path app.locks`

			statements, renderErr := builtin.GetOrderedCreateStatementsWithCapabilities(
				test.declared, platform.YDB, capability.YDB262(),
			)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			diff, planErr := schemadiff.CompareWithDatabaseInfo(t.Context(), test.declared, test.current,
				catalog.ServerInfo{Dialect: platform.YDB, Capabilities: capability.YDB262()}, nil, runtime)

			c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(renderErr, qt.ErrorMatches, want)
			c.Assert(statements, qt.IsNil)
			c.Assert(planErr, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(planErr, qt.ErrorMatches, want)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// A coordination node and a topic at one path are two owners' objects, and
// neither owner sees the other's statement, so the plan's own check of the
// path refuses them, on the render surface and on the plan surface alike.
func TestValidateSchema_YDBRefusesACoordinationNodeOnATopicsPath(t *testing.T) {
	c := qt.New(t)
	declared := nodeBesideTopic("app", "locks")
	want := `invalid schema diff: conflicting plan effects: unordered effects on ptah\.run/ydb/scheme-path app\.locks ` +
		`in \{ptah\.run/ydb coordination/000000/create\} and \{ptah\.run/ydb topic/000000/create\}`

	statements, renderErr := builtin.GetOrderedCreateStatementsWithCapabilities(declared, platform.YDB, capability.YDB262())
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	diff, planErr := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, &catalog.Database{},
		catalog.ServerInfo{Dialect: platform.YDB, Capabilities: capability.YDB262()}, nil, runtime)

	c.Assert(renderErr, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(renderErr, qt.ErrorMatches, want)
	c.Assert(statements, qt.IsNil)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(planErr, qt.ErrorMatches, want)
	c.Assert(diff, qt.IsNil)
}

// The controls: a name only shares a path when the directory and the name
// are both the same, and a dotted name in quotes is one path segment.
func TestValidateSchema_YDBTakesACoordinationNodeBesideOtherObjects(t *testing.T) {
	tests := []struct {
		name     string
		declared *schemamodel.Database
	}{
		{name: "a table in another directory", declared: nodeBesideTable("jobs", "locks")},
		{name: "a table under another name", declared: nodeBesideTable("app", "locks2")},
		{name: "a view whose dotted name is one segment", declared: nodeBesideView(`"app.locks"`)},
		{name: "a topic under another name", declared: nodeBesideTopic("app", "events")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := builtin.ValidateSchema(test.declared, platform.YDB)
			c.Assert(err, qt.IsNil)
		})
	}
}
