package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func TestYDBSharedPaths_RefusesViewsBeforeRenderingOrPlanning(t *testing.T) {
	for _, test := range []struct {
		name string
		kind string
		view string
	}{
		{"table and view", "table", "app.object"},
		{"topic and view", "topic", "app.object"},
		{"quoted view parts", "topic", `"app"."object"`},
		{"slash path", "topic", "app/object"},
		{"trailing directory slash", "table", `"app/".object`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := ydbPathFixture(test.kind, test.view)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(declared, "ydb", capability.YDB262())
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*app.object has the path of a declared.*`)
			c.Assert(statements, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabaseInfo(declared, &catalog.Database{}, catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262()}, nil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(diff, qt.IsNil)
		})
	}
}

func TestYDBSharedPaths_KeepDistinctPaths(t *testing.T) {
	for _, view := range []string{"other.object", "app.other", `"app.object"`, "app.Object"} {
		t.Run(view, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(builtin.ValidateSchema(ydbPathFixture("topic", view), "ydb"), qt.IsNil)
		})
	}
}

func ydbPathFixture(kind, view string) *schemamodel.Database {
	db := &schemamodel.Database{Views: []schemamodel.View{{Name: view, Body: "SELECT 1 AS a"}}}
	if kind == "table" {
		db.Tables = []schemamodel.Table{{StructName: "Object", Schema: "app", Name: "object"}}
		db.Fields = []schemamodel.Field{{StructName: "Object", Name: "id", Type: "BIGINT", Primary: true}}
	} else {
		db.Topics = []schemamodel.Topic{{Schema: "app", Name: "object"}}
	}
	schemamodel.Finalize(db)
	return db
}
