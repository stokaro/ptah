package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// TestYDBSharedPaths_RefusesAViewAtATablesPath refuses a view declared at the
// path of a declared table before anything is rendered or planned: YDB keeps
// one object at a path.
func TestYDBSharedPaths_RefusesAViewAtATablesPath(t *testing.T) {
	for _, view := range []string{"app.object", `"app/".object`} {
		t.Run(view, func(t *testing.T) {
			c := qt.New(t)
			declared := viewBesideTable(view)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(declared, "ydb", capability.YDB262())
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `view app\.object has the path of a declared table, .*`)
			c.Assert(statements, qt.IsNil)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, &catalog.Database{}, catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262()}, nil, runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestYDBSharedPaths_RefusesAViewAtATopicsPath refuses a view declared at the
// path of a declared topic, however the view spells the path: the topic's
// owner finds the view's statement at its path.
func TestYDBSharedPaths_RefusesAViewAtATopicsPath(t *testing.T) {
	for _, view := range []string{"app.object", `"app"."object"`, "app/object"} {
		t.Run(view, func(t *testing.T) {
			c := qt.New(t)
			declared := viewBesideTopic(view)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(declared, "ydb", capability.YDB262())
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `topic create conflicts with create at scheme path ptah\.run/ydb/scheme-path app\.object`)
			c.Assert(statements, qt.IsNil)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, &catalog.Database{}, catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262()}, nil, runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(diff, qt.IsNil)
		})
	}
}

func TestYDBSharedPaths_KeepDistinctPaths(t *testing.T) {
	for _, view := range []string{"other.object", "app.other", `"app.object"`, "app.Object"} {
		t.Run(view, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(builtin.ValidateSchema(viewBesideTopic(view), "ydb"), qt.IsNil)
		})
	}
}

// viewBesideTable declares the view and the table app.object.
func viewBesideTable(view string) *schemamodel.Database {
	db := &schemamodel.Database{
		Views:  []schemamodel.View{{Name: view, Body: "SELECT 1 AS a"}},
		Tables: []schemamodel.Table{{StructName: "Object", Schema: "app", Name: "object"}},
		Fields: []schemamodel.Field{{StructName: "Object", Name: "id", Type: "BIGINT", Primary: true}},
	}
	schemamodel.Finalize(db)
	return db
}

// viewBesideTopic declares the view and the topic app/object.
func viewBesideTopic(view string) *schemamodel.Database {
	db := &schemamodel.Database{
		Views:          []schemamodel.View{{Name: view, Body: "SELECT 1 AS a"}},
		FeatureObjects: must.Must(schemaext.NewObjects(ydbtopic.DesiredObject("app", "object", "", ydbtopic.Spec{}))),
	}
	schemamodel.Finalize(db)
	return db
}
