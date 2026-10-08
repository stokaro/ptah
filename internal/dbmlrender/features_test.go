package dbmlrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/dbmlrender"
)

func TestRender_FeatureLossNeedsNoTarget(t *testing.T) {
	c := qt.New(t)
	result, err := renderDBML(c, storageSchema(c), dbmlrender.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Omitted, qt.Contains, "changefeeds (2)")

	filtered, err := renderDBML(c, storageSchema(c), dbmlrender.Options{ExcludeTables: []string{"events", "archive"}})
	c.Assert(err, qt.IsNil)
	c.Assert(filtered.Omitted, qt.HasLen, 0)
}

func TestRender_FeatureParentsKeepComponentBoundaries(t *testing.T) {
	c := qt.New(t)
	spec := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	objects, err := schemaext.NewObjects(ydbschema.DesiredObject("", "tenant.events", spec), ydbschema.DesiredObject("tenant", "events", spec))
	c.Assert(err, qt.IsNil)
	db := &schemamodel.Database{FeatureObjects: objects, Tables: []schemamodel.Table{
		{Name: "tenant.events", StructName: "Literal"}, {Schema: "tenant", Name: "events", StructName: "Qualified"},
	}}
	result, err := renderDBML(c, db, dbmlrender.Options{IncludeTables: []string{"tenant.events"}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Omitted, qt.DeepEquals, []string{"changefeeds (1)"})
}

func TestRender_UnknownFeatureParentCannotDisappearThroughFiltering(t *testing.T) {
	c := qt.New(t)
	db := storageSchema(c)
	db.Tables = []schemamodel.Table{{Name: "plain", StructName: "Plain"}}
	result, err := renderDBML(c, db, dbmlrender.Options{IncludeTables: []string{"plain"}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, dbmlrender.Result{})
}
