package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/convert/goschematodb"
)

// TestToDBSchema_LeavesOutAnObjectTheTargetSkips pins the conversion of a
// declaration for a target that does not serve one of its objects, whose
// owner skips it there: the object is not part of the target's state, so it
// is left out rather than refused for want of a converter. On a target that
// serves it, it is converted.
func TestToDBSchema_LeavesOutAnObjectTheTargetSkips(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(
		pgextension.Ref("pg_trgm"), pgextension.DesiredExtension{}))))}

	mysql, err := goschematodb.ToDBSchema(t.Context(), declared, "mysql", runtime)
	c.Assert(err, qt.IsNil)
	postgres, err := goschematodb.ToDBSchema(t.Context(), declared, "postgres", runtime)
	c.Assert(err, qt.IsNil)

	c.Assert(mysql.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(postgres.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{pgextension.Ref("pg_trgm")})
}
