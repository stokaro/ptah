package ydbextensions_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/internal/ydbextensions"
)

func TestCoordinationObjectHasNoTableCreationPlacement(t *testing.T) {
	c := qt.New(t)
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: ydbcoordination.Ref("app", "node"), Value: &ydbcoordination.Desired{}})
	c.Assert(err, qt.IsNil)
	c.Assert(ydbextensions.ValidateObjects("ydb", capability.YDB262(), objects), qt.IsNil)
	err = ydbextensions.ValidateCreationObjects("ydb", capability.YDB262(), objects)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "(?s).*cannot be created inside a table.*")
}

func TestCoordinationObjectValidationRefusesUnsupportedState(t *testing.T) {
	for _, test := range []struct {
		name   string
		target string
		caps   capability.Capabilities
		ref    objectidentity.ID
		value  schemaext.Value
		want   error
	}{
		{"missing capability", "ydb", capability.Capabilities{}, ydbcoordination.Ref("", "node"), &ydbcoordination.Desired{}, ptaherr.ErrUnsupportedFeature},
		{"wrong target", "postgres", capability.YDB262(), ydbcoordination.Ref("", "node"), &ydbcoordination.Desired{}, ptaherr.ErrUnsupportedFeature},
		{"observed value", "ydb", capability.YDB262(), ydbcoordination.Ref("", "node"), &ydbcoordination.Observed{}, ptaherr.ErrUnsupportedFeature},
		{"protected root", "ydb", capability.YDB262(), ydbcoordination.Ref("", ydbcoordination.LockNode), &ydbcoordination.Desired{}, ptaherr.ErrInvalidSchemaDiff},
		{"invalid periods", "ydb", capability.YDB262(), ydbcoordination.Ref("", "node"), &ydbcoordination.Desired{Spec: ydbcoordination.Spec{SelfCheckPeriodMillis: 1}}, ptaherr.ErrInvalidSchemaDiff},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects, err := schemaext.NewObjects(schemaext.Object{Ref: test.ref, Value: test.value})
			c.Assert(err, qt.IsNil)
			c.Assert(ydbextensions.ValidateObjects(test.target, test.caps, objects), qt.ErrorIs, test.want)
		})
	}
}
