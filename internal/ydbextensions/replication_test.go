package ydbextensions_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbextensions"
)

func TestRetainedReplicationStateIsValidButCannotBeCreated(t *testing.T) {
	c := qt.New(t)
	value := &ydbschema.DesiredChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "stream", Mode: "UPDATES", Format: "JSON"},
		RetainedReplication: &ydbschema.ReplicationBinding{DestinationPath: "/remote/replica", ItemID: "1"}}
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: ydbschema.ChangefeedRef("", "t", "stream"), Value: value})
	c.Assert(err, qt.IsNil)
	c.Assert(ydbextensions.ValidateObjects("ydb", capability.YDB262(), objects), qt.IsNil)
	err = ydbextensions.ValidateCreationObjects("ydb", capability.YDB262(), objects)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "(?s).*not a standalone creation instruction.*")
}
