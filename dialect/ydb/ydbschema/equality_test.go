package ydbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

func TestOptionalChangefeedOperandEquality(t *testing.T) {
	var desired *ydbschema.DesiredChangefeed
	var observed *ydbschema.ObservedChangefeed
	for _, test := range []struct {
		name        string
		left, right schemaext.Value
		want        bool
	}{
		{name: "absent desired operands", left: desired, right: desired, want: true},
		{name: "absent observed operands", left: observed, right: observed, want: true},
		{name: "different representations", left: desired, right: observed},
		{name: "absent and present desired", left: desired, right: &ydbschema.DesiredChangefeed{}},
		{name: "present and absent desired", left: &ydbschema.DesiredChangefeed{}, right: desired},
		{name: "absent and present observed", left: observed, right: &ydbschema.ObservedChangefeed{}},
		{name: "present and absent observed", left: &ydbschema.ObservedChangefeed{}, right: observed},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.left.Equal(test.right), qt.Equals, test.want)
		})
	}
}
