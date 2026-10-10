package mysqlconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlconvert"
	"ptah.run/dialect/mysql/mysqlschema"
)

// An observation becomes the declaration of the hint it holds. A declaration
// becomes the observation of an index on a table of the default row format,
// where MariaDB keeps the hint and MySQL does not.
func TestIndexBlockSizeService_HappyPath(t *testing.T) {
	tests := []struct {
		name, target string
		from, to     schemaext.Representation
		value        schemaext.Value
		want         schemaext.Value
	}{
		{name: "an observation", target: "mysql", from: schemaext.Observed, to: schemaext.Desired,
			value: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: true}, want: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}},
		{name: "a MariaDB declaration", target: "mariadb", from: schemaext.Desired, to: schemaext.Observed,
			value: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}, want: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: true}},
		{name: "a MySQL declaration", target: "mysql", from: schemaext.Desired, to: schemaext.Observed,
			value: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}, want: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := mysqlconvert.IndexBlockSizeService{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
				Target: test.target, From: test.from, To: test.to, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.IsNil)
			c.Assert(converted, qt.DeepEquals, []schemaext.Value{test.want})
		})
	}
}

func TestIndexBlockSizeService_FailurePath(t *testing.T) {
	c := qt.New(t)

	converted, err := mysqlconvert.IndexBlockSizeService{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "mysql", From: schemaext.Desired, To: schemaext.Observed,
		Values: []schemaext.Value{&mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8}}})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*expected desired MySQL index block size.*`)
	c.Assert(converted, qt.IsNil)
}
