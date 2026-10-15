package oracledata_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/oracledata"
	"ptah.run/internal/oracletype"
)

// TestDescriptor_NumericBoolFollowsTheTypeMap pins the Oracle descriptor's
// boolean spelling against the type a BOOLEAN column is created as: 1 and 0
// while the map makes it NUMBER(1), TRUE and FALSE the day it makes it
// BOOLEAN. A literal chosen by the server's leniency would stop matching the
// column when the map changes; this keeps the two in step.
func TestDescriptor_NumericBoolFollowsTheTypeMap(t *testing.T) {
	c := qt.New(t)

	described, found := platform.Lookup(platform.Oracle)

	c.Assert(found, qt.IsTrue)
	c.Assert(described.Literals.NumericBool, qt.Equals, oracletype.Map("BOOLEAN") != "BOOLEAN")
}

func TestColumns_HoldsMoment(t *testing.T) {
	tests := []struct {
		declared string
		want     bool
	}{
		{declared: "DATE", want: true},
		{declared: "TIMESTAMP", want: true},
		{declared: "timestamp(6) with time zone", want: true},
		{declared: "VARCHAR(20)", want: false},
		{declared: "INTEGER", want: false},
		{declared: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oracledata.Columns{}.HoldsMoment(test.declared), qt.Equals, test.want)
		})
	}
}

func TestServices_ProvideColumnsOnly(t *testing.T) {
	c := qt.New(t)

	services := oracledata.Services()

	c.Assert(services.Columns, qt.Equals, oracledata.Columns{})
	c.Assert(services.Literals, qt.IsNil)
	c.Assert(services.Rows, qt.IsNil)
}
