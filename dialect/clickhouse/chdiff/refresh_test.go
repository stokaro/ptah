package chdiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
)

// A refresh change replaces its view exactly when MODIFY REFRESH cannot make
// it: a schedule gained or lost, or APPEND added or removed. Every other change
// is made in place and keeps the view's rows.
func TestRefreshReplacesOwner(t *testing.T) {
	schedule := chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR"}
	appending := schedule
	appending.Append = true
	later := schedule
	later.Interval = "2 HOUR"
	tests := []struct {
		name     string
		change   chdiff.Refresh
		replaces bool
		impact   schemaext.Impact
	}{
		{name: "gained", change: chdiff.Refresh{After: &chschema.DesiredRefresh{Schedule: schedule}}, replaces: true, impact: schemaext.Destructive},
		{name: "lost", change: chdiff.Refresh{Before: &chschema.ObservedRefresh{Schedule: schedule}}, replaces: true, impact: schemaext.Destructive},
		{name: "APPEND added", change: chdiff.Refresh{Before: &chschema.ObservedRefresh{Schedule: schedule}, After: &chschema.DesiredRefresh{Schedule: appending}}, replaces: true, impact: schemaext.Destructive},
		{name: "APPEND removed", change: chdiff.Refresh{Before: &chschema.ObservedRefresh{Schedule: appending}, After: &chschema.DesiredRefresh{Schedule: schedule}}, replaces: true, impact: schemaext.Destructive},
		{name: "interval changed", change: chdiff.Refresh{Before: &chschema.ObservedRefresh{Schedule: schedule}, After: &chschema.DesiredRefresh{Schedule: later}}, impact: schemaext.Behavioral},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chdiff.ValidateRefresh(&test.change), qt.IsNil)
			c.Assert(test.change.ReplacesOwner(), qt.Equals, test.replaces)
			c.Assert(schemaext.ReplacesOwner(&test.change), qt.Equals, test.replaces)
			c.Assert(test.change.Effect().Impact, qt.Equals, test.impact)
		})
	}
}

func TestRefreshCodecRoundTripsAbsentSides(t *testing.T) {
	schedule := chschema.Schedule{Mode: chschema.RefreshAfter, Interval: "30 MINUTE", DependsOn: []string{"db.a", "db.b"}, Append: true}
	codec := chdiff.RefreshCodecs()[0]
	for _, change := range []*chdiff.Refresh{
		{After: &chschema.DesiredRefresh{Schedule: schedule}},
		{Before: &chschema.ObservedRefresh{Schedule: schedule}},
		{Before: &chschema.ObservedRefresh{Schedule: schedule}, After: &chschema.DesiredRefresh{Schedule: schedule}},
	} {
		c := qt.New(t)
		encoded, err := codec.Encode(change)
		c.Assert(err, qt.IsNil)
		decoded, err := codec.Decode(encoded)
		c.Assert(err, qt.IsNil)
		c.Assert(decoded, qt.DeepEquals, change)
	}
}

func TestRefreshCodecRefusesAChangeWithoutASchedule_FailurePath(t *testing.T) {
	codec := chdiff.RefreshCodecs()[0]
	for _, data := range []string{
		`{"before":null,"after":null}`,
		`{"after":null}`,
		`{"before":null,"after":{"mode":"EVERY","interval":"1 HOUR"},"extra":1}`,
		`{"before":null,"after":{"mode":"SOMETIMES","interval":"1 HOUR"}}`,
		`{"before":{"mode":"AFTER","interval":"1 HOUR","offset":"5 MINUTE"},"after":null}`,
	} {
		t.Run(data, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := codec.Decode([]byte(data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
	c := qt.New(t)
	encoded, err := codec.Encode(&chdiff.Refresh{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(encoded, qt.IsNil)
}
