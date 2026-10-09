package chreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chreverse"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func refreshRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: "example.org/refresh-reversal", Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs:    append(chschema.RefreshCodecs(), chdiff.RefreshCodecs()...),
		Reversals: []engine.Reversal{{Target: "clickhouse", Kinds: []schemaext.Kind{chdiff.RefreshKind}, Service: chreverse.RefreshService{}}},
	}))
}

func refreshView() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", "totals")
}

func hourly() chschema.Schedule {
	return chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR", DependsOn: []string{"analytics.source"}}
}

func daily() chschema.Schedule {
	return chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 DAY", Offset: "2 HOUR"}
}

// A schedule changed to another is changed back in place, and reversing the
// reversal gives the forward change back.
func TestRefreshReversalChangesTheScheduleBackInPlace(t *testing.T) {
	c := qt.New(t)
	record := schemaext.ChangeRecord{Subject: refreshView(), Value: &chdiff.Refresh{
		Before: &chschema.ObservedRefresh{Schedule: hourly()}, After: &chschema.DesiredRefresh{Schedule: daily()},
	}}

	result, err := refreshRuntime().ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "clickhouse", Changes: []schemaext.ChangeRecord{record}})

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	inverse := result[0].Change.Value.(*chdiff.Refresh)
	c.Assert(inverse.Before, qt.DeepEquals, &chschema.ObservedRefresh{Schedule: daily()})
	c.Assert(inverse.After, qt.DeepEquals, &chschema.DesiredRefresh{Schedule: hourly()})
	c.Assert(inverse.ReplacesOwner(), qt.IsFalse)
	c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: chschema.RefreshKind, Value: &chschema.ObservedRefresh{Schedule: daily()}}})
	c.Assert(result[0].Strategy, qt.Equals, "change the refresh schedule back in place")
	c.Assert(result[0].Limitations, qt.HasLen, 0)
	roundTrip, err := refreshRuntime().ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "clickhouse", Changes: []schemaext.ChangeRecord{result[0].Change}})
	c.Assert(err, qt.IsNil)
	c.Assert(roundTrip[0].Change, qt.DeepEquals, record)
}

// A schedule gained or lost replaced the view, so undoing it replaces the
// view again, and the rows the view held are not part of what comes back.
func TestRefreshReversalOfAReplacementReportsTheLostRows(t *testing.T) {
	tests := []struct {
		name    string
		change  *chdiff.Refresh
		inverse *chdiff.Refresh
		forward schemaext.Value
	}{
		{
			name:    "a schedule gained",
			change:  &chdiff.Refresh{After: &chschema.DesiredRefresh{Schedule: daily()}},
			inverse: &chdiff.Refresh{Before: &chschema.ObservedRefresh{Schedule: daily()}},
			forward: &chschema.ObservedRefresh{Schedule: daily()},
		},
		{
			name:    "a schedule lost",
			change:  &chdiff.Refresh{Before: &chschema.ObservedRefresh{Schedule: hourly()}},
			inverse: &chdiff.Refresh{After: &chschema.DesiredRefresh{Schedule: hourly()}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			record := schemaext.ChangeRecord{Subject: refreshView(), Value: test.change}

			result, err := refreshRuntime().ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "clickhouse", Changes: []schemaext.ChangeRecord{record}})

			c.Assert(err, qt.IsNil)
			c.Assert(result[0].Change.Value, qt.DeepEquals, test.inverse)
			c.Assert(result[0].Change.Value.(*chdiff.Refresh).ReplacesOwner(), qt.IsTrue)
			c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: chschema.RefreshKind, Value: test.forward}})
			c.Assert(result[0].Strategy, qt.Equals, "replace the materialized view with its captured definition and refresh schedule")
			c.Assert(result[0].Limitations, qt.DeepEquals, []string{"Replacing a materialized view restores its definition and refresh schedule but not the rows it held."})
		})
	}
}

func TestRefreshReversal_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*schemaext.ReversalRequest)
		want error
	}{
		{"target", func(r *schemaext.ReversalRequest) { r.Target = "postgres" }, ptaherr.ErrUnsupportedDialect},
		{"wrong model", func(r *schemaext.ReversalRequest) { r.Changes[1].Value = &chdiff.Index{} }, schemaext.ErrInvalidValue},
		{"no operand", func(r *schemaext.ReversalRequest) { r.Changes[1].Value = &chdiff.Refresh{} }, schemaext.ErrInvalidValue},
		{"invalid operand", func(r *schemaext.ReversalRequest) {
			r.Changes[1].Value.(*chdiff.Refresh).After.Mode = "SOMETIMES"
		}, schemaext.ErrInvalidValue},
		{"a table subject", func(r *schemaext.ReversalRequest) { r.Changes[1].Subject.Kind = objectidentity.KindTable }, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			change := func() schemaext.ChangeRecord {
				return schemaext.ChangeRecord{Subject: refreshView(), Value: &chdiff.Refresh{
					Before: &chschema.ObservedRefresh{Schedule: hourly()}, After: &chschema.DesiredRefresh{Schedule: daily()},
				}}
			}
			request := schemaext.ReversalRequest{Target: "clickhouse", Changes: []schemaext.ChangeRecord{change(), change()}}
			test.edit(&request)

			result, err := (chreverse.RefreshService{}).ReverseChanges(t.Context(), request)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}
