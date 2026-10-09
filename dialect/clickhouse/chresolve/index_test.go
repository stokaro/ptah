package chresolve_test

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
)

func TestIndexResolutionSeparatesIntentEvidenceAndCreationRules(t *testing.T) {
	current := &chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}
	explicit := (&chschema.ObservedIndex{IndexType: "bloom_filter(0.01)", Granularity: 64}).Desired()
	defaults := &chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Default}, Granularity: chschema.GranularitySetting{State: chschema.Default}}
	for _, test := range []struct {
		name    string
		request chresolve.IndexRequest
		want    chschema.ObservedIndex
		origins chresolve.IndexOrigins
	}{
		{"create omitted", chresolve.IndexRequest{Desired: &chschema.DesiredIndex{}, Creating: true}, chschema.ObservedIndex{IndexType: "minmax", Granularity: 1},
			chresolve.IndexOrigins{IndexType: chresolve.CreationRule, Granularity: chresolve.CreationRule}},
		{"retain captured", chresolve.IndexRequest{Desired: &chschema.DesiredIndex{}, Current: current}, *current,
			chresolve.IndexOrigins{IndexType: chresolve.Observation, Granularity: chresolve.Observation}},
		{"reset existing", chresolve.IndexRequest{Desired: defaults, Current: current}, chschema.ObservedIndex{IndexType: "minmax", Granularity: 1},
			chresolve.IndexOrigins{IndexType: chresolve.CreationRule, Granularity: chresolve.CreationRule}},
		{"explicit without observation", chresolve.IndexRequest{Desired: explicit}, chschema.ObservedIndex{IndexType: "bloom_filter(0.01)", Granularity: 64},
			chresolve.IndexOrigins{IndexType: chresolve.Declaration, Granularity: chresolve.Declaration}},
		{"default without observation", chresolve.IndexRequest{Desired: defaults}, chschema.ObservedIndex{IndexType: "minmax", Granularity: 1},
			chresolve.IndexOrigins{IndexType: chresolve.CreationRule, Granularity: chresolve.CreationRule}},
		{"change type retain granularity", chresolve.IndexRequest{Desired: &chschema.DesiredIndex{IndexType: explicit.IndexType}, Current: current},
			chschema.ObservedIndex{IndexType: "bloom_filter(0.01)", Granularity: math.MaxUint64},
			chresolve.IndexOrigins{IndexType: chresolve.Declaration, Granularity: chresolve.Observation}},
		{"retain type change granularity", chresolve.IndexRequest{Desired: &chschema.DesiredIndex{Granularity: explicit.Granularity}, Current: current},
			chschema.ObservedIndex{IndexType: "set(100)", Granularity: 64},
			chresolve.IndexOrigins{IndexType: chresolve.Observation, Granularity: chresolve.Declaration}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := *test.request.Desired
			result, err := chresolve.Index(test.request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Declared, qt.DeepEquals, before)
			c.Assert(result.Prepared, qt.DeepEquals, *test.want.Desired())
			c.Assert(result.Origins, qt.DeepEquals, test.origins)
			c.Assert(*test.request.Desired, qt.DeepEquals, before)
			c.Assert(current.Granularity, qt.Equals, uint64(math.MaxUint64))
		})
	}
}

func TestIndexResolutionRefusesUnknownAndContradictoryInputs(t *testing.T) {
	for _, test := range []struct {
		name    string
		request chresolve.IndexRequest
		want    error
	}{
		{"nil declaration", chresolve.IndexRequest{}, schemaext.ErrInvalidValue},
		{"invalid intent", chresolve.IndexRequest{Desired: &chschema.DesiredIndex{Granularity: chschema.GranularitySetting{State: chschema.Explicit}}}, schemaext.ErrInvalidValue},
		{"invalid observation", chresolve.IndexRequest{Desired: &chschema.DesiredIndex{}, Current: &chschema.ObservedIndex{}}, schemaext.ErrInvalidValue},
		{"new with observation", chresolve.IndexRequest{Desired: &chschema.DesiredIndex{}, Current: &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}, Creating: true}, schemaext.ErrInvalidValue},
		{"unknown type", chresolve.IndexRequest{Desired: &chschema.DesiredIndex{Granularity: chschema.GranularitySetting{State: chschema.Explicit, Value: 1}}}, chresolve.ErrUnknownCurrent},
		{"unknown granularity", chresolve.IndexRequest{Desired: &chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Explicit, Value: "minmax"}}}, chresolve.ErrUnknownCurrent},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := chresolve.Index(test.request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, chresolve.IndexResult{})
		})
	}
}
