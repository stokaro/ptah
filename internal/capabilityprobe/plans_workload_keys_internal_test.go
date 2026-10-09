package capabilityprobe

// White-box testing required: read-back classification is otherwise reached
// only by a live probe. A malformed snapshot must be indeterminate, rather than
// report that a server does not support a feature it may have executed.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

func TestWorkloadReadBackDistinguishesMatchingAndMissingObjects(t *testing.T) {
	pool := ydbworkload.ObservedPoolObject("pool", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(3))})
	classifier := ydbworkload.ObservedClassifierObject("route", ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 1})
	for _, test := range []struct {
		name    string
		objects []schemaext.Object
		matches bool
	}{
		{name: "matching", objects: []schemaext.Object{pool, classifier}, matches: true},
		{name: "missing pool", objects: []schemaext.Object{classifier}},
		{name: "missing classifier", objects: []schemaext.Object{pool}},
		{name: "different settings", objects: []schemaext.Object{ydbworkload.ObservedPoolObject("pool", ydbworkload.PoolSpec{}), classifier}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			found, err := observedWorkloadMatches(must.Must(schemaext.NewObjects(test.objects...)), "pool",
				ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(3))}, "route", ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 1})
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.Equals, test.matches)
		})
	}
}

func TestWorkloadReadBackRefusesDesiredValues(t *testing.T) {
	for _, test := range []struct {
		name    string
		objects []schemaext.Object
	}{
		{name: "pool", objects: []schemaext.Object{
			ydbworkload.DesiredPoolObject("pool", "", ydbworkload.PoolSpec{}),
			ydbworkload.ObservedClassifierObject("route", ydbworkload.ClassifierSpec{ResourcePool: "pool"}),
		}},
		{name: "classifier", objects: []schemaext.Object{
			ydbworkload.ObservedPoolObject("pool", ydbworkload.PoolSpec{}),
			ydbworkload.DesiredClassifierObject("route", "", ydbworkload.ClassifierSpec{ResourcePool: "pool"}),
		}},
		{name: "desired pool and missing classifier", objects: []schemaext.Object{
			ydbworkload.DesiredPoolObject("pool", "", ydbworkload.PoolSpec{}),
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			found, err := observedWorkloadMatches(must.Must(schemaext.NewObjects(test.objects...)), "pool",
				ydbworkload.PoolSpec{}, "route", ydbworkload.ClassifierSpec{ResourcePool: "pool"})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(found, qt.IsFalse)
		})
	}
}
