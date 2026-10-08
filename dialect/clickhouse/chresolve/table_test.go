package chresolve_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
)

func observed() *chschema.ObservedTable {
	return &chschema.ObservedTable{
		Engine: "ReplacingMergeTree(version)", OrderBy: "tenant, id", PrimaryKey: "tenant",
		PartitionBy: "toYYYYMM(created_at)", SampleBy: "tenant",
		TTL: "created_at + toIntervalDay(30)", Settings: "index_granularity = 4096",
	}
}

func allOrigins(origin chresolve.Origin) chresolve.Origins {
	return chresolve.Origins{Engine: origin, OrderBy: origin, PrimaryKey: origin,
		PartitionBy: origin, SampleBy: origin, TTL: origin, Settings: origin}
}

func TestOmittedSettingsRetainEveryObservedProperty(t *testing.T) {
	c := qt.New(t)
	current := observed()
	desired := &chschema.DesiredTable{}
	result, err := chresolve.Table(chresolve.Request{Desired: desired, Current: current})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Declared, qt.DeepEquals, *desired)
	c.Assert(result.Prepared, qt.DeepEquals, *current.Desired())
	c.Assert(result.Origins, qt.DeepEquals, allOrigins(chresolve.Observation))
	projected, err := result.Prepared.Observed()
	c.Assert(err, qt.IsNil)
	c.Assert(projected, qt.DeepEquals, current)
	current.TTL, desired.TTL = "mutated", chschema.Setting{State: chschema.Explicit, Value: "mutated"}
	c.Assert(result.Prepared.TTL.Value, qt.Equals, "created_at + toIntervalDay(30)")
	c.Assert(result.Declared.TTL, qt.DeepEquals, chschema.Setting{})
}

func TestCreationDefaultsAreExplicitPlanningInput(t *testing.T) {
	c := qt.New(t)
	key := []string{"tenant", "id"}
	result, err := chresolve.Table(chresolve.Request{Desired: &chschema.DesiredTable{}, Creating: true, CommonKey: key})
	c.Assert(err, qt.IsNil)
	want := (&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tenant, id", PrimaryKey: "tenant, id"}).Desired()
	c.Assert(result.Prepared, qt.DeepEquals, *want)
	c.Assert(result.Declared, qt.DeepEquals, chschema.DesiredTable{})
	c.Assert(result.Origins, qt.DeepEquals, allOrigins(chresolve.CreationRule))
	key[0] = "mutated"
	c.Assert(result.Prepared.OrderBy.Value, qt.Equals, "tenant, id")
}

func TestExplicitDefaultsDoNotRetainObservedClauses(t *testing.T) {
	c := qt.New(t)
	value := chschema.Setting{State: chschema.Default}
	desired := &chschema.DesiredTable{Engine: value, OrderBy: value, PrimaryKey: value,
		PartitionBy: value, SampleBy: value, TTL: value, Settings: value}
	result, err := chresolve.Table(chresolve.Request{Desired: desired, Current: observed(), CommonKey: []string{"id"}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Prepared, qt.DeepEquals, *(&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id"}).Desired())
	c.Assert(result.Declared, qt.DeepEquals, *desired)
	c.Assert(result.Origins, qt.DeepEquals, allOrigins(chresolve.CreationRule))
}

func TestPrimaryKeyResolutionPreservesIntent(t *testing.T) {
	for _, test := range []struct {
		name    string
		primary chschema.Setting
		want    string
		origin  chresolve.Origin
	}{
		{"omitted retains sparse prefix", chschema.Setting{}, "tenant", chresolve.Observation},
		{"default inherits new sorting key", chschema.Setting{State: chschema.Default}, "tenant, id, created_at", chresolve.CreationRule},
		{"empty removes sparse key", chschema.Setting{State: chschema.Explicit}, "", chresolve.Declaration},
		{"explicit preserves chosen prefix", chschema.Setting{State: chschema.Explicit, Value: " tenant, id "}, "tenant, id", chresolve.Declaration},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &chschema.DesiredTable{OrderBy: chschema.Setting{State: chschema.Explicit, Value: "tenant, id, created_at"}, PrimaryKey: test.primary}
			result, err := chresolve.Table(chresolve.Request{Desired: desired, Current: observed()})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Prepared.PrimaryKey, qt.DeepEquals, chschema.Setting{State: chschema.Explicit, Value: test.want})
			c.Assert(result.Origins.PrimaryKey, qt.Equals, test.origin)
			c.Assert(result.Prepared.TTL.Value, qt.Equals, observed().TTL)
			c.Assert(result.Declared, qt.DeepEquals, *desired)
		})
	}
}

func TestExplicitEmptySortingKeyDoesNotUseCommonKeyFallback(t *testing.T) {
	c := qt.New(t)
	result, err := chresolve.Table(chresolve.Request{
		Desired: &chschema.DesiredTable{OrderBy: chschema.Setting{State: chschema.Explicit}}, Creating: true, CommonKey: []string{"id"},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Prepared.OrderBy.Value, qt.Equals, "")
	c.Assert(result.Prepared.PrimaryKey.Value, qt.Equals, "")
	c.Assert(result.Origins.OrderBy, qt.Equals, chresolve.Declaration)
	c.Assert(result.Origins.PrimaryKey, qt.Equals, chresolve.CreationRule)
}

func TestExplicitPropertiesDoNotNeedAnObservation(t *testing.T) {
	c := qt.New(t)
	desired := observed().Desired()
	result, err := chresolve.Table(chresolve.Request{Desired: desired})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Prepared, qt.DeepEquals, *desired)
	c.Assert(result.Declared, qt.DeepEquals, *desired)
	c.Assert(result.Origins, qt.DeepEquals, allOrigins(chresolve.Declaration))
}

func TestEveryPropertyRequiresEvidenceWhenOmittedOnAnExistingTable(t *testing.T) {
	// Derive the corpus from the model so adding a property must also give it
	// resolution and provenance. Missing current state cannot become a default.
	for _, field := range reflect.VisibleFields(reflect.TypeFor[chschema.DesiredTable]()) {
		t.Run(field.Name, func(t *testing.T) {
			c := qt.New(t)
			desired := observed().Desired()
			reflect.ValueOf(desired).Elem().FieldByIndex(field.Index).Set(reflect.ValueOf(chschema.Setting{}))
			result, err := chresolve.Table(chresolve.Request{Desired: desired})
			c.Assert(err, qt.ErrorIs, chresolve.ErrUnknownCurrent)
			c.Assert(result, qt.DeepEquals, chresolve.Result{})
		})
	}
}

func TestInvalidResolutionInputsReturnNoPartialState(t *testing.T) {
	for _, test := range []struct {
		name    string
		request chresolve.Request
	}{
		{"nil declaration", chresolve.Request{Creating: true}},
		{"contradictory creation", chresolve.Request{Desired: &chschema.DesiredTable{}, Current: observed(), Creating: true}},
		{"invalid observation", chresolve.Request{Desired: observed().Desired(), Current: &chschema.ObservedTable{}}},
		{"invalid state", chresolve.Request{Desired: &chschema.DesiredTable{TTL: chschema.Setting{State: "invalid"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := chresolve.Table(test.request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, chresolve.Result{})
		})
	}
}

func TestMergeTreeDefaultsRequireACommonSortingKey(t *testing.T) {
	for _, setting := range []chschema.Setting{{}, {State: chschema.Default}} {
		c := qt.New(t)
		result, err := chresolve.Table(chresolve.Request{Desired: &chschema.DesiredTable{OrderBy: setting}, Creating: true})
		c.Assert(err, qt.ErrorIs, chresolve.ErrMissingSortingKey)
		c.Assert(result, qt.DeepEquals, chresolve.Result{})
	}
}

func TestNonMergeTreeCreationNeedsNoSortingKey(t *testing.T) {
	c := qt.New(t)
	result, err := chresolve.Table(chresolve.Request{
		Desired: &chschema.DesiredTable{Engine: chschema.Setting{State: chschema.Explicit, Value: "Memory"}}, Creating: true,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Prepared, qt.DeepEquals, *(&chschema.ObservedTable{Engine: "Memory"}).Desired())
}

func TestMalformedCommonKeyCannotProducePreparedState(t *testing.T) {
	for _, key := range []string{"id\x00", string([]byte{0xff})} {
		c := qt.New(t)
		result, err := chresolve.Table(chresolve.Request{
			Desired: &chschema.DesiredTable{}, Creating: true, CommonKey: []string{key},
		})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(result, qt.DeepEquals, chresolve.Result{})
	}
}
