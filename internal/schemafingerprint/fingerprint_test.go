package schemafingerprint_test

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/schemafingerprint"
)

// policy is ClickHouse row policy name on table events, filtered by filter
// and applied to roles.
func policy(name, filter string, roles ...string) schemaext.Object {
	return schemaext.Object{Ref: chschema.RowPolicyRef("", "events", name),
		Value: &chschema.DesiredRowPolicy{Filter: new(filter), Roles: chschema.RoleSelection{Names: roles}}}
}

// observedPolicy is policy as a read reports it.
func observedPolicy(name, filter string) schemaext.Object {
	return schemaext.Object{Ref: chschema.RowPolicyRef("", "events", name),
		Value: &chschema.ObservedRowPolicy{Filter: new(filter), Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true}}}
}

func TestDesiredFingerprintBindsObjectsAndCoverage(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	empty := &schemamodel.Database{}
	unknown := must.Must(schemafingerprint.Desired(t.Context(), runtime, empty))
	empty.FeatureCoverage = must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	absent := must.Must(schemafingerprint.Desired(t.Context(), runtime, empty))
	c.Assert(absent, qt.Not(qt.Equals), unknown)
	a := policy("a", "tenant = 1", "audit", "worker")
	b := policy("b", "tenant = 2")
	empty.FeatureObjects = must.Must(schemaext.NewObjects(a, b))
	first := must.Must(schemafingerprint.Desired(t.Context(), runtime, empty))
	slices.Reverse(a.Value.(*chschema.DesiredRowPolicy).Roles.Names)
	empty.FeatureObjects = must.Must(schemaext.NewObjects(b, a))
	c.Assert(must.Must(schemafingerprint.Desired(t.Context(), runtime, empty)), qt.Equals, first)
	a.Value.(*chschema.DesiredRowPolicy).Filter = new("tenant = 3")
	empty.FeatureObjects = must.Must(schemaext.NewObjects(a, b))
	c.Assert(must.Must(schemafingerprint.Desired(t.Context(), runtime, empty)), qt.Not(qt.Equals), first)
	empty.FeatureCoverage = must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: chschema.RowPolicyKind, Subject: a.Ref,
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "source cannot describe this policy"}}}))
	c.Assert(must.Must(schemafingerprint.Desired(t.Context(), runtime, empty)), qt.Not(qt.Equals), first)
}

const testKind schemaext.Kind = "example.org/fingerprint-facet"

type facet struct{ Number int }

func (*facet) Kind() schemaext.Kind     { return testKind }
func (v *facet) Clone() schemaext.Value { return &facet{Number: v.Number} }
func (v *facet) Equal(other schemaext.Value) bool {
	value, ok := other.(*facet)
	return ok && value != nil && *value == *v
}

func selectedRuntime(c *qt.C, version uint32) *engine.Runtime {
	c.Helper()
	encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) }
	codecs := make([]schemaext.Codec, 0, 2)
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		codecs = append(codecs, schemaext.Codec{Prototype: &facet{}, Representation: representation, Version: version,
			Definition: json.RawMessage(`{"type":"object","properties":{"Number":{"type":"integer"}}}`),
			Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(*facet).Clone(), nil },
			Encode:     encode, Canonical: encode, Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*facet](data) },
		})
	}
	runtime, err := engine.New(engine.Provider{ID: "example.org/fingerprint", Codecs: codecs})
	c.Assert(err, qt.IsNil)
	return runtime
}

// Every envelope carrying facets gets an instance, including new envelopes
// added later. This tests source ownership independently of the clone's field
// list and avoids a fixture that silently stops covering a newly added slice.
func populateFacetEnvelopes(value reflect.Value) {
	for _, field := range value.Fields() {
		if field.Kind() != reflect.Slice || field.Type().Elem().Kind() != reflect.Struct {
			continue
		}
		if _, found := field.Type().Elem().FieldByName("Facets"); !found {
			continue
		}
		field.Set(reflect.MakeSlice(field.Type(), 1, 1))
		populateFacetEnvelopes(field.Index(0))
	}
}

func TestFingerprintPreservesAllSourceFacets(t *testing.T) {
	c := qt.New(t)
	runtime := selectedRuntime(c, 1)
	desired := &schemamodel.Database{}
	observed := &catalog.Database{}
	populateFacetEnvelopes(reflect.ValueOf(desired).Elem())
	populateFacetEnvelopes(reflect.ValueOf(observed).Elem())
	desiredSlots, observedSlots := desired.FacetSlots(), observed.FacetSlots()
	c.Assert(len(desiredSlots) >= 14, qt.IsTrue, qt.Commentf("%d desired slots", len(desiredSlots)))
	// Domains, composite types and range types are owner objects, so they add
	// no common facet slot.
	c.Assert(len(observedSlots) >= 13, qt.IsTrue, qt.Commentf("%d observed slots", len(observedSlots)))
	for _, slots := range [][]*schemaext.Facets{desiredSlots, observedSlots} {
		for i, slot := range slots {
			*slot = must.Must(schemaext.NewFacets(&facet{Number: i + 1}))
		}
	}
	first, err := schemafingerprint.Desired(t.Context(), runtime, desired)
	c.Assert(err, qt.IsNil)
	current, err := schemafingerprint.Observed(t.Context(), runtime, observed)
	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.Not(qt.Equals), current)
	for _, slots := range [][]*schemaext.Facets{desiredSlots, observedSlots} {
		for i, slot := range slots {
			values, err := slot.Values()
			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.HasLen, 1)
			c.Assert(values[0].(*facet).Number, qt.Equals, i+1)
		}
	}
	desired.Tables[0].Facets, desired.Fields[0].Facets = desired.Fields[0].Facets, desired.Tables[0].Facets
	c.Assert(must.Must(schemafingerprint.Desired(t.Context(), runtime, desired)), qt.Not(qt.Equals), first)
	c.Assert(must.Must(schemafingerprint.Observed(t.Context(), selectedRuntime(c, 2), observed)), qt.Not(qt.Equals), current)
}

func TestFingerprintRefusesMissingContextRuntimeAndCodecs(t *testing.T) {
	c := qt.New(t)
	runtime := selectedRuntime(c, 1)
	value, err := schemafingerprint.Desired(t.Context(), nil, &schemamodel.Database{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(value, qt.Equals, "")
	var missingContext context.Context
	value, err = schemafingerprint.Desired(missingContext, runtime, &schemamodel.Database{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(value, qt.Equals, "")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	value, err = schemafingerprint.Observed(ctx, runtime, &catalog.Database{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(value, qt.Equals, "")
	source := &catalog.Database{Facets: must.Must(schemaext.NewFacets(&facet{Number: 3}))}
	value, err = schemafingerprint.Observed(t.Context(), must.Must(builtin.New()), source)
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	c.Assert(value, qt.Equals, "")
	values, err := source.Facets.Values()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].(*facet).Number, qt.Equals, 3)
}

// A filtered read carries the planning context outside its scope beside its
// feature objects. The fingerprint encodes it through the registry, as it does
// the feature objects, and binds it: a plan made over one context does not
// match a read whose context changed, and context objects are not mistaken
// for feature objects.
func TestObservedFingerprintBindsThePlanningContext(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	a := observedPolicy("a", "tenant = 1")
	b := observedPolicy("b", "tenant = 2")
	known := must.Must(chschema.RowPolicyCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
	without := must.Must(schemafingerprint.Observed(t.Context(), runtime, &catalog.Database{}))
	read := &catalog.Database{PlanningContextObjects: must.Must(schemaext.NewObjects(a, b)), PlanningContextCoverage: known}

	first, err := schemafingerprint.Observed(t.Context(), runtime, read)

	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.Not(qt.Equals), without)
	c.Assert(read.PlanningContextObjects.Len(), qt.Equals, 2)
	c.Assert(read.PlanningContextCoverage.IsZero(), qt.IsFalse)
	reordered := &catalog.Database{PlanningContextObjects: must.Must(schemaext.NewObjects(b, a)), PlanningContextCoverage: known}
	c.Assert(must.Must(schemafingerprint.Observed(t.Context(), runtime, reordered)), qt.Equals, first)
	unknown := &catalog.Database{PlanningContextObjects: must.Must(schemaext.NewObjects(a, b))}
	c.Assert(must.Must(schemafingerprint.Observed(t.Context(), runtime, unknown)), qt.Not(qt.Equals), first)
	features := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(a, b)), FeatureCoverage: known}
	c.Assert(must.Must(schemafingerprint.Observed(t.Context(), runtime, features)), qt.Not(qt.Equals), first)
	changed := observedPolicy("a", "tenant = 3")
	altered := &catalog.Database{PlanningContextObjects: must.Must(schemaext.NewObjects(changed, b)), PlanningContextCoverage: known}
	c.Assert(must.Must(schemafingerprint.Observed(t.Context(), runtime, altered)), qt.Not(qt.Equals), first)
}

// rowFacts are the row statistics of a read of public.notes.
type rowFacts struct {
	rows    int64
	unknown bool
}

// notes is a read holding one table, public.notes, with the row statistics
// given.
func notes(facts rowFacts) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "notes", Schema: "public", Type: "TABLE", EstimatedRows: facts.rows, RowStatsUnknown: facts.unknown,
		Columns: []catalog.Column{{Name: "id", DataType: "integer"}},
	}}}
}

// A table's row estimate moves with every write and every analyze, and of it
// a plan depends only on whether the table holds rows. Two reads that agree on
// that fingerprint alike, whatever their estimates, and a read keeps its own
// statistics after it is fingerprinted.
func TestObservedFingerprintIgnoresTheRowEstimateOfAPopulatedTable(t *testing.T) {
	runtime := must.Must(builtin.New())
	for _, test := range []struct {
		name        string
		read, other rowFacts
	}{
		{name: "one row, then three", read: rowFacts{rows: 1}, other: rowFacts{rows: 3}},
		{name: "statistics unknown, then counted", read: rowFacts{unknown: true}, other: rowFacts{rows: 5}},
		{name: "counted, then statistics unknown", read: rowFacts{rows: 2}, other: rowFacts{unknown: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			read := notes(test.read)

			got, err := schemafingerprint.Observed(t.Context(), runtime, read)

			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(schemafingerprint.Observed(t.Context(), runtime, notes(test.other))), qt.Equals, got)
			c.Assert(read, qt.DeepEquals, notes(test.read))
		})
	}
}

// Whether a table holds rows decides how a plan builds an index on it, so a
// read in which an empty table holds rows fingerprints apart from the read
// before.
func TestObservedFingerprintBindsWhetherATableHoldsRows(t *testing.T) {
	runtime := must.Must(builtin.New())
	for _, test := range []struct {
		name        string
		read, other rowFacts
	}{
		{name: "empty, then one row", read: rowFacts{}, other: rowFacts{rows: 1}},
		{name: "empty, then statistics unknown", read: rowFacts{}, other: rowFacts{unknown: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := schemafingerprint.Observed(t.Context(), runtime, notes(test.read))

			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(schemafingerprint.Observed(t.Context(), runtime, notes(test.other))), qt.Not(qt.Equals), got)
		})
	}
}
