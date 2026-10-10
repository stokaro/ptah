package featurejson_test

import (
	"errors"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/internal/featurejson"
	"ptah.run/migration/schemadiff/difftypes"
)

// pointed holds every feature value behind a pointer, and a feature object on
// its own.
type pointed struct {
	Facets    *schemaext.Facets         `json:"facets"`
	NilFacets *schemaext.Facets         `json:"nil_facets"`
	Changes   []*schemaext.ChangeRecord `json:"changes"`
	Object    schemaext.Object          `json:"object"`
	NoObject  schemaext.Object          `json:"no_object"`
	Objects   *schemaext.Objects        `json:"objects"`
	Coverage  *schemaext.Coverage       `json:"coverage"`
}

// A pointer to a feature value carries the value's methods, including the one
// that refuses default encoding, so it is projected like the value itself.
// A single feature object is written as one envelope, and as null when zero.
func TestMarshal_RoundTripsFeatureValuesBehindPointers(t *testing.T) {
	c := qt.New(t)
	table := must.Must(schemaext.NewFacets(ttlChange().Value.(*chdiff.Table).After))
	object := ydbcoordination.DesiredObject("", "locks", "", ydbcoordination.Spec{ReadConsistencyMode: "strict"})
	objects := must.Must(schemaext.NewObjects(object))
	coverage := must.Must(ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	record := ttlChange()
	document := pointed{Facets: &table, Changes: []*schemaext.ChangeRecord{&record, nil}, Object: object, Objects: &objects, Coverage: &coverage}

	data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, document)

	c.Assert(err, qt.IsNil)
	var read pointed
	c.Assert(featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, data, &read), qt.IsNil)
	c.Assert(read.Facets.Equal(table), qt.IsTrue)
	c.Assert(read.NilFacets, qt.IsNil)
	c.Assert(read.Changes, qt.HasLen, 2)
	c.Assert(*read.Changes[0], qt.DeepEquals, record)
	c.Assert(read.Changes[1], qt.IsNil)
	c.Assert(read.Object.Ref, qt.DeepEquals, object.Ref)
	c.Assert(read.Object.Value.Equal(object.Value), qt.IsTrue)
	c.Assert(read.NoObject.Value, qt.IsNil)
	c.Assert(read.Objects.Len(), qt.Equals, 1)
	c.Assert(read.Coverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("", "locks")).State, qt.Equals, schemaext.Complete)
	c.Assert(string(data), qt.Contains, `"no_object":null`)
}

// A shape the projection cannot mirror is refused when the type is built,
// before anything is encoded, with an error that names it: the relation types
// have no JSON form at all, and a type that holds feature data through itself
// would need a recursive wire type.
func TestMarshal_RefusesShapesWithoutAWireForm_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
		want  string
	}{
		{"a relation value", struct {
			Relation schemaext.RelationValue `json:"relation"`
		}{}, `.*schemaext.RelationValue has no JSON form`},
		{"a relation snapshot behind a pointer", struct {
			Snapshot *schemaext.RelationSnapshot `json:"snapshot"`
		}{}, `.*schemaext.RelationSnapshot has no JSON form`},
		{"a recursive type holding facets", tree{}, `.*featurejson_test.tree holds feature data through a recursive type`},
		{"a cycle through two types", cycleA{}, `.*featurejson_test.cycleA holds feature data through a recursive type`},
		{"an unexported field encoding/json reads", struct {
			hiddenInner `json:"hidden"`
			Facets      schemaext.Facets `json:"facets,omitzero"`
		}{}, `.*has the unexported field hiddenInner beside feature data`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, test.value)

			c.Assert(err, qt.ErrorIs, featurejson.ErrUnsupportedShape)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(data, qt.IsNil)
		})
	}
}

type tree struct {
	Children []tree           `json:"children"`
	Facets   schemaext.Facets `json:"facets,omitzero"`
}

type cycleA struct {
	B      *cycleB          `json:"b"`
	Facets schemaext.Facets `json:"facets,omitzero"`
}

type cycleB struct {
	A *cycleA `json:"a"`
}

type hiddenInner struct {
	X int `json:"x"`
}

// chain is recursive and holds no feature data.
type chain struct {
	Next  *chain `json:"next"`
	Value int    `json:"value"`
}

// A recursive type without feature data is written as encoding/json writes
// it, beside feature data and on its own.
func TestMarshal_EncodesARecursiveTypeWithoutFeatureData(t *testing.T) {
	c := qt.New(t)
	document := struct {
		Chain  chain                    `json:"chain"`
		Change []schemaext.ChangeRecord `json:"changes"`
	}{Chain: chain{Value: 1, Next: &chain{Value: 2}}, Change: []schemaext.ChangeRecord{ttlChange()}}

	data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, document)

	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Contains, `{"chain":{"next":{"next":null,"value":2},"value":1},"changes":[`)
}

// An embedded struct with a JSON name is an ordinary field to encoding/json,
// so it can sit beside feature data; only promotion has no wire form.
func TestMarshal_EmbeddedStructWithAName(t *testing.T) {
	c := qt.New(t)
	table := must.Must(schemaext.NewFacets(ttlChange().Value.(*chdiff.Table).After))
	type named struct {
		Inner  `json:"inner"`
		Facets schemaext.Facets `json:"facets"`
	}

	data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, named{Inner: Inner{X: 1}, Facets: table})

	c.Assert(err, qt.IsNil)
	var read named
	c.Assert(featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, data, &read), qt.IsNil)
	c.Assert(read.Inner, qt.Equals, Inner{X: 1})
	c.Assert(read.Facets.Equal(table), qt.IsTrue)
	c.Assert(string(data), qt.Contains, `{"inner":{"x":1},"facets":[`)
}

// interfaced has an interface field encoding/json asks IsZero of, beside
// feature data.
type interfaced struct {
	Check   zeroer                   `json:"check,omitzero"`
	Changes []schemaext.ChangeRecord `json:"changes"`
}

// omitzero on an interface field follows encoding/json: a nil interface and a
// typed nil pointer are zero without calling IsZero, and a value is asked.
func TestMarshal_OmitZeroOnAnInterfaceField(t *testing.T) {
	for _, test := range []struct {
		name  string
		check zeroer
		want  string
	}{
		{"nil", nil, `{"changes":null}`},
		{"a typed nil pointer", (*pointerZero)(nil), `{"changes":null}`},
		{"zero by its method", valueZero{Note: "n"}, `{"changes":null}`},
		{"not zero", &pointerZero{N: 1}, `{"check":{"n":1,"note":""},"changes":null}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, interfaced{Check: test.check})

			c.Assert(err, qt.IsNil)
			c.Assert(string(data), qt.Equals, test.want)
		})
	}
}

// Unmarshal replaces the target, also for a type without feature data, which
// encoding/json would merge into.
func TestUnmarshal_ReplacesTheTarget(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
	}{
		{"a document without feature data", `{"name":"new"}`},
		{"a document with feature data", `{"name":"new","changes":null}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plain := struct {
				Name  string `json:"name"`
				Count int    `json:"count"`
			}{Name: "old", Count: 5}
			featured := interfaced{Changes: []schemaext.ChangeRecord{ttlChange()}}

			c.Assert(featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, []byte(test.data), &plain), qt.IsNil)
			c.Assert(featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, []byte(test.data), &featured), qt.IsNil)

			c.Assert(plain.Name, qt.Equals, "new")
			c.Assert(plain.Count, qt.Equals, 0)
			c.Assert(featured.Changes, qt.IsNil)
		})
	}
}

// A document that fails partway leaves the target as it was, also for a type
// without feature data, which encoding/json writes into up to the error.
func TestUnmarshal_LeavesAPlainTargetOnFailure(t *testing.T) {
	c := qt.New(t)
	target := struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}{Name: "old", Count: 5}

	err := featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, []byte(`{"name":"new","count":"five"}`), &target)

	c.Assert(err, qt.ErrorMatches, `.*cannot unmarshal string into Go struct field .*count of type int`)
	c.Assert(target.Name, qt.Equals, "old")
	c.Assert(target.Count, qt.Equals, 5)
}

// envelopeCorruption damages one envelope of a document with an unknown field
// or a repeated key, which encoding/json accepts and
// [schemaext.Registry.Unmarshal] refuses.
type envelopeCorruption struct {
	name, find, replace, want string
}

// Change envelopes are read as strictly as [schemaext.Registry.Unmarshal]
// reads them, and the target is left as it was.
func TestUnmarshal_ReadsChangeEnvelopesStrictly_FailurePath(t *testing.T) {
	document := string(must.Must(featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, diffWithChanges())))
	for _, test := range []envelopeCorruption{
		{"an unknown field in the envelope", `"value":{"format":1,`, `"value":{"format":1,"extra":true,`, `.*unknown field "extra".*`},
		{"a repeated key in the envelope", `"value":{"format":1,`, `"value":{"format":1,"format":1,`, `.*duplicate object key "format".*`},
		{"an unknown field beside the subject", `{"subject":{`, `{"extra":true,"subject":{`, `.*unknown field "extra".*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(strings.Contains(document, test.find), qt.IsTrue, qt.Commentf("%s is not in %s", test.find, document))
			target := changesDocument{FeatureChanges: []schemaext.ChangeRecord{coordinationChange()}}

			err := featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, []byte(strings.Replace(document, test.find, test.replace, 1)), &target)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(target.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{coordinationChange()})
		})
	}
}

// Facet envelopes and coverage documents are read as strictly, and the target
// is left as it was.
func TestUnmarshal_ReadsSchemaEnvelopesStrictly_FailurePath(t *testing.T) {
	document := string(must.Must(featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, &schemamodel.Database{
		Tables:          []schemamodel.Table{{Name: "events", Facets: must.Must(schemaext.NewFacets(ttlChange().Value.(*chdiff.Table).After))}},
		FeatureCoverage: must.Must(ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	})))
	for _, test := range []envelopeCorruption{
		{"an unknown field in a facet envelope", `"facets":[{"kind":`, `"facets":[{"extra":true,"kind":`, `.*unknown field "extra".*`},
		{"a repeated key in a facet envelope", `"facets":[{"kind":`, `"facets":[{"kind":"x","kind":`, `.*duplicate object key "kind".*`},
		{"an unknown field in a coverage document", `"feature_coverage":{"format":1,`, `"feature_coverage":{"format":1,"extra":true,`, `.*unknown field "extra".*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(strings.Contains(document, test.find), qt.IsTrue, qt.Commentf("%s is not in %s", test.find, document))
			target := schemamodel.Database{Tables: []schemamodel.Table{{Name: "kept"}}}

			err := featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, []byte(strings.Replace(document, test.find, test.replace, 1)), &target)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(target.Tables, qt.DeepEquals, []schemamodel.Table{{Name: "kept"}})
		})
	}
}

// An unknown key outside the feature data is read as encoding/json reads it.
func TestUnmarshal_AcceptsUnknownKeysOutsideFeatureData(t *testing.T) {
	c := qt.New(t)
	data := strings.Replace(string(must.Must(featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, diffWithChanges()))),
		`{"feature_changes":`, `{"extra":true,"feature_changes":`, 1)
	var read changesDocument

	err := featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, []byte(data), &read)

	c.Assert(err, qt.IsNil)
	c.Assert(read.FeatureChanges, qt.DeepEquals, diffWithChanges().FeatureChanges)
}

// Coverage is checked against the representation the caller reads, as facets
// and feature objects are: an observed account read as a desired one would
// claim knowledge of the wrong side.
func TestUnmarshal_RefusesCoverageOfTheOtherRepresentation_FailurePath(t *testing.T) {
	c := qt.New(t)
	observed := &catalog.Database{FeatureCoverage: must.Must(ydbcoordination.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))}
	data := must.Must(featurejson.Marshal(t.Context(), codecs(), schemaext.Observed, observed))
	var asObserved, asDesired catalog.Database

	c.Assert(featurejson.Unmarshal(t.Context(), codecs(), schemaext.Observed, data, &asObserved), qt.IsNil)
	err := featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, data, &asDesired)

	c.Assert(asObserved.FeatureCoverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("", "locks")).State, qt.Equals, schemaext.Complete)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*coverage is recorded as "observed", and the document is read as "desired"`)
	c.Assert(asDesired.FeatureCoverage.IsZero(), qt.IsTrue)
}

// The document types the commands write are built without refusal.
func TestMarshal_BuildsTheShippingDocumentTypes(t *testing.T) {
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		value          any
	}{
		{"a schema diff", schemaext.Desired, &difftypes.SchemaDiff{}},
		{"a desired schema", schemaext.Desired, &schemamodel.Database{}},
		{"an observed catalog", schemaext.Observed, &catalog.Database{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := featurejson.Marshal(t.Context(), codecs(), test.representation, test.value)

			c.Assert(err, qt.IsNil)
		})
	}
}

// Calls share the wire types they build, so concurrent calls over one type
// agree with a call made alone.
func TestMarshal_ConcurrentCallsAgree(t *testing.T) {
	c := qt.New(t)
	want := string(must.Must(featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, diffWithChanges())))
	results := make([]string, 8)
	errs := make([]error, len(results))
	var group sync.WaitGroup
	for i := range results {
		group.Go(func() {
			data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, diffWithChanges())
			results[i], errs[i] = string(data), err
		})
	}
	group.Wait()

	c.Assert(errors.Join(errs...), qt.IsNil)
	for _, got := range results {
		c.Assert(got, qt.Equals, want)
	}
}

func BenchmarkMarshal_SchemaDiff(b *testing.B) {
	registry := codecs()
	diff := diffWithChanges()
	b.ReportAllocs()
	for b.Loop() {
		must.Must(featurejson.Marshal(b.Context(), registry, schemaext.Desired, diff))
	}
}
