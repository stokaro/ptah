package ydbcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

func vectorSubject() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).IndexParts("", "docs", "by_emb")
}

// builtSettings is the settings of the index the tests below hold.
func builtSettings() ydbschema.VectorSettings {
	return ydbschema.VectorSettings{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}
}

func vectorRecords(value schemaext.Value) []schemaext.FacetRecord {
	return []schemaext.FacetRecord{{Subject: vectorSubject(), Values: must.Must(schemaext.NewFacets(value))}}
}

func complete(representation schemaext.Representation) schemaext.Coverage {
	return must.Must(ydbschema.VectorIndexCoverage(representation, schemaext.Knowledge{State: schemaext.Complete}, nil))
}

// vectorRequest compares index by_emb, which both sides hold, under complete
// coverage on both sides.
func vectorRequest(desired, current []schemaext.FacetRecord) schemaext.FacetComparisonRequest {
	return schemaext.FacetComparisonRequest{
		Target: "ydb", Capabilities: capability.YDB262(), Kinds: []schemaext.Kind{ydbschema.VectorIndexKind},
		Identifiers: identifier.ForDialect("ydb"),
		Owners:      []schemaext.ParentState{{Subject: vectorSubject(), Desired: true, Current: true}},
		Desired:     schemaext.FacetState{Records: desired, Coverage: complete(schemaext.Desired)},
		Current:     schemaext.FacetState{Records: current, Coverage: complete(schemaext.Observed)},
	}
}

// TestVectorIndexService_KeepsAnIndexBuiltAsDeclared plans nothing for an
// index built with the declared settings, for an index of another kind on
// both sides, and for an index whose kind changes away from a vector index,
// which the common comparison rebuilds.
func TestVectorIndexService_KeepsAnIndexBuiltAsDeclared(t *testing.T) {
	tests := []struct {
		name    string
		desired []schemaext.FacetRecord
		current []schemaext.FacetRecord
	}{
		{name: "the same settings", desired: vectorRecords(new(ydbschema.DesiredVectorIndex(builtSettings()))),
			current: vectorRecords(new(ydbschema.ObservedVectorIndex(builtSettings())))},
		{name: "no vector index on either side"},
		{name: "a vector index made another kind", current: vectorRecords(new(ydbschema.ObservedVectorIndex(builtSettings())))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := ydbcompare.VectorIndexService{}.CompareFacets(c.Context(), vectorRequest(test.desired, test.current))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Desired.Records, qt.HasLen, len(test.desired))
		})
	}
}

// TestVectorIndexService_ChangesAnIndexBuiltOtherwise records a change for
// each setting that differs, for 25.1's index built without levels and
// clusters, for a declaration stating none of its settings, and for vector
// settings declared on an index the read found without any. The planner
// builds the index again or refuses the declaration.
func TestVectorIndexService_ChangesAnIndexBuiltOtherwise(t *testing.T) {
	with := func(change func(*ydbschema.VectorSettings)) *ydbschema.DesiredVectorIndex {
		settings := builtSettings()
		change(&settings)
		return new(ydbschema.DesiredVectorIndex(settings))
	}
	held := new(ydbschema.ObservedVectorIndex(builtSettings()))
	unsized := &ydbschema.ObservedVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3}
	tests := []struct {
		name    string
		desired *ydbschema.DesiredVectorIndex
		current []schemaext.FacetRecord
		before  *ydbschema.ObservedVectorIndex
	}{
		{name: "another distance", desired: with(func(s *ydbschema.VectorSettings) { s.Distance = "euclidean" }), current: vectorRecords(held), before: held},
		{name: "a similarity of the same name", desired: with(func(s *ydbschema.VectorSettings) { s.Distance, s.Similarity = "", "cosine" }),
			current: vectorRecords(held), before: held},
		{name: "another element type", desired: with(func(s *ydbschema.VectorSettings) { s.VectorType = "int8" }), current: vectorRecords(held), before: held},
		{name: "another dimension", desired: with(func(s *ydbschema.VectorSettings) { s.Dimension = 4 }), current: vectorRecords(held), before: held},
		{name: "another depth", desired: with(func(s *ydbschema.VectorSettings) { s.Levels = 3 }), current: vectorRecords(held), before: held},
		{name: "another width", desired: with(func(s *ydbschema.VectorSettings) { s.Clusters = 64 }), current: vectorRecords(held), before: held},
		{name: "25.1's index without levels and clusters", desired: new(ydbschema.DesiredVectorIndex(builtSettings())),
			current: vectorRecords(unsized), before: unsized},
		{name: "a declaration stating none", desired: &ydbschema.DesiredVectorIndex{}, current: vectorRecords(held), before: held},
		{name: "settings on an index of another kind", desired: new(ydbschema.DesiredVectorIndex(builtSettings()))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := ydbcompare.VectorIndexService{}.CompareFacets(c.Context(), vectorRequest(vectorRecords(test.desired), test.current))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Change.Subject, qt.DeepEquals, vectorSubject())
			c.Assert(result.Changes[0].Change.Value, qt.DeepEquals, &ydbdiff.VectorIndex{Before: test.before, After: test.desired})
		})
	}
}

// TestVectorIndexService_AdoptsWhatASourceCannotDescribe keeps the settings an
// index holds where the source does not describe vector settings, as an HCL
// document that states none does: they become the effective declaration, and
// nothing is planned.
func TestVectorIndexService_AdoptsWhatASourceCannotDescribe(t *testing.T) {
	c := qt.New(t)
	request := vectorRequest(nil, vectorRecords(new(ydbschema.ObservedVectorIndex(builtSettings()))))
	request.Desired.Coverage = schemaext.Coverage{}

	result, err := ydbcompare.VectorIndexService{}.CompareFacets(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Desired.Records, qt.HasLen, 1)
	adopted, _, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](result.Desired.Records[0].Values, ydbschema.VectorIndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(adopted, qt.DeepEquals, new(ydbschema.DesiredVectorIndex(builtSettings())))
}

// TestVectorIndexService_LeavesWhatTheReadDidNotInspectUndecided reports an
// index whose settings the read did not inspect as undecided, rather than as
// one to build again.
func TestVectorIndexService_LeavesWhatTheReadDidNotInspectUndecided(t *testing.T) {
	c := qt.New(t)
	request := vectorRequest(vectorRecords(new(ydbschema.DesiredVectorIndex(builtSettings()))), nil)
	request.Current.Coverage = must.Must(ydbschema.VectorIndexCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.VectorIndexKind, Subject: vectorSubject(),
			Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}}}))

	result, err := ydbcompare.VectorIndexService{}.CompareFacets(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Equals, "YDB vector index settings were not fully inspected")
}
