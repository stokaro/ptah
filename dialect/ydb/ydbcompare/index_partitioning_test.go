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
	"ptah.run/engine"
)

func indexSettingsComparisonRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: ydbschema.Owner, Targets: []engine.Target{{Name: "ydb"}},
		Codecs: append(ydbschema.IndexPartitioningCodecs(), ydbdiff.IndexPartitioningCodec()),
		FacetComparisons: []engine.FacetComparison{{
			Target: "ydb", OwnerKinds: []objectidentity.Kind{objectidentity.KindIndex},
			Kinds:       []schemaext.Kind{ydbschema.IndexPartitioningKind},
			ChangeKinds: []schemaext.Kind{ydbdiff.IndexPartitioningKind}, Service: ydbcompare.IndexPartitioningService{},
		}},
	}))
}

func settingsIndex() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).IndexParts("", "items", "by_kind")
}

func indexSettingsRecords(value schemaext.Value) []schemaext.FacetRecord {
	if value == nil {
		return nil
	}
	return []schemaext.FacetRecord{{Subject: settingsIndex(), Values: must.Must(schemaext.NewFacets(value))}}
}

func indexSettingsCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge) schemaext.Coverage {
	return must.Must(ydbschema.IndexPartitioningCoverage(representation, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned indexes"},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.IndexPartitioningKind, Subject: settingsIndex(), Knowledge: knowledge}}))
}

// indexSettingsRequest compares one index both sides hold, the declaration
// and the read each known.
func indexSettingsRequest(declared, held *ydbschema.IndexPartitioning) schemaext.FacetComparisonRequest {
	var desired, current schemaext.Value
	if declared != nil {
		desired = &ydbschema.DesiredIndexPartitioning{IndexPartitioning: *declared}
	}
	if held != nil {
		current = &ydbschema.ObservedIndexPartitioning{IndexPartitioning: *held}
	}
	complete := schemaext.Knowledge{State: schemaext.Complete}
	return schemaext.FacetComparisonRequest{
		Target: "ydb", Capabilities: capability.YDB262(), Kinds: []schemaext.Kind{ydbschema.IndexPartitioningKind},
		Identifiers: identifier.ForDialect("ydb"),
		Owners:      []schemaext.ParentState{{Subject: settingsIndex(), Desired: true, Current: true}},
		Desired:     schemaext.FacetState{Records: indexSettingsRecords(desired), Coverage: indexSettingsCoverage(schemaext.Desired, complete)},
		Current:     schemaext.FacetState{Records: indexSettingsRecords(current), Coverage: indexSettingsCoverage(schemaext.Observed, complete)},
	}
}

// TestIndexPartitioningCompareFacets_Changes reports an index whose settings,
// the declaration read over what the index holds, differ, carrying both sides;
// and a side YDB could not hold, which the planner refuses with the reason.
func TestIndexPartitioningCompareFacets_Changes(t *testing.T) {
	tests := []struct {
		name           string
		declared, held *ydbschema.IndexPartitioning
	}{
		{name: "a setting declared", declared: &ydbschema.IndexPartitioning{MinPartitions: 3}},
		{name: "a setting moved", declared: &ydbschema.IndexPartitioning{MinPartitions: 4}, held: &ydbschema.IndexPartitioning{MinPartitions: 3}},
		{name: "replicas declared away", declared: &ydbschema.IndexPartitioning{ReadReplicas: "PER_AZ:0"},
			held: &ydbschema.IndexPartitioning{ReadReplicas: "PER_AZ:1"}},
		{name: "a maximum lowered", declared: &ydbschema.IndexPartitioning{MaxPartitions: 4}, held: &ydbschema.IndexPartitioning{MaxPartitions: 9}},
		{name: "a declaration YDB refuses", declared: &ydbschema.IndexPartitioning{BySize: new(false), PartitionSizeMB: 100}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := indexSettingsRequest(test.declared, test.held)

			result, err := indexSettingsComparisonRuntime().CompareFacets(t.Context(), input)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Change.Subject, qt.DeepEquals, settingsIndex())
			change := result.Changes[0].Change.Value.(*ydbdiff.IndexPartitioning)
			c.Assert(change.After.IndexPartitioning, qt.DeepEquals, *test.declared)
			c.Assert(change.Before, qt.DeepEquals, observedIndexOrNil(test.held))
		})
	}
}

func observedIndexOrNil(held *ydbschema.IndexPartitioning) *ydbschema.ObservedIndexPartitioning {
	if held == nil {
		return nil
	}
	return &ydbschema.ObservedIndexPartitioning{IndexPartitioning: *held}
}

// TestIndexPartitioningCompareFacets_NoChange is the control: settings that
// resolve to what the index holds are no change, a setting a declaration
// leaves out keeps the held value, and so does every setting once the
// declaration states none.
func TestIndexPartitioningCompareFacets_NoChange(t *testing.T) {
	tests := []struct {
		name           string
		declared, held *ydbschema.IndexPartitioning
	}{
		{name: "the defaults declared", declared: &ydbschema.IndexPartitioning{BySize: new(true), PartitionSizeMB: 2048, MinPartitions: 1}},
		{name: "read replicas of zero", declared: &ydbschema.IndexPartitioning{ReadReplicas: "PER_AZ:0"}},
		{name: "the same settings", declared: &ydbschema.IndexPartitioning{ByLoad: new(true), MaxPartitions: 9},
			held: &ydbschema.IndexPartitioning{ByLoad: new(true), MaxPartitions: 9}},
		{name: "a setting no longer declared", held: &ydbschema.IndexPartitioning{ReadReplicas: "PER_AZ:1"}},
		{name: "a maximum left out", declared: &ydbschema.IndexPartitioning{MinPartitions: 2},
			held: &ydbschema.IndexPartitioning{MinPartitions: 2, MaxPartitions: 9}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := indexSettingsComparisonRuntime().CompareFacets(t.Context(), indexSettingsRequest(test.declared, test.held))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestIndexPartitioningCompareFacets_UninspectedIsUndecided withholds a
// declaration met by an index whose settings the read did not see: a vector
// index, whose settings the reader does not read, is one.
func TestIndexPartitioningCompareFacets_UninspectedIsUndecided(t *testing.T) {
	c := qt.New(t)
	input := indexSettingsRequest(&ydbschema.IndexPartitioning{MinPartitions: 4}, nil)
	input.Current.Coverage = indexSettingsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "x"})

	result, err := indexSettingsComparisonRuntime().CompareFacets(t.Context(), input)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Subject, qt.DeepEquals, settingsIndex())
}
