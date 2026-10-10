package ydbcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
)

func settingsComparisonRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: ydbschema.Owner, Targets: []engine.Target{{Name: "ydb"}},
		Codecs: append(ydbschema.TablePartitioningCodecs(), ydbdiff.TablePartitioningCodec()),
		FacetComparisons: []engine.FacetComparison{{
			Target: "ydb", OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
			Kinds:       []schemaext.Kind{ydbschema.TablePartitioningKind},
			ChangeKinds: []schemaext.Kind{ydbdiff.TablePartitioningKind}, Service: ydbcompare.TablePartitioningService{},
		}},
	}))
}

func settingsTable() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "items")
}

func settingsRecords(value schemaext.Value) []schemaext.FacetRecord {
	if value == nil {
		return nil
	}
	return []schemaext.FacetRecord{{Subject: settingsTable(), Values: must.Must(schemaext.NewFacets(value))}}
}

func settingsCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge) schemaext.Coverage {
	return must.Must(ydbschema.TablePartitioningCoverage(representation, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables"},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.TablePartitioningKind, Subject: settingsTable(), Knowledge: knowledge}}))
}

// settingsRequest compares one table both sides hold, the declaration and the
// read each known.
func settingsRequest(declared, held *ydbschema.TablePartitioning) schemaext.FacetComparisonRequest {
	var desired, current schemaext.Value
	if declared != nil {
		desired = &ydbschema.DesiredTablePartitioning{TablePartitioning: *declared}
	}
	if held != nil {
		current = &ydbschema.ObservedTablePartitioning{TablePartitioning: *held}
	}
	complete := schemaext.Knowledge{State: schemaext.Complete}
	return schemaext.FacetComparisonRequest{
		Target: "ydb", Capabilities: capability.YDB262(), Kinds: []schemaext.Kind{ydbschema.TablePartitioningKind},
		Identifiers: identifier.ForDialect("ydb"),
		Owners:      []schemaext.ParentState{{Subject: settingsTable(), Desired: true, Current: true}},
		Desired:     schemaext.FacetState{Records: settingsRecords(desired), Coverage: settingsCoverage(schemaext.Desired, complete)},
		Current:     schemaext.FacetState{Records: settingsRecords(current), Coverage: settingsCoverage(schemaext.Observed, complete)},
	}
}

// TestTablePartitioningCompareFacets_Changes reports a table whose settings,
// the declaration read over what the table holds, differ, carrying both
// sides; and a side YDB could not hold, which the planner refuses with the
// reason.
func TestTablePartitioningCompareFacets_Changes(t *testing.T) {
	tests := []struct {
		name           string
		declared, held *ydbschema.TablePartitioning
	}{
		{name: "a new minimum", declared: &ydbschema.TablePartitioning{MinPartitions: 4}},
		{name: "a filter declared away", declared: &ydbschema.TablePartitioning{KeyBloomFilter: new(false)}, held: &ydbschema.TablePartitioning{KeyBloomFilter: new(true)}},
		{name: "a layout over another minimum", declared: &ydbschema.TablePartitioning{UniformPartitions: 4}, held: &ydbschema.TablePartitioning{MinPartitions: 2}},
		{name: "a size YDB refuses", declared: &ydbschema.TablePartitioning{BySize: new(false), PartitionSizeMB: 64}},
		{name: "held settings YDB could not hold", declared: &ydbschema.TablePartitioning{BySize: new(false)},
			held: &ydbschema.TablePartitioning{BySize: new(false), PartitionSizeMB: 64}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := settingsRequest(test.declared, test.held)

			result, err := settingsComparisonRuntime().CompareFacets(t.Context(), input)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			change := result.Changes[0].Change.Value.(*ydbdiff.TablePartitioning)
			c.Assert(change.After.TablePartitioning, qt.DeepEquals, *test.declared)
			c.Assert(change.Before, qt.DeepEquals, observedOrNil(test.held))
			c.Assert(result.Desired.Records, qt.DeepEquals, input.Desired.Records)
		})
	}
}

func observedOrNil(held *ydbschema.TablePartitioning) *ydbschema.ObservedTablePartitioning {
	if held == nil {
		return nil
	}
	return &ydbschema.ObservedTablePartitioning{TablePartitioning: *held}
}

// TestTablePartitioningCompareFacets_NoChange reads the same settings as no
// change: a setting left out keeps what the table holds, so a declaration
// stating nothing changes nothing, and a starting layout counts through the
// minimum it gives a new table.
func TestTablePartitioningCompareFacets_NoChange(t *testing.T) {
	tests := []struct {
		name           string
		declared, held *ydbschema.TablePartitioning
	}{
		{name: "nothing declared over tuned settings", held: &ydbschema.TablePartitioning{ByLoad: new(true), MaxPartitions: 9}},
		{name: "a declaration stating nothing", declared: &ydbschema.TablePartitioning{}, held: &ydbschema.TablePartitioning{ByLoad: new(true)}},
		{name: "the defaults declared", declared: &ydbschema.TablePartitioning{BySize: new(true), MinPartitions: 1, ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(false)}},
		{name: "a layout read back as its minimum", declared: &ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10"}}}, held: &ydbschema.TablePartitioning{MinPartitions: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := settingsComparisonRuntime().CompareFacets(t.Context(), settingsRequest(test.declared, test.held))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestTablePartitioningCompareFacets_UnknownStateIsUndecided withholds a
// declaration met by settings the read did not see, or written by a source
// that could not describe them, on a table the plan creates too. A
// declaration stating nothing asks nothing a read has to answer.
func TestTablePartitioningCompareFacets_UnknownStateIsUndecided(t *testing.T) {
	declared := &ydbschema.TablePartitioning{MinPartitions: 4}
	tests := []struct {
		name       string
		declared   *ydbschema.TablePartitioning
		mutate     func(*schemaext.FacetComparisonRequest)
		wantReason []string
	}{
		{name: "a catalog that did not inspect them", declared: declared,
			mutate:     func(r *schemaext.FacetComparisonRequest) { r.Current.Coverage = schemaext.Coverage{} },
			wantReason: []string{"the table's settings were not inspected"}},
		{name: "a read that left the table uninspected", declared: declared,
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Current.Coverage = settingsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "x"})
			},
			wantReason: []string{"the table's settings were not inspected"}},
		{name: "a read that could not describe them", declared: declared,
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Current.Coverage = settingsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "x"})
			},
			wantReason: []string{"the read found settings it could not describe"}},
		{name: "a created table whose source could not describe them",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Owners = []schemaext.ParentState{{Subject: settingsTable(), Desired: true}}
				r.Desired.Coverage = settingsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "x"})
			},
			wantReason: []string{"the desired source could not describe the table's settings"}},
		{name: "nothing declared against an uninspected table",
			mutate: func(r *schemaext.FacetComparisonRequest) { r.Current.Coverage = schemaext.Coverage{} }},
		{name: "a declaration stating nothing against an uninspected table", declared: &ydbschema.TablePartitioning{},
			mutate: func(r *schemaext.FacetComparisonRequest) { r.Current.Coverage = schemaext.Coverage{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := settingsRequest(test.declared, nil)
			test.mutate(&input)

			result, err := settingsComparisonRuntime().CompareFacets(t.Context(), input)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			var reasons []string
			for _, undecided := range result.Undecided {
				reasons = append(reasons, undecided.Reason)
			}
			c.Assert(reasons, qt.DeepEquals, test.wantReason)
		})
	}
}

// TestTablePartitioningCompareFacets_FailurePath ends the comparison on an
// invalid request with a zero result.
func TestTablePartitioningCompareFacets_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*schemaext.FacetComparisonRequest)
		wantErr error
	}{
		{name: "another target", mutate: func(r *schemaext.FacetComparisonRequest) { r.Target = "postgres" }, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "another kind", mutate: func(r *schemaext.FacetComparisonRequest) { r.Kinds = []schemaext.Kind{ydbschema.TTLKind} }, wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid observation", mutate: func(r *schemaext.FacetComparisonRequest) {
			r.Current.Records = settingsRecords(&ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{UniformPartitions: 4}})
		}, wantErr: schemaext.ErrInvalidValue},
		{name: "an invalid declaration", mutate: func(r *schemaext.FacetComparisonRequest) {
			r.Desired.Records = settingsRecords(&ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ReadReplicas: "x"}})
		}, wantErr: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := settingsRequest(&ydbschema.TablePartitioning{MinPartitions: 4}, nil)
			test.mutate(&input)

			result, err := ydbcompare.TablePartitioningService{}.CompareFacets(t.Context(), input)

			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}
