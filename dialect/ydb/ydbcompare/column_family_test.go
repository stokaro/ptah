package ydbcompare_test

import (
	"context"
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

func familyComparisonRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID:      ydbschema.Owner,
		Targets: []engine.Target{{Name: "ydb"}},
		Codecs:  append(ydbschema.ColumnFamiliesCodecs(), ydbdiff.ColumnFamiliesCodec()),
		FacetComparisons: []engine.FacetComparison{{
			Target: "ydb", OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
			Kinds:       []schemaext.Kind{ydbschema.ColumnFamiliesKind},
			ChangeKinds: []schemaext.Kind{ydbdiff.ColumnFamiliesKind}, Service: ydbcompare.ColumnFamiliesService{},
		}},
	}))
}

func familyTable() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "docs")
}

func familyFacetRecords(values ...schemaext.Value) []schemaext.FacetRecord {
	if len(values) == 0 {
		return nil
	}
	return []schemaext.FacetRecord{{Subject: familyTable(), Values: must.Must(schemaext.NewFacets(values...))}}
}

// declaredFamilies is the desired value a declaration of families states, or
// none for a declaration that states no family.
func declaredFamilies(families []ydbschema.ColumnFamily) []schemaext.Value {
	if families == nil {
		return nil
	}
	return []schemaext.Value{&ydbschema.DesiredColumnFamilies{Families: families}}
}

// heldFamilies is the observation of a table holding families, or nil for a
// table the read found without families.
func heldFamilies(families []ydbschema.ColumnFamily) *ydbschema.ObservedColumnFamilies {
	if families == nil {
		return nil
	}
	return &ydbschema.ObservedColumnFamilies{Families: families}
}

// familyRequest compares one table both sides hold. The desired source can
// declare families, and the read inspected this table, unless a test says
// otherwise.
func familyRequest(declared, held []ydbschema.ColumnFamily) schemaext.FacetComparisonRequest {
	var current []schemaext.FacetRecord
	if observed := heldFamilies(held); observed != nil {
		current = familyFacetRecords(observed)
	}
	return schemaext.FacetComparisonRequest{
		Target: "ydb", Capabilities: capability.YDB262(), Kinds: []schemaext.Kind{ydbschema.ColumnFamiliesKind},
		Identifiers: identifier.ForDialect("ydb"),
		Owners:      []schemaext.ParentState{{Subject: familyTable(), Desired: true, Current: true}},
		Desired: schemaext.FacetState{
			Records:  familyFacetRecords(declaredFamilies(declared)...),
			Coverage: must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
		},
		Current: schemaext.FacetState{Records: current, Coverage: familyReadCoverage(schemaext.Knowledge{State: schemaext.Complete})},
	}
}

func familyReadCoverage(knowledge schemaext.Knowledge) schemaext.Coverage {
	return must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.ColumnFamiliesKind, Subject: familyTable(), Knowledge: knowledge}}))
}

func familyDesiredCoverage(knowledge schemaext.Knowledge) schemaext.Coverage {
	return must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.ColumnFamiliesKind, Subject: familyTable(), Knowledge: knowledge}}))
}

// TestColumnFamiliesCompareFacets_ReportsWhatTheTableHoldsOnceApplied pins the
// change: its After is what the table holds once the declaration is applied.
// Every family the table holds stays, a setting the declaration leaves out
// keeps the held value, and each column sits where the declaration puts it, so
// a declaration that states no family moves every column to the default
// family. The declaration itself is never replaced.
func TestColumnFamiliesCompareFacets_ReportsWhatTheTableHoldsOnceApplied(t *testing.T) {
	tests := []struct {
		name           string
		declared, held []ydbschema.ColumnFamily
		wantAfter      []ydbschema.ColumnFamily
	}{
		{
			name:      "a setting",
			declared:  []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}},
			held:      []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}, {Name: "cold", Data: "hdd", Compression: "off", Columns: []string{"body"}}},
			wantAfter: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"body"}}, {Name: "default", Compression: "off"}},
		},
		{
			name:      "a column into a family",
			declared:  []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body", "blob"}}},
			held:      []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"body"}}},
			wantAfter: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"blob", "body"}}},
		},
		{
			name:     "a family the table lacks",
			declared: []ydbschema.ColumnFamily{{Name: "warm", CacheMode: "in_memory", Columns: []string{"blob"}}},
			held:     []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", KeepInMemory: true}},
			wantAfter: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", KeepInMemory: true},
				{Name: "warm", CacheMode: "in_memory", Columns: []string{"blob"}}},
		},
		{
			name:      "no family declared",
			held:      []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}},
			wantAfter: []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4"}},
		},
		{
			name:      "families on a table the read found without them",
			declared:  []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}},
			wantAfter: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := familyRequest(test.declared, test.held)

			result, err := familyComparisonRuntime().CompareFacets(t.Context(), input)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Kind, qt.Equals, ydbschema.ColumnFamiliesKind)
			c.Assert(result.Changes[0].Change.Subject, qt.DeepEquals, familyTable())
			change := result.Changes[0].Change.Value.(*ydbdiff.ColumnFamilies)
			c.Assert(change.Before, qt.DeepEquals, heldFamilies(test.held))
			c.Assert(change.After.Families, qt.DeepEquals, test.wantAfter)
			c.Assert(result.Desired.Records, qt.DeepEquals, input.Desired.Records)
		})
	}
}

// TestColumnFamiliesCompareFacets_HoldingWhatIsDeclaredIsNoChange pins
// convergence: order means nothing, and neither does a setting, a family or a
// keep_in_memory the table holds and the declaration leaves out, which a
// cluster's table profile can give every new table.
func TestColumnFamiliesCompareFacets_HoldingWhatIsDeclaredIsNoChange(t *testing.T) {
	tests := []struct {
		name           string
		declared, held []ydbschema.ColumnFamily
	}{
		{
			name:     "families and columns in another order",
			declared: []ydbschema.ColumnFamily{{Name: "warm", Columns: []string{"b", "a"}}, {Name: "cold", Data: "hdd", Columns: []string{"c"}}},
			held:     []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"c"}}, {Name: "warm", Columns: []string{"a", "b"}}},
		},
		{
			name:     "what the declaration leaves out",
			declared: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}},
			held: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "lz4", CacheMode: "regular", Columns: []string{"body"}},
				{Name: "default", Compression: "lz4", KeepInMemory: true}, {Name: "extra", Compression: "lz4"}},
		},
		{
			name:     "a default family stating nothing",
			declared: []ydbschema.ColumnFamily{{Name: "default"}},
			held:     []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", KeepInMemory: true}},
		},
		{name: "no family on either side"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := familyComparisonRuntime().CompareFacets(t.Context(), familyRequest(test.declared, test.held))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestColumnFamiliesCompareFacets_AdoptsWhatAnUndescribingSourceLeavesOut pins
// the source that cannot declare families, an HCL or a DBML document: it is
// silent about where each column sits, so the table keeps its families,
// columns included. They join the effective declaration beside the facets the
// table already declares, so a rebuild keeps them, and nothing is planned.
func TestColumnFamiliesCompareFacets_AdoptsWhatAnUndescribingSourceLeavesOut(t *testing.T) {
	held := []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"body"}}, {Name: "default", Compression: "lz4", KeepInMemory: true}}
	ttl := &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P30D"}}
	tests := []struct {
		name      string
		declared  []schemaext.Value
		wantKinds []schemaext.Kind
	}{
		{name: "a table declaring no facet", wantKinds: []schemaext.Kind{ydbschema.ColumnFamiliesKind}},
		{name: "a table declaring another facet", declared: []schemaext.Value{ttl}, wantKinds: []schemaext.Kind{ydbschema.ColumnFamiliesKind, ydbschema.TTLKind}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := familyRequest(nil, held)
			input.Desired = schemaext.FacetState{Records: familyFacetRecords(test.declared...)}

			result, err := ydbcompare.ColumnFamiliesService{}.CompareFacets(t.Context(), input)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Desired.Records, qt.HasLen, 1)
			adopted, found, err := schemaext.FacetAs[*ydbschema.DesiredColumnFamilies](result.Desired.Records[0].Values, ydbschema.ColumnFamiliesKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(adopted.Families, qt.DeepEquals, held)
			c.Assert(result.Desired.Records[0].Values.Kinds(), qt.DeepEquals, test.wantKinds)
			c.Assert(input.Desired.Records, qt.DeepEquals, familyFacetRecords(test.declared...))
		})
	}
	c := qt.New(t)
	input := familyRequest(nil, nil)
	input.Desired.Coverage = schemaext.Coverage{}
	result, err := familyComparisonRuntime().CompareFacets(t.Context(), input)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Desired.Records, qt.HasLen, 0)
}

// TestColumnFamiliesCompareFacets_ParentLifecycleCarriesTheFamilies pins that
// a table created or removed by the plan carries its families with it.
func TestColumnFamiliesCompareFacets_ParentLifecycleCarriesTheFamilies(t *testing.T) {
	tests := []struct {
		name   string
		owner  schemaext.ParentState
		mutate func(*schemaext.FacetComparisonRequest)
	}{
		{name: "a created table", owner: schemaext.ParentState{Subject: familyTable(), Desired: true},
			mutate: func(r *schemaext.FacetComparisonRequest) { r.Current.Records = nil }},
		{name: "a removed table", owner: schemaext.ParentState{Subject: familyTable(), Current: true},
			mutate: func(r *schemaext.FacetComparisonRequest) { r.Desired.Records = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := familyRequest([]ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}}, []ydbschema.ColumnFamily{{Name: "warm", Columns: []string{"b"}}})
			input.Owners = []schemaext.ParentState{test.owner}
			test.mutate(&input)

			result, err := familyComparisonRuntime().CompareFacets(t.Context(), input)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestColumnFamiliesCompareFacets_UnknownStateIsUndecided pins that a
// declaration met by families the read did not see, or written by a source
// that could not describe them, is never an empty successful comparison: a
// statement built against families nobody saw could undo what nobody read. A
// table the plan creates is no exception.
func TestColumnFamiliesCompareFacets_UnknownStateIsUndecided(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*schemaext.FacetComparisonRequest)
		wantReason string
	}{
		{
			name:       "a catalog that did not inspect families",
			mutate:     func(r *schemaext.FacetComparisonRequest) { r.Current.Coverage = schemaext.Coverage{} },
			wantReason: "the table's column families were not inspected",
		},
		{
			name: "a read that left the table uninspected",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Current.Coverage = familyReadCoverage(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not asked"})
			},
			wantReason: "the table's column families were not inspected",
		},
		{
			name: "a read that could not describe the families",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Current.Coverage = familyReadCoverage(schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "an unread setting"})
			},
			wantReason: "the read found column families it could not describe",
		},
		{
			name: "a source that could not describe the declaration",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Desired.Records = nil
				r.Desired.Coverage = familyDesiredCoverage(schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "x"})
			},
			wantReason: "the desired source could not describe the table's column families",
		},
		{
			name: "a created table whose source could not describe it",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Owners = []schemaext.ParentState{{Subject: familyTable(), Desired: true}}
				r.Desired.Records = nil
				r.Desired.Coverage = familyDesiredCoverage(schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "x"})
			},
			wantReason: "the desired source could not describe the table's column families",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := familyRequest([]ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}}, nil)
			test.mutate(&input)

			result, err := familyComparisonRuntime().CompareFacets(t.Context(), input)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Kind, qt.Equals, ydbschema.ColumnFamiliesKind)
			c.Assert(result.Undecided[0].Reason, qt.Equals, test.wantReason)
		})
	}
}

// TestColumnFamiliesCompareFacets_NothingStatedAgainstAnUnreadTableIsNoChange
// is the control on the test above: a declaration that states no family, or
// only a default family stating nothing, asks nothing a read has to answer,
// so families the read did not see leave it decided.
func TestColumnFamiliesCompareFacets_NothingStatedAgainstAnUnreadTableIsNoChange(t *testing.T) {
	tests := []struct {
		name     string
		declared []ydbschema.ColumnFamily
		coverage schemaext.Coverage
	}{
		{name: "no family against a catalog that did not inspect families", coverage: schemaext.Coverage{}},
		{name: "a plain default family against a catalog that did not inspect families", declared: []ydbschema.ColumnFamily{{Name: "default"}}, coverage: schemaext.Coverage{}},
		{name: "no family against families the read could not describe",
			coverage: familyReadCoverage(schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "an unread setting"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := familyRequest(test.declared, nil)
			input.Current.Coverage = test.coverage

			result, err := familyComparisonRuntime().CompareFacets(t.Context(), input)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestColumnFamiliesCompareFacets_FailurePath pins the refusals that end the
// comparison with a zero result.
func TestColumnFamiliesCompareFacets_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*schemaext.FacetComparisonRequest)
		wantErr error
	}{
		{name: "another target", mutate: func(r *schemaext.FacetComparisonRequest) { r.Target = "postgres" }, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "another kind", mutate: func(r *schemaext.FacetComparisonRequest) { r.Kinds = []schemaext.Kind{ydbschema.TTLKind} }, wantErr: schemaext.ErrInvalidValue},
		{name: "an owner twice", mutate: func(r *schemaext.FacetComparisonRequest) { r.Owners = append(r.Owners, r.Owners[0]) }, wantErr: schemaext.ErrInvalidValue},
		{name: "an owner on neither side", mutate: func(r *schemaext.FacetComparisonRequest) {
			r.Owners = []schemaext.ParentState{{Subject: familyTable()}}
		},
			wantErr: schemaext.ErrInvalidValue},
		{
			name: "an invalid declaration",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Desired.Records = familyFacetRecords(&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold", Compression: "zstd"}}})
			},
			wantErr: schemaext.ErrInvalidValue,
		},
		{
			name: "an invalid observation",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Current.Records = familyFacetRecords(&ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default", Columns: []string{"a"}}}})
			},
			wantErr: schemaext.ErrInvalidValue,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := familyRequest([]ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}}, nil)
			test.mutate(&input)

			result, err := ydbcompare.ColumnFamiliesService{}.CompareFacets(t.Context(), input)

			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
	c := qt.New(t)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := ydbcompare.ColumnFamiliesService{}.CompareFacets(canceled, familyRequest(nil, nil))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.Complete, qt.IsFalse)
}
