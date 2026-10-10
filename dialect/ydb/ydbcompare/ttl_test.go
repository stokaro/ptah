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

func ttlComparisonRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID:      ydbschema.Owner,
		Targets: []engine.Target{{Name: "ydb"}},
		Codecs:  append(ydbschema.TTLCodecs(), ydbdiff.TTLCodec()),
		FacetComparisons: []engine.FacetComparison{{
			Target: "ydb", OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
			Kinds: []schemaext.Kind{ydbschema.TTLKind}, ChangeKinds: []schemaext.Kind{ydbdiff.TTLKind}, Service: ydbcompare.TTLService{},
		}},
	}))
}

func ttlTable() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "sessions")
}

func ttlRecords(value schemaext.Value) []schemaext.FacetRecord {
	if value == nil {
		return nil
	}
	return []schemaext.FacetRecord{{Subject: ttlTable(), Values: must.Must(schemaext.NewFacets(value))}}
}

// request compares one table both sides hold. The desired source can declare a
// policy, and the read inspected this table, unless a test says otherwise.
func ttlRequest(desired, current *ydbschema.TTL) schemaext.FacetComparisonRequest {
	var desiredValue, currentValue schemaext.Value
	if desired != nil {
		desiredValue = &ydbschema.DesiredTTL{Policy: *desired}
	}
	if current != nil {
		currentValue = &ydbschema.ObservedTTL{Policy: *current}
	}
	return schemaext.FacetComparisonRequest{
		Target: "ydb", Capabilities: capability.YDB262(), Kinds: []schemaext.Kind{ydbschema.TTLKind},
		Identifiers: identifier.ForDialect("ydb"),
		Owners:      []schemaext.ParentState{{Subject: ttlTable(), Desired: true, Current: true}},
		Desired: schemaext.FacetState{
			Records:  ttlRecords(desiredValue),
			Coverage: must.Must(ydbschema.TTLCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
		},
		Current: schemaext.FacetState{Records: ttlRecords(currentValue), Coverage: ttlReadCoverage(schemaext.Knowledge{State: schemaext.Complete})},
	}
}

func ttlReadCoverage(knowledge schemaext.Knowledge) schemaext.Coverage {
	return must.Must(ydbschema.TTLCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.TTLKind, Subject: ttlTable(), Knowledge: knowledge}}))
}

func TestTTLCompareFacets_ReportsEveryTransition(t *testing.T) {
	tests := []struct {
		name             string
		desired, current *ydbschema.TTL
		wantBefore       *ydbschema.ObservedTTL
		wantAfter        *ydbschema.DesiredTTL
	}{
		{
			name: "adding a policy", desired: &ydbschema.TTL{Column: "created_at", Interval: "P30D"},
			wantAfter: &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P30D"}},
		},
		{
			// A source that can declare a policy and declares none requests
			// none, which is what makes the removal plannable.
			name: "removing a policy", current: &ydbschema.TTL{Column: "created_at", Interval: "P30D"},
			wantBefore: &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P30D"}},
		},
		{
			name: "changing a parameter", desired: &ydbschema.TTL{Column: "created_at", Interval: "P30D"}, current: &ydbschema.TTL{Column: "created_at", Interval: "P60D"},
			wantBefore: &ydbschema.ObservedTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P60D"}},
			wantAfter:  &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P30D"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := ttlRequest(test.desired, test.current)
			result, err := ttlComparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Kind, qt.Equals, ydbschema.TTLKind)
			c.Assert(result.Changes[0].Change.Subject, qt.DeepEquals, ttlTable())
			change := result.Changes[0].Change.Value.(*ydbdiff.TTL)
			c.Assert(change.Before, qt.DeepEquals, test.wantBefore)
			c.Assert(change.After, qt.DeepEquals, test.wantAfter)
			c.Assert(result.Desired.Records, qt.DeepEquals, input.Desired.Records)
		})
	}
}

// TestTTLCompareFacets_ReadsRewrittenValuesAsValues pins convergence on the
// spellings the server stores: no change for two spellings of one policy.
// YDB keeps whole seconds and shows them in days, hours, minutes and seconds.
func TestTTLCompareFacets_ReadsRewrittenValuesAsValues(t *testing.T) {
	tests := []struct {
		name             string
		desired, current *ydbschema.TTL
	}{
		{name: "thirty days", desired: &ydbschema.TTL{Column: "created_at", Interval: "P30D"}, current: &ydbschema.TTL{Column: "created_at", Interval: "PT720H"}},
		{name: "a week", desired: &ydbschema.TTL{Column: "created_at", Interval: "P1W"}, current: &ydbschema.TTL{Column: "created_at", Interval: "P7D"}},
		{name: "one day", desired: &ydbschema.TTL{Column: "created_at", Interval: "P1D"}, current: &ydbschema.TTL{Column: "created_at", Interval: "PT24H"}},
		{name: "neither side has one"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := ttlComparisonRuntime().CompareFacets(t.Context(), ttlRequest(test.desired, test.current))
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestTTLCompareFacets_LeavesAnUndescribedPolicyAlone pins the source that cannot
// declare a policy: it says nothing about TTL, the live policy is adopted into
// the effective declaration so a rebuild keeps it, and no change is planned.
func TestTTLCompareFacets_LeavesAnUndescribedPolicyAlone(t *testing.T) {
	c := qt.New(t)

	input := ttlRequest(nil, &ydbschema.TTL{Column: "created_at", Interval: "P30D"})
	input.Desired.Coverage = schemaext.Coverage{}
	result, err := ttlComparisonRuntime().CompareFacets(t.Context(), input)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Desired.Records, qt.HasLen, 1)
	adopted, found, err := schemaext.FacetAs[*ydbschema.DesiredTTL](result.Desired.Records[0].Values, ydbschema.TTLKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(adopted.Policy, qt.DeepEquals, ydbschema.TTL{Column: "created_at", Interval: "P30D"})
}

// TestTTLCompareFacets_ParentLifecycleCarriesThePolicy pins that a table created
// or removed by the plan carries its policy with it: no change is reported.
func TestTTLCompareFacets_ParentLifecycleCarriesThePolicy(t *testing.T) {
	tests := []struct {
		name   string
		owner  schemaext.ParentState
		mutate func(*schemaext.FacetComparisonRequest)
	}{
		{name: "a created table", owner: schemaext.ParentState{Subject: ttlTable(), Desired: true},
			mutate: func(r *schemaext.FacetComparisonRequest) { r.Current.Records = nil }},
		{name: "a removed table", owner: schemaext.ParentState{Subject: ttlTable(), Current: true},
			mutate: func(r *schemaext.FacetComparisonRequest) { r.Desired.Records = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := ttlRequest(&ydbschema.TTL{Column: "a", Interval: "P1D"}, &ydbschema.TTL{Column: "b", Interval: "P1D"})
			input.Owners = []schemaext.ParentState{test.owner}
			test.mutate(&input)
			result, err := ttlComparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestTTLCompareFacets_UnknownStateIsUndecided pins that an uninspected live
// policy, or a desired source that met one it cannot represent, never becomes
// an empty successful comparison.
func TestTTLCompareFacets_UnknownStateIsUndecided(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*schemaext.FacetComparisonRequest)
		wantReason string
	}{
		{
			name:       "the read did not inspect the table",
			mutate:     func(r *schemaext.FacetComparisonRequest) { r.Current.Coverage = schemaext.Coverage{} },
			wantReason: "the YDB TTL was not inspected",
		},
		{
			name: "the read could not represent the table's policy",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Current.Coverage = ttlReadCoverage(schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "an unknown parameter"})
			},
			wantReason: "the read found a YDB TTL it could not describe",
		},
		{
			name: "the source could not represent the declaration",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Desired.Records = nil
				r.Desired.Coverage = must.Must(ydbschema.TTLCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
					[]schemaext.SubjectCoverage{{Kind: ydbschema.TTLKind, Subject: ttlTable(), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "x"}}}))
			},
			wantReason: "the desired source could not describe the YDB TTL",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := ttlRequest(&ydbschema.TTL{Column: "created_at", Interval: "P30D"}, nil)
			test.mutate(&input)
			result, err := ttlComparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Reason, qt.Equals, test.wantReason)
		})
	}
}

// TestTTLCompareFacets_ACreatedTableWithAnUndescribedDeclarationIsUndecided pins
// that a table the plan creates is no exception: creating it without the
// policy its source could not describe would make the limit a silent omission.
func TestTTLCompareFacets_ACreatedTableWithAnUndescribedDeclarationIsUndecided(t *testing.T) {
	c := qt.New(t)
	input := ttlRequest(nil, nil)
	input.Owners = []schemaext.ParentState{{Subject: ttlTable(), Desired: true}}
	input.Desired.Coverage = must.Must(ydbschema.TTLCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.TTLKind, Subject: ttlTable(), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "x"}}}))

	result, err := ttlComparisonRuntime().CompareFacets(t.Context(), input)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Equals, "the desired source could not describe the YDB TTL")
}

// TestTTLCompareFacets_NothingDeclaredOnATargetWithoutTTLIsNoChange pins the one
// case where a declaration of no policy is met without a read of it: a target
// established to lack row-level TTL holds none.
func TestTTLCompareFacets_NothingDeclaredOnATargetWithoutTTLIsNoChange(t *testing.T) {
	tests := []struct {
		name     string
		coverage schemaext.Coverage
	}{
		{name: "a catalog with no row-level TTL coverage", coverage: schemaext.Coverage{}},
		{name: "a read that left the table uninspected", coverage: ttlReadCoverage(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not asked"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := ttlRequest(nil, nil)
			input.Capabilities = capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false)
			input.Current.Coverage = test.coverage
			result, err := ttlComparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestTTLCompareFacets_NothingDeclaredAgainstAnUninspectedReadIsUndecided is the
// control on the test above. A source that declares no policy manages the
// policy, so where the target may hold one -- a catalog from a file, a read
// that left the table uninspected, a capability set that does not answer the
// key -- the comparison cannot call the two sides equal.
func TestTTLCompareFacets_NothingDeclaredAgainstAnUninspectedReadIsUndecided(t *testing.T) {
	tests := []struct {
		name         string
		capabilities capability.Capabilities
		coverage     schemaext.Coverage
	}{
		{name: "a catalog with no row-level TTL coverage", capabilities: capability.YDB262(), coverage: schemaext.Coverage{}},
		{
			name:         "a read that left the table uninspected",
			capabilities: capability.YDB262(),
			coverage:     ttlReadCoverage(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not asked"}),
		},
		{name: "a capability set that does not answer the key", capabilities: capability.Capabilities{}, coverage: schemaext.Coverage{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := ttlRequest(nil, nil)
			input.Capabilities = test.capabilities
			input.Current.Coverage = test.coverage
			result, err := ttlComparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Reason, qt.Equals, "the YDB TTL was not inspected")
		})
	}
}

// TestTTLCompareFacets_NothingDeclaredAgainstAnUndescribedPolicyIsUndecided pins
// a read that inspected the table and saw a policy it could not describe: it
// has seen something, and the reason says so.
func TestTTLCompareFacets_NothingDeclaredAgainstAnUndescribedPolicyIsUndecided(t *testing.T) {
	c := qt.New(t)
	input := ttlRequest(nil, nil)
	input.Current.Coverage = ttlReadCoverage(schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "an unknown parameter"})

	result, err := ttlComparisonRuntime().CompareFacets(t.Context(), input)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Equals, "the read found a YDB TTL it could not describe")
}

// TestTTLCompareFacets_FailurePath pins the refusals that end the comparison.
func TestTTLCompareFacets_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*schemaext.FacetComparisonRequest)
		wantErr error
	}{
		{
			name: "a declared policy on a capability set without the key",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Capabilities = capability.YDB262().With(capability.RowDeletionPolicyEpochColumn, false).With(capability.RowDeletionPolicy, false)
			},
			wantErr: ptaherr.ErrUnsupportedFeature,
		},
		{
			name: "an invalid declaration",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Desired.Records = ttlRecords(&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "PT1.5S"}})
			},
			wantErr: schemaext.ErrInvalidValue,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := ttlRequest(&ydbschema.TTL{Column: "created_at", Interval: "P30D"}, nil)
			test.mutate(&input)
			result, err := ydbcompare.TTLService{}.CompareFacets(t.Context(), input)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}

// TestTTLCompareFacets_TheColumnIsComparedByTheTargetsRule pins the column
// half: YDB compares column names exactly, so a TTL moved to a column that
// differs only in case is a change, and the same interval on another column is
// one too. A unit is part of the TTL.
func TestTTLCompareFacets_TheColumnIsComparedByTheTargetsRule(t *testing.T) {
	tests := []struct {
		name             string
		desired, current ydbschema.TTL
	}{
		{name: "another column", desired: ydbschema.TTL{Column: "updated_at", Interval: "P30D"}, current: ydbschema.TTL{Column: "created_at", Interval: "P30D"}},
		{name: "another case", desired: ydbschema.TTL{Column: "CreatedAt", Interval: "P30D"}, current: ydbschema.TTL{Column: "createdat", Interval: "P30D"}},
		{name: "another interval", desired: ydbschema.TTL{Column: "created_at", Interval: "P31D"}, current: ydbschema.TTL{Column: "created_at", Interval: "PT720H"}},
		{name: "another unit", desired: ydbschema.TTL{Column: "e", Interval: "P1D", Unit: "SECONDS"}, current: ydbschema.TTL{Column: "e", Interval: "P1D", Unit: "MILLISECONDS"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := ttlRequest(&test.desired, &test.current)
			result, err := ttlComparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 1)
		})
	}
}
