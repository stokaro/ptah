package crdbcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbcompare"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/engine"
)

func comparisonRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID:      crdbschema.Owner,
		Targets: []engine.Target{{Name: "cockroachdb"}},
		Codecs:  append(crdbschema.Codecs(), crdbdiff.Codecs()...),
		FacetComparisons: []engine.FacetComparison{{
			Target: "cockroachdb", OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
			Kinds: []schemaext.Kind{crdbschema.RowTTLKind}, ChangeKinds: []schemaext.Kind{crdbdiff.RowTTLKind}, Service: crdbcompare.Service{},
		}},
	}))
}

func sessions() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("cockroachdb")).TableParts("", "sessions")
}

func records(value schemaext.Value) []schemaext.FacetRecord {
	if value == nil {
		return nil
	}
	return []schemaext.FacetRecord{{Subject: sessions(), Values: must.Must(schemaext.NewFacets(value))}}
}

// request compares one table both sides hold. The desired source can declare a
// policy, and the read inspected this table, unless a test says otherwise.
func request(desired, current *crdbschema.Policy) schemaext.FacetComparisonRequest {
	var desiredValue, currentValue schemaext.Value
	if desired != nil {
		desiredValue = &crdbschema.DesiredRowTTL{Policy: *desired}
	}
	if current != nil {
		currentValue = &crdbschema.ObservedRowTTL{Policy: *current}
	}
	return schemaext.FacetComparisonRequest{
		Target: "cockroachdb", Capabilities: capability.CockroachDB26(), Kinds: []schemaext.Kind{crdbschema.RowTTLKind},
		Owners: []schemaext.ParentState{{Subject: sessions(), Desired: true, Current: true}},
		Desired: schemaext.FacetState{
			Records:  records(desiredValue),
			Coverage: must.Must(crdbschema.RowTTLCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
		},
		Current: schemaext.FacetState{Records: records(currentValue), Coverage: readCoverage(schemaext.Knowledge{State: schemaext.Complete})},
	}
}

func readCoverage(knowledge schemaext.Knowledge) schemaext.Coverage {
	return must.Must(crdbschema.RowTTLCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
		[]schemaext.SubjectCoverage{{Kind: crdbschema.RowTTLKind, Subject: sessions(), Knowledge: knowledge}}))
}

func TestCompareFacets_ReportsEveryTransition(t *testing.T) {
	tests := []struct {
		name             string
		desired, current *crdbschema.Policy
		wantBefore       *crdbschema.ObservedRowTTL
		wantAfter        *crdbschema.DesiredRowTTL
	}{
		{
			name: "adding a policy", desired: &crdbschema.Policy{ExpirationExpression: "expires_at"},
			wantAfter: &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at"}},
		},
		{
			// A source that can declare a policy and declares none requests
			// none, which is what makes the removal plannable.
			name: "removing a policy", current: &crdbschema.Policy{ExpirationExpression: "expires_at"},
			wantBefore: &crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at"}},
		},
		{
			name: "changing a parameter", desired: &crdbschema.Policy{ExpirationExpression: "expires_at"}, current: &crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"},
			wantBefore: &crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"}},
			wantAfter:  &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := request(test.desired, test.current)
			result, err := comparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Kind, qt.Equals, crdbschema.RowTTLKind)
			c.Assert(result.Changes[0].Change.Subject, qt.DeepEquals, sessions())
			change := result.Changes[0].Change.Value.(*crdbdiff.RowTTL)
			c.Assert(change.Before, qt.DeepEquals, test.wantBefore)
			c.Assert(change.After, qt.DeepEquals, test.wantAfter)
			c.Assert(result.Desired.Records, qt.DeepEquals, input.Desired.Records)
		})
	}
}

// TestCompareFacets_ReadsRewrittenValuesAsValues pins convergence on the
// spellings the server stores: no change for two spellings of one policy.
func TestCompareFacets_ReadsRewrittenValuesAsValues(t *testing.T) {
	tests := []struct {
		name             string
		desired, current *crdbschema.Policy
	}{
		{name: "an interval", desired: &crdbschema.Policy{ExpireAfter: "72 hours"}, current: &crdbschema.Policy{ExpireAfter: "72:00:00"}},
		{
			name:    "a poll interval",
			desired: &crdbschema.Policy{ExpireAfter: "1 day", RowStatsPollInterval: "600s"}, current: &crdbschema.Policy{ExpireAfter: "1 day", RowStatsPollInterval: "10m0s"},
		},
		{name: "neither side has one"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := comparisonRuntime().CompareFacets(t.Context(), request(test.desired, test.current))
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestCompareFacets_LeavesAnUndescribedPolicyAlone pins the source that cannot
// declare a policy: it says nothing about TTL, the live policy is adopted into
// the effective declaration so a rebuild keeps it, and no change is planned.
func TestCompareFacets_LeavesAnUndescribedPolicyAlone(t *testing.T) {
	c := qt.New(t)

	input := request(nil, &crdbschema.Policy{ExpirationExpression: "expires_at"})
	input.Desired.Coverage = schemaext.Coverage{}
	result, err := comparisonRuntime().CompareFacets(t.Context(), input)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Desired.Records, qt.HasLen, 1)
	adopted, found, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](result.Desired.Records[0].Values, crdbschema.RowTTLKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(adopted.Policy, qt.DeepEquals, crdbschema.Policy{ExpirationExpression: "expires_at"})
}

// TestCompareFacets_ParentLifecycleCarriesThePolicy pins that a table created
// or removed by the plan carries its policy with it: no change is reported.
func TestCompareFacets_ParentLifecycleCarriesThePolicy(t *testing.T) {
	tests := []struct {
		name   string
		owner  schemaext.ParentState
		mutate func(*schemaext.FacetComparisonRequest)
	}{
		{name: "a created table", owner: schemaext.ParentState{Subject: sessions(), Desired: true},
			mutate: func(r *schemaext.FacetComparisonRequest) { r.Current.Records = nil }},
		{name: "a removed table", owner: schemaext.ParentState{Subject: sessions(), Current: true},
			mutate: func(r *schemaext.FacetComparisonRequest) { r.Desired.Records = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := request(&crdbschema.Policy{ExpirationExpression: "a"}, &crdbschema.Policy{ExpirationExpression: "b"})
			input.Owners = []schemaext.ParentState{test.owner}
			test.mutate(&input)
			result, err := comparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestCompareFacets_UnknownStateIsUndecided pins that an uninspected live
// policy, or a desired source that met one it cannot represent, never becomes
// an empty successful comparison.
func TestCompareFacets_UnknownStateIsUndecided(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*schemaext.FacetComparisonRequest)
		wantReason string
	}{
		{
			name:       "the read did not inspect the table",
			mutate:     func(r *schemaext.FacetComparisonRequest) { r.Current.Coverage = schemaext.Coverage{} },
			wantReason: "CockroachDB row-level TTL was not inspected",
		},
		{
			name: "the read could not represent the table's policy",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Current.Coverage = readCoverage(schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "an unknown parameter"})
			},
			wantReason: "the read found a CockroachDB row-level TTL it could not describe",
		},
		{
			name: "the source could not represent the declaration",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Desired.Records = nil
				r.Desired.Coverage = must.Must(crdbschema.RowTTLCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
					[]schemaext.SubjectCoverage{{Kind: crdbschema.RowTTLKind, Subject: sessions(), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "x"}}}))
			},
			wantReason: "the desired source could not describe CockroachDB row-level TTL",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := request(&crdbschema.Policy{ExpirationExpression: "expires_at"}, nil)
			test.mutate(&input)
			result, err := comparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Reason, qt.Equals, test.wantReason)
		})
	}
}

// TestCompareFacets_ACreatedTableWithAnUndescribedDeclarationIsUndecided pins
// that a table the plan creates is no exception: creating it without the
// policy its source could not describe would make the limit a silent omission.
func TestCompareFacets_ACreatedTableWithAnUndescribedDeclarationIsUndecided(t *testing.T) {
	c := qt.New(t)
	input := request(nil, nil)
	input.Owners = []schemaext.ParentState{{Subject: sessions(), Desired: true}}
	input.Desired.Coverage = must.Must(crdbschema.RowTTLCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: crdbschema.RowTTLKind, Subject: sessions(), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "x"}}}))

	result, err := comparisonRuntime().CompareFacets(t.Context(), input)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Equals, "the desired source could not describe CockroachDB row-level TTL")
}

// TestCompareFacets_NothingDeclaredOnATargetWithoutTTLIsNoChange pins the one
// case where a declaration of no policy is met without a read of it: a target
// established to lack row-level TTL holds none.
func TestCompareFacets_NothingDeclaredOnATargetWithoutTTLIsNoChange(t *testing.T) {
	tests := []struct {
		name     string
		coverage schemaext.Coverage
	}{
		{name: "a catalog with no row-level TTL coverage", coverage: schemaext.Coverage{}},
		{name: "a read that left the table uninspected", coverage: readCoverage(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not asked"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := request(nil, nil)
			input.Capabilities = capability.CockroachDB26().With(capability.RowLevelTTL, false)
			input.Current.Coverage = test.coverage
			result, err := comparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestCompareFacets_NothingDeclaredAgainstAnUninspectedReadIsUndecided is the
// control on the test above. A source that declares no policy manages the
// policy, so where the target may hold one -- a catalog from a file, a read
// that left the table uninspected, a capability set that does not answer the
// key -- the comparison cannot call the two sides equal.
func TestCompareFacets_NothingDeclaredAgainstAnUninspectedReadIsUndecided(t *testing.T) {
	tests := []struct {
		name         string
		capabilities capability.Capabilities
		coverage     schemaext.Coverage
	}{
		{name: "a catalog with no row-level TTL coverage", capabilities: capability.CockroachDB26(), coverage: schemaext.Coverage{}},
		{
			name:         "a read that left the table uninspected",
			capabilities: capability.CockroachDB26(),
			coverage:     readCoverage(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not asked"}),
		},
		{name: "a capability set that does not answer the key", capabilities: capability.Capabilities{}, coverage: schemaext.Coverage{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := request(nil, nil)
			input.Capabilities = test.capabilities
			input.Current.Coverage = test.coverage
			result, err := comparisonRuntime().CompareFacets(t.Context(), input)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Reason, qt.Equals, "CockroachDB row-level TTL was not inspected")
		})
	}
}

// TestCompareFacets_NothingDeclaredAgainstAnUndescribedPolicyIsUndecided pins
// a read that inspected the table and saw a policy it could not describe: it
// has seen something, and the reason says so.
func TestCompareFacets_NothingDeclaredAgainstAnUndescribedPolicyIsUndecided(t *testing.T) {
	c := qt.New(t)
	input := request(nil, nil)
	input.Current.Coverage = readCoverage(schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "an unknown parameter"})

	result, err := comparisonRuntime().CompareFacets(t.Context(), input)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Equals, "the read found a CockroachDB row-level TTL it could not describe")
}

// TestCompareFacets_FailurePath pins the refusals that end the comparison.
func TestCompareFacets_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*schemaext.FacetComparisonRequest)
		wantErr error
	}{
		{
			name: "a declared policy on a capability set without the key",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Capabilities = capability.CockroachDB26().With(capability.RowLevelTTL, false)
			},
			wantErr: ptaherr.ErrUnsupportedFeature,
		},
		{
			name: "an invalid declaration",
			mutate: func(r *schemaext.FacetComparisonRequest) {
				r.Desired.Records = records(&crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{JobCron: "@daily"}})
			},
			wantErr: schemaext.ErrInvalidValue,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			input := request(&crdbschema.Policy{ExpirationExpression: "expires_at"}, nil)
			test.mutate(&input)
			result, err := crdbcompare.Service{}.CompareFacets(t.Context(), input)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}

// TestCompareFacets_ChangeOperandsAreCopies pins that the change cannot reach
// back into the states it was computed from.
func TestCompareFacets_ChangeOperandsAreCopies(t *testing.T) {
	c := qt.New(t)

	input := request(&crdbschema.Policy{ExpirationExpression: "a", SelectBatchSize: new(int64(1))}, &crdbschema.Policy{ExpirationExpression: "b"})
	result, err := crdbcompare.Service{}.CompareFacets(t.Context(), input)
	c.Assert(err, qt.IsNil)
	change := result.Changes[0].Change.Value.(*crdbdiff.RowTTL)
	*change.After.Policy.SelectBatchSize = 9

	declared, _, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](input.Desired.Records[0].Values, crdbschema.RowTTLKind)
	c.Assert(err, qt.IsNil)
	c.Assert(*declared.Policy.SelectBatchSize, qt.Equals, int64(1))
}
