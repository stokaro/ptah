package embedrelease_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedrelease"
)

// TestVerification_TheMeasurementDigestIgnoresWhenItWasMeasured is what makes a
// cutover plan citable.
//
// A plan is built before the record that would carry the artifact exists, and
// on a cutover that re-verifies the record may never be written at all. Citing
// the artifact digest therefore cited something nobody could fetch: measured on
// the shipped binary, a cutover record's `verification_digest` was the digest
// of the published report restamped at the cutover's own instant
// (stokaro/ptah#2643).
func TestVerification_TheMeasurementDigestIgnoresWhenItWasMeasured(t *testing.T) {
	c := qt.New(t)
	first := embedrelease.Verification{
		Generation: "gen-1", Passed: true, SourceRows: 3, TargetRows: 3, MeasuredAt: at,
	}
	later := first
	later.MeasuredAt = at.Add(17 * time.Second)

	c.Assert(later.MeasurementDigest(), qt.Equals, first.MeasurementDigest())
	// The control: the artifact digest still separates them, because naming one
	// record in a registry is the job it kept.
	c.Assert(later.Digest(), qt.Not(qt.Equals), first.Digest())
}

// TestVerification_TheMeasurementDigestCoversWhatWasMeasured is the other half.
//
// Ignoring the instant is only safe if everything else still moves it. A digest
// that ignored a finding would let an approval given for a clean report keep
// applying to one that found something, which is the property the plan cites it
// for.
func TestVerification_TheMeasurementDigestCoversWhatWasMeasured(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedrelease.Verification)
	}{
		{name: "generation", change: func(v *embedrelease.Verification) { v.Generation = "gen-2" }},
		{name: "verdict", change: func(v *embedrelease.Verification) { v.Passed = false }},
		{name: "source rows", change: func(v *embedrelease.Verification) { v.SourceRows = 4 }},
		{name: "target rows", change: func(v *embedrelease.Verification) { v.TargetRows = 4 }},
		{
			name: "a finding appearing",
			change: func(v *embedrelease.Verification) {
				v.Findings = append(v.Findings, embedrelease.Finding{
					Layer: "coverage", Severity: "blocking", Summary: "one row is stale", Count: 1,
				})
			},
		},
		{
			name: "a layer going unmeasured",
			change: func(v *embedrelease.Verification) {
				v.Unmeasured = append(v.Unmeasured, "the stored vectors were not read back")
			},
		},
		{
			name: "a retrieval measurement appearing",
			change: func(v *embedrelease.Verification) {
				v.Retrieval = &embedrelease.Retrieval{RecallAtK: 0.9}
			},
		},
	}
	base := embedrelease.Verification{
		Generation: "gen-1", Passed: true, SourceRows: 3, TargetRows: 3, MeasuredAt: at,
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := base
			changed.Findings = append([]embedrelease.Finding(nil), base.Findings...)
			changed.Unmeasured = append([]string(nil), base.Unmeasured...)

			test.change(&changed)

			c.Assert(changed.MeasurementDigest(), qt.Not(qt.Equals), base.MeasurementDigest())
		})
	}
}
