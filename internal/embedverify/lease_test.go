package embedverify_test

import (
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedverify"
)

// leaseAt returns the run state a healthy generation has, with the lease the
// test is about.
func leaseAt(holder string, expires, now time.Time) embedverify.RunState {
	return embedverify.RunState{
		SnapshotComplete: true, CatchUpReached: true,
		LeaseHolder: holder, LeaseExpires: expires, Now: now,
	}
}

// TestVerify_ALiveLeaseIsReportedUnmeasuredHappyPath covers stokaro/ptah#2738.
//
// The consistency layer advertised "is a lease still held?" on two documentation
// pages and answered it nowhere: the field the finding read was set only in this
// package's own tests, so a run whose lease was live reported `every
// deterministic layer passed` while `SELECT lease_owner, lease_expires > now()`
// said otherwise.
//
// It is unmeasured rather than a finding because the question the finding asked
// -- could another worker still write -- is not answerable from what a run
// records. Every CLI verb claims under the constant `ptah-cli`, and no verb
// releases its lease, so a live lease is the ordinary state immediately after a
// backfill. A blocking finding there would refuse the sequence the guides
// publish, on every healthy run.
func TestVerify_ALiveLeaseIsReportedUnmeasuredHappyPath(t *testing.T) {
	c := qt.New(t)
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	expectation, structure, source, target, _ := healthy()

	report := verify(c, expectation, structure, source, target,
		leaseAt("ptah-cli", now.Add(5*time.Minute), now))

	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", summaries(report)))
	c.Assert(report.Unmeasured, qt.Contains,
		`a lease on this run is held by "ptah-cli" until 2026-09-02T12:05:00Z, and whether that `+
			`worker could still write was not decided: every command claims under one name, so the `+
			`holder does not identify a process`)
}

// TestVerify_ALeaseSaysNothingWhenItIsGoneHappyPath is the pair of controls the
// statement above needs.
//
// Without them a verifier that appended the sentence unconditionally would
// satisfy the test above and tell every operator their run might still be being
// written -- including the ones whose lease expired an hour ago, and the ones
// that never had one.
func TestVerify_ALeaseSaysNothingWhenItIsGoneHappyPath(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		state embedverify.RunState
	}{
		{
			name:  "nobody holds it",
			state: leaseAt("", time.Time{}, now),
		},
		{
			name:  "the holder's lease expired",
			state: leaseAt("ptah-cli", now.Add(-time.Minute), now),
		},
		{
			name:  "the lease expires exactly now",
			state: leaseAt("ptah-cli", now, now),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			expectation, structure, source, target, _ := healthy()

			report := verify(c, expectation, structure, source, target, test.state)

			c.Assert(report.Passed(), qt.IsTrue)
			c.Assert(unmeasuredAbout(report, "lease"), qt.HasLen, 0)
		})
	}
}

// unmeasuredAbout keeps the unmeasured entries mentioning a word, so a test can
// assert about one of them without pinning the others' wording.
func unmeasuredAbout(report embedverify.Report, word string) []string {
	var matching []string
	for _, entry := range report.Unmeasured {
		for range onlyWhenContains(entry, word) {
			matching = append(matching, entry)
		}
	}
	return matching
}

// onlyWhenContains yields once when the entry mentions the word, so
// unmeasuredAbout filters without a conditional in a test function.
func onlyWhenContains(entry, word string) []struct{} {
	if !strings.Contains(entry, word) {
		return nil
	}
	return make([]struct{}, 1)
}
