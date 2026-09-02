package embedengine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedrun"
)

// fourChanges is one page of four settled changes, which against MaxInputs 2 is
// two provider requests and against the old single-batch shape was one.
func fourChanges() *fakeChanges {
	return &fakeChanges{
		pages: [][]embedcatchup.Event{{
			changed(101, 1, "1", embedcatchup.OperationUpdate),
			changed(101, 2, "2", embedcatchup.OperationUpdate),
			changed(101, 3, "3", embedcatchup.OperationUpdate),
			changed(101, 4, "4", embedcatchup.OperationUpdate),
		}},
		horizons: []uint64{102},
	}
}

// TestCatchUp_SplitsOnePageIntoProviderRequestsHappyPath covers
// stokaro/ptah#2740.
//
// `catchup` registered --batch-inputs and ignored it: every prepared row went
// into one embedrun.Batch, so the only thing bounding a provider request was
// --batch-rows, which is the page's limit and not the endpoint's. An operator
// tuning their provider's request size set the flag, saw no error, and got
// whatever the page happened to hold.
//
// Measured here as the request shape, because that is what the flag names.
func TestCatchUp_SplitsOnePageIntoProviderRequestsHappyPath(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, embedrun.BatchBounds{MaxRows: 4, MaxInputs: 2})
	run, _, err := caughtUp(c, h, fourChanges(), livingRows("1", "2", "3", "4"))

	c.Assert(err, qt.IsNil)
	c.Assert(h.provider.calls, qt.HasLen, 2)
	c.Assert(h.provider.calls[0], qt.HasLen, 2)
	c.Assert(h.provider.calls[1], qt.HasLen, 2)
	c.Assert(run.Progress.RowsEmbedded, qt.Equals, int64(4))
}

// TestCatchUp_TheSplitIsTheRequestAndNotTheCommitHappyPath is the guarantee the
// fix above must not have traded away.
//
// The backfill commits per batch. Catch-up does not: its writes and the cursor
// they carry the run to are one transaction, which is what lets a run that dies
// resume at a position whose work is on disk. Splitting the provider request
// must leave that alone -- two requests, still one commit.
func TestCatchUp_TheSplitIsTheRequestAndNotTheCommitHappyPath(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, embedrun.BatchBounds{MaxRows: 4, MaxInputs: 2})
	_, _, err := caughtUp(c, h, fourChanges(), livingRows("1", "2", "3", "4"))

	c.Assert(err, qt.IsNil)
	c.Assert(h.provider.calls, qt.HasLen, 2)
	// Exactly one commit carries writes, and it carries all four. The second
	// commit this page produces is the barrier -- the empty pass that records
	// how far the catch-up read -- and it exists either side of this change.
	c.Assert(commitsWithWrites(h), qt.HasLen, 1)
	c.Assert(commitsWithWrites(h)[0].writes, qt.HasLen, 4)
}

// commitsWithWrites keeps the commits that carried target writes, dropping the
// watermark-only ones.
func commitsWithWrites(h *harness) []commit {
	var carrying []commit
	for _, landed := range h.target.commits {
		if len(landed.writes) > 0 {
			carrying = append(carrying, landed)
		}
	}
	return carrying
}

// TestCatchUp_TokenCountsCoverEveryRequestHappyPath is the accounting half.
// Two requests, and the run's totals have to hold both -- keeping only the last
// would under-report what the provider was paid for.
func TestCatchUp_TokenCountsCoverEveryRequestHappyPath(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, embedrun.BatchBounds{MaxRows: 4, MaxInputs: 2})
	h.provider.reportsUsageOn = []int{1, 2}
	run, _, err := caughtUp(c, h, fourChanges(), livingRows("1", "2", "3", "4"))

	c.Assert(err, qt.IsNil)
	// One prompt token per input, so four inputs across two requests are four
	// however they were split. Both requests report, because an answer carrying
	// no usage object leaves its counts at zero -- asserting totals without
	// that would assert a state no provider produces.
	c.Assert(run.Progress.ProviderPromptTokens, qt.Equals, int64(4))
	c.Assert(run.Progress.ProviderTotalTokens, qt.Equals, int64(8))
}

// TestCatchUp_ARequestThatReportedNothingAddsNoTokensHappyPath is the other
// side, and it is what keeps the counts and the flag from contradicting.
//
// `status` prints "the provider reported no token usage" when no batch carried
// a usage object, so a total including tokens from an answer that reported none
// would sit beside that sentence.
func TestCatchUp_ARequestThatReportedNothingAddsNoTokensHappyPath(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, embedrun.BatchBounds{MaxRows: 4, MaxInputs: 2})
	h.provider.reportsUsageOn = []int{1}
	run, _, err := caughtUp(c, h, fourChanges(), livingRows("1", "2", "3", "4"))

	c.Assert(err, qt.IsNil)
	c.Assert(h.provider.calls, qt.HasLen, 2)
	// Two inputs from the request that accounted for itself, nothing from the
	// one that did not.
	c.Assert(run.Progress.ProviderPromptTokens, qt.Equals, int64(2))
	c.Assert(run.Progress.ProviderTotalTokens, qt.Equals, int64(4))
	c.Assert(run.Progress.ProviderUsageBatches, qt.Equals, int64(1))
}

// TestCatchUp_UsageIsClaimedOnlyWhenEveryRequestReportedIt is the rule the
// split forced a decision about.
//
// ProviderUsageBatches exists to separate a provider that reported zero from
// one that reported nothing, and a page that is now several requests has to
// collapse several answers into one.
//
// Any answer carrying a usage object is enough. Requiring all of them reads as
// the stricter rule and contradicts itself: an answer with no usage object
// leaves its counts at zero, so a mixed page still holds real tokens from the
// requests that did report -- and `status` keys "the provider reported no token
// usage" on this count being zero, which would then sit beside a non-zero
// total (stokaro/ptah#2740 review).
func TestCatchUp_UsageIsClaimedWhenAnyRequestReportedIt(t *testing.T) {
	tests := []struct {
		name       string
		reportedOn []int
		want       int64
	}{
		{name: "neither request reported", reportedOn: nil, want: 0},
		{name: "only the first reported", reportedOn: []int{1}, want: 1},
		{name: "only the second reported", reportedOn: []int{2}, want: 1},
		{name: "both reported", reportedOn: []int{1, 2}, want: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h := newHarness(c, embedrun.BatchBounds{MaxRows: 4, MaxInputs: 2})
			h.provider.reportsUsageOn = test.reportedOn

			run, _, err := caughtUp(c, h, fourChanges(), livingRows("1", "2", "3", "4"))

			c.Assert(err, qt.IsNil)
			c.Assert(h.provider.calls, qt.HasLen, 2)
			c.Assert(run.Progress.ProviderUsageBatches, qt.Equals, test.want)
		})
	}
}
