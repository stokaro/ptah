package embedcatchup_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedcatchup"
)

// TestResumeFrom_HappyPath covers the positions a run can resume at.
//
// The precedence is the point: a run that has caught up resumes where catch-up
// left it, and only a run that has not yet caught up resumes at the boundary
// its backfill started from. Reading them the other way round would send a
// resumed run back over changes it has already processed, and would put the
// prune floor behind every reader.
func TestResumeFrom_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		catchUp  string
		snapshot string
		want     embedcatchup.Cursor
	}{
		{
			name:     "the catch-up watermark wins over the snapshot boundary",
			catchUp:  "4446",
			snapshot: "1200",
			want:     embedcatchup.Cursor{Transaction: 4446},
		},
		{
			name:     "the snapshot boundary is used before catch-up has run",
			snapshot: "1200",
			want:     embedcatchup.Cursor{Transaction: 1200},
		},
		{
			name:    "a position inside a transaction keeps both halves",
			catchUp: "4446:12",
			want:    embedcatchup.Cursor{Transaction: 4446, Sequence: 12},
		},
		{
			name:     "a snapshot boundary inside a transaction keeps both halves",
			snapshot: "1200:7",
			want:     embedcatchup.Cursor{Transaction: 1200, Sequence: 7},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok, err := embedcatchup.ResumeFrom(test.catchUp, test.snapshot)
			c.Assert(err, qt.IsNil)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestResumeFrom_NoPosition covers a run that records nowhere to resume.
//
// Separated from the failures below because it is not one: a run whose mode
// records no changes has no position by design. What matters is that the answer
// is absence rather than the zero cursor, which would mean "every change ever
// recorded" to a reader and would authorize deleting the whole outbox to a
// pruner.
func TestResumeFrom_NoPosition(t *testing.T) {
	t.Run("neither watermark reports absence rather than zero", func(t *testing.T) {
		c := qt.New(t)
		got, ok, err := embedcatchup.ResumeFrom("", "")
		c.Assert(err, qt.IsNil)
		c.Assert(ok, qt.IsFalse)
		c.Assert(got, qt.Equals, embedcatchup.Cursor{})
	})
}

// TestResumeFrom_FailurePath covers a watermark that does not parse.
//
// Refused rather than skipped: a value nobody can read is a position nobody
// knows, and defaulting it to zero is the same mistake as the one above with a
// worse disguise.
func TestResumeFrom_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		catchUp  string
		snapshot string
		wantErr  string
	}{
		{
			name:    "a catch-up watermark that is not a transaction identity",
			catchUp: "later",
			wantErr: `the catch-up watermark "later" is not a transaction identity.*`,
		},
		{
			name:     "a snapshot boundary that is not a transaction identity",
			snapshot: "boundary",
			wantErr:  `the snapshot watermark "boundary" is not a transaction identity.*`,
		},
		{
			name:    "a sequence half that is not a number",
			catchUp: "4446:many",
			wantErr: `the catch-up watermark "4446:many" does not carry a sequence.*`,
		},
		{
			name:    "a negative sequence",
			catchUp: "4446:-1",
			wantErr: `the catch-up watermark "4446:-1" carries a negative sequence`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok, err := embedcatchup.ResumeFrom(test.catchUp, test.snapshot)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, embedcatchup.Cursor{})
		})
	}
}
