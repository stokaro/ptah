package assistloop_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/aiprovider"
	"go.5x5.cz/ptah/internal/assistloop"
)

// TestRun_AToolCallTheServerRefusesComesBackAsAnAnswer is #1490's invalid and
// hallucinated tool-call box.
//
// A model can name a tool that does not exist, omit a required argument, invent
// one, pass the wrong type, or name a value the workspace has no such thing
// for. None of those is exotic: they are what a model does when it is guessing,
// and a hostile repository can steer it into guessing. What matters is that
// each comes back as something the model can read and act on, and that the run
// keeps going -- a loop that ended at the first bad call would make one wrong
// guess a denial of service.
//
// The messages are asserted by their diagnostic code plus the one detail that
// separates the case from its neighbours, rather than by their whole prose: the
// code is the contract a client branches on, and the sentence around it is
// allowed to be reworded.
func TestRun_AToolCallTheServerRefusesComesBackAsAnAnswer(t *testing.T) {
	rows := []struct {
		name     string
		call     aiprovider.Response
		contains []string
	}{
		{
			name:     "a tool nobody serves",
			call:     aiprovider.ToolTurn("t1", "drop_everything", make(map[string]any)),
			contains: []string{"unknown tool", "drop_everything"},
		},
		{
			name:     "a required argument left out",
			call:     aiprovider.ToolTurn("t1", "read_artifact", make(map[string]any)),
			contains: []string{"invalid_request", "missing properties", "artifact"},
		},
		{
			name: "an argument nobody declared",
			call: aiprovider.ToolTurn("t1", "read_artifact", map[string]any{
				"artifact": "migrations", "unlock_everything": true,
			}),
			contains: []string{"invalid_request", "additional properties", "unlock_everything"},
		},
		{
			name:     "an argument of the wrong type",
			call:     aiprovider.ToolTurn("t1", "read_artifact", map[string]any{"artifact": 7}),
			contains: []string{"invalid_request", "type"},
		},
		{
			name:     "an artifact class this workspace has no directory for",
			call:     aiprovider.ToolTurn("t1", "read_artifact", map[string]any{"artifact": "tests"}),
			contains: []string{"artifact_class_not_configured", "tests"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			provider := aiprovider.NewFake(row.call, aiprovider.TextTurn("I could not do that."))
			loop := loopWith(c, provider, assistloop.Options{})

			run, err := loop.Run(context.Background(), "do something")

			c.Assert(err, qt.IsNil,
				qt.Commentf("a call the server refused ended the run instead of being answered"))
			c.Assert(run.StopReason, qt.Equals, assistloop.StoppedWithAnswer)
			c.Assert(run.Tools, qt.HasLen, 1)
			c.Assert(run.Tools[0].Failed, qt.IsTrue)
			for _, fragment := range row.contains {
				c.Assert(run.Tools[0].Result, qt.Contains, fragment)
			}
		})
	}
}

// TestRun_ARefusedCallDoesNotStopTheOnesAfterIt is the non-interference control.
//
// Each row above runs one bad call and nothing else, so a loop that quietly
// stopped serving tools after a refusal would satisfy every one of them. This
// runs a refusal and then a real call in the same run, and the real one has to
// answer.
func TestRun_ARefusedCallDoesNotStopTheOnesAfterIt(t *testing.T) {
	c := qt.New(t)
	provider := aiprovider.NewFake(
		aiprovider.ToolTurn("t1", "drop_everything", make(map[string]any)),
		aiprovider.ToolTurn("t2", "read_artifact", map[string]any{"artifact": "migrations"}),
		aiprovider.TextTurn("There is one migration pair."),
	)
	loop := loopWith(c, provider, assistloop.Options{})

	run, err := loop.Run(context.Background(), "what is here?")

	c.Assert(err, qt.IsNil)
	c.Assert(run.StopReason, qt.Equals, assistloop.StoppedWithAnswer)
	c.Assert(run.Tools, qt.HasLen, 2)
	c.Assert(run.Tools[0].Failed, qt.IsTrue)
	c.Assert(run.Tools[1].Failed, qt.IsFalse)
	c.Assert(run.Tools[1].Result, qt.Contains, "1700000000_init.up.sql")
}
