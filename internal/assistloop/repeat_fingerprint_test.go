package assistloop_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/aiprovider"
	"ptah.run/internal/assistloop"
)

// The repeat limit counts a call, not a tool -- stokaro/ptah#1490.
//
// The fingerprint is `name + arguments`, and every existing test repeats one
// tool with ONE argument set, so the arguments half was unmeasured: a change
// reducing the fingerprint to the name alone stayed green, and a model
// legitimately reading four artifacts in a row would have been cut off with
// nothing red.
//
// Six calls to one tool, six different arguments, MaxRepeats of 2. A loop
// keyed on the name alone stops after the second; a loop keyed on the pair runs
// them all.
func TestRun_RepeatingOneToolWithDifferentArgumentsIsNotRepeatingItself(t *testing.T) {
	c := qt.New(t)
	turns := make([]aiprovider.Response, 0, 6)
	for index := range 6 {
		turns = append(turns, aiprovider.ToolTurn("t", "read_artifact",
			map[string]any{"artifact": artifactNames[index]}))
	}
	loop := loopWith(c, aiprovider.NewFake(turns...),
		assistloop.Options{MaxRepeats: 2, MaxTurns: 20, MaxToolCalls: 20})

	result, err := loop.Run(context.Background(), "read them all")

	c.Assert(result.StopReason, qt.Not(qt.Equals), assistloop.StoppedRepeating,
		qt.Commentf("six distinct calls to one tool were read as one call repeated"))
	c.Assert(len(result.Tools) > 2, qt.IsTrue,
		qt.Commentf("stopped after %d calls, so the arguments were not part of the fingerprint",
			len(result.Tools)))
	c.Assert(err, qt.IsNotNil, qt.Commentf("the fake runs out of turns, which is the loop's own limit"))
}

// artifactNames are six distinct argument values, so the six calls above differ
// only in the half the fingerprint is being measured for.
var artifactNames = []string{"migrations", "schema", "config", "lineage", "policy", "session"}

// The same tool with the SAME arguments still stops, which is the control.
//
// Without it, a loop that had lost the repeat limit entirely would pass the
// test above.
func TestRun_TheSameCallRepeatedStillStops(t *testing.T) {
	c := qt.New(t)
	turns := make([]aiprovider.Response, 0, 6)
	for range 6 {
		turns = append(turns, aiprovider.ToolTurn("t", "read_artifact",
			map[string]any{"artifact": "migrations"}))
	}
	loop := loopWith(c, aiprovider.NewFake(turns...),
		assistloop.Options{MaxRepeats: 2, MaxTurns: 20, MaxToolCalls: 20})

	result, err := loop.Run(context.Background(), "read it again")

	c.Assert(err, qt.ErrorIs, assistloop.ErrLimit)
	c.Assert(result.StopReason, qt.Equals, assistloop.StoppedRepeating)
	c.Assert(result.Tools, qt.HasLen, 2)
}

// A tool call whose arguments are not a JSON object is answered, not crashed
// on -- stokaro/ptah#1490.
//
// That is exactly what a model emits when it is cut off mid-call: a truncated
// argument string, or a bare value where an object was expected. The loop has
// always handled it, and nothing reached the path, so a change that panicked
// there or dropped the call silently would not have been caught.
//
// The record is asserted as failed AND the loop as continuing: answering the
// call and then stopping would leave the model with no way to correct itself,
// which is the same complaint invalid_tool_calls_test.go makes about refusals.
func TestRun_ArgumentsThatAreNotAJSONObjectAreAnswered(t *testing.T) {
	tests := []struct {
		name      string
		arguments string
	}{
		{name: "cut off mid-object", arguments: `{"artifact": "migrat`},
		{name: "a bare string", arguments: `"migrations"`},
		{name: "a bare number", arguments: `7`},
		{name: "a list", arguments: `["migrations"]`},
		{name: "empty", arguments: ``},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			turns := []aiprovider.Response{{
				Model: "fake-model",
				Message: aiprovider.Message{
					Role: aiprovider.RoleAssistant,
					ToolCalls: []aiprovider.ToolCall{
						{ID: "a", Name: "read_artifact", Arguments: []byte(test.arguments)},
					},
				},
			}, aiprovider.TextTurn("done")}
			loop := loopWith(c, aiprovider.NewFake(turns...), assistloop.Options{MaxTurns: 4})

			result, err := loop.Run(context.Background(), "read it")

			c.Assert(err, qt.IsNil)
			c.Assert(result.Tools, qt.HasLen, 1)
			c.Assert(result.Tools[0].Failed, qt.IsTrue)
			c.Assert(result.Tools[0].Result, qt.Contains, "not a JSON object")
			// The loop carried on and produced its answer, rather than ending
			// on the malformed call.
			c.Assert(result.Turns > 1, qt.IsTrue,
				qt.Commentf("the loop stopped on a malformed call instead of letting the model correct it"))
		})
	}
}
