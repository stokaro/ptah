package readscope_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/readscope"
)

func TestIsAuthored(t *testing.T) {
	tests := []struct {
		name   string
		ctx    context.Context
		wanted bool
	}{
		{name: "a run that says nothing", ctx: t.Context(), wanted: false},
		{name: "a run that left a kind out", ctx: readscope.Without(t.Context(), readscope.Views), wanted: false},
		{name: "a read of what the run created", ctx: readscope.Authored(t.Context()), wanted: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(readscope.IsAuthored(test.ctx), qt.Equals, test.wanted)
		})
	}
}

func TestNeeds(t *testing.T) {
	tests := []struct {
		name   string
		ctx    context.Context
		kind   readscope.Kind
		wanted bool
	}{
		{
			name:   "a run that says nothing needs every kind",
			ctx:    t.Context(),
			kind:   readscope.Triggers,
			wanted: true,
		},
		{
			name:   "a kind the run left out",
			ctx:    readscope.Without(t.Context(), readscope.Triggers),
			kind:   readscope.Triggers,
			wanted: false,
		},
		{
			name:   "a kind the run did not leave out",
			ctx:    readscope.Without(t.Context(), readscope.Triggers),
			kind:   readscope.Routines,
			wanted: true,
		},
		{
			name:   "a second call adds to the first",
			ctx:    readscope.Without(readscope.Without(t.Context(), readscope.Triggers), readscope.Routines),
			kind:   readscope.Triggers,
			wanted: false,
		},
		{
			name:   "leaving nothing out leaves the context as it was",
			ctx:    readscope.Without(t.Context()),
			kind:   readscope.Views,
			wanted: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(readscope.Needs(test.ctx, test.kind), qt.Equals, test.wanted)
		})
	}
}
