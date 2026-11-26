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

func TestIsQualified(t *testing.T) {
	tests := []struct {
		name   string
		ctx    context.Context
		wanted bool
	}{
		{name: "a run that says nothing", ctx: t.Context(), wanted: false},
		{name: "a run that left a kind out", ctx: readscope.Without(t.Context(), readscope.Views), wanted: false},
		{name: "a read of what the run created", ctx: readscope.Authored(t.Context()), wanted: false},
		{name: "a run that names every schema", ctx: readscope.Qualified(t.Context()), wanted: true},
		{
			name:   "a kind left out after the run named every schema",
			ctx:    readscope.Without(readscope.Qualified(t.Context()), readscope.Views),
			wanted: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(readscope.IsQualified(test.ctx), qt.Equals, test.wanted)
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

// readOfThree is a read of one table and the two triggers on it.
var readOfThree = readscope.Read{Dialect: "mysql", Current: "app", Objects: []readscope.Object{
	{Kind: readscope.Triggers, Table: "t", Name: "keep"},
	{Kind: readscope.Triggers, Table: "t", Name: "drop"},
	{Kind: readscope.Tables, Name: "t"},
}}

// leaves returns a selector that keeps every object but the one named name.
func leaves(name string) readscope.Selector {
	return func(read readscope.Read) []bool {
		kept := make([]bool, len(read.Objects))
		for index, object := range read.Objects {
			kept[index] = object.Name != name
		}
		return kept
	}
}

func TestKeeps(t *testing.T) {
	tests := []struct {
		name   string
		ctx    context.Context
		wanted []bool
	}{
		{name: "a run that says nothing keeps every object", ctx: t.Context(), wanted: []bool{true, true, true}},
		{name: "a nil selector says nothing", ctx: readscope.Selecting(t.Context(), nil), wanted: []bool{true, true, true}},
		{
			name:   "an object the selector leaves out",
			ctx:    readscope.Selecting(t.Context(), leaves("drop")),
			wanted: []bool{true, false, true},
		},
		{
			name:   "a second selector leaves out more",
			ctx:    readscope.Selecting(readscope.Selecting(t.Context(), leaves("drop")), leaves("keep")),
			wanted: []bool{false, false, true},
		},
		{
			name:   "a read of what the run created keeps every object",
			ctx:    readscope.Authored(readscope.Selecting(t.Context(), leaves("drop"))),
			wanted: []bool{true, true, true},
		},
		{
			name: "an answer too short keeps the objects it does not reach",
			ctx: readscope.Selecting(t.Context(), func(readscope.Read) []bool {
				return []bool{false}
			}),
			wanted: []bool{false, true, true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(readscope.Keeps(test.ctx, readOfThree), qt.DeepEquals, test.wanted)
		})
	}
}

// A selector sees the read it is asked about.
func TestKeeps_PassesTheRead(t *testing.T) {
	c := qt.New(t)
	var seen readscope.Read
	ctx := readscope.Selecting(t.Context(), func(read readscope.Read) []bool {
		seen = read
		return nil
	})

	readscope.Keeps(ctx, readOfThree)

	c.Assert(seen, qt.DeepEquals, readOfThree)
}
