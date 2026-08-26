package atlasscript

// White-box testing required: the condition predicate is package-local, and the
// shapes that reach it come from drivers this package does not depend on.

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// A condition is read over the shapes engines report booleans as -- stokaro/ptah#1017.
//
// Engines disagree: a bool, an int64, or the bytes "t"/"f". The reading is over
// the shapes rather than over one type, and the cases below are the ones a
// driver actually produces.
func TestTruthy_ReadsTheShapesEnginesReport(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  bool
	}{
		{name: "a true bool", value: true, want: true},
		{name: "a false bool", value: false, want: false},
		{name: "a positive count", value: int64(3), want: true},
		{name: "a zero count", value: int64(0), want: false},
		{name: "a positive float", value: 1.5, want: true},
		{name: "a zero float", value: 0.0, want: false},
		{name: "the byte t", value: []byte("t"), want: true},
		{name: "the byte f", value: []byte("f"), want: false},
		{name: "the string true", value: "true", want: true},
		{name: "the string false", value: "false", want: false},
		{name: "a NULL", value: nil, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(truthy(test.value), qt.Equals, test.want)
		})
	}
}

// A value nobody recognizes is FALSE.
//
// This is the case the shapes above cannot reach through SQLite, and it is the
// one that matters most: a guard whose value cannot be read must not be treated
// as satisfied. Reading an unknown shape as true would let a purge past a
// condition the author wrote to stop it, on a driver whose type this package
// has never seen.
func TestTruthy_AnUnrecognizedShapeIsNotSatisfied(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "a timestamp", value: time.Unix(0, 0)},
		{name: "a struct", value: struct{ N int }{N: 1}},
		{name: "a slice of something else", value: []int{1}},
		{name: "a map", value: map[string]int{"n": 1}},
		{name: "a pointer", value: new(int)},
		{name: "an unrecognized string", value: "maybe"},
		{name: "an empty string", value: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(truthy(test.value), qt.IsFalse,
				qt.Commentf("%T read as satisfied, so a guard on it would not stop anything", test.value))
		})
	}
}
