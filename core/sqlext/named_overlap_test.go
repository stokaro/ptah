package sqlext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlext"
)

func namedPattern(name string, words, after, next, notNext []string) sqlext.Pattern {
	return sqlext.Pattern{Name: name, Site: sqlext.Statement, Words: words, AfterName: after, Next: next, NotNext: notNext}
}

// TestNewSet_ShiftedNames_Disjoint accepts named statements told apart by
// fixed words beyond differently positioned names, in either registration order.
func TestNewSet_ShiftedNames_Disjoint(t *testing.T) {
	tests := []struct {
		name string
		a, b sqlext.Pattern
	}{
		{name: "optional ONLY", a: namedPattern("a", []string{"ALTER", "TABLE"}, []string{"ENABLE", "ROW", "LEVEL", "SECURITY"}, nil, nil), b: namedPattern("b", []string{"ALTER", "TABLE", "ONLY"}, []string{"ENABLE", "ROW", "LEVEL", "SECURITY"}, nil, nil)},
		{name: "next excludes shifted tail", a: namedPattern("a", []string{"CREATE"}, []string{"AS"}, []string{"ENUM"}, nil), b: namedPattern("b", []string{"CREATE", "TYPE"}, []string{"RANGE"}, nil, nil)},
		{name: "not next excludes shifted tail", a: namedPattern("a", []string{"CREATE"}, []string{"AS"}, nil, []string{"RANGE", "."}), b: namedPattern("b", []string{"CREATE", "TYPE"}, []string{"RANGE"}, nil, nil)},
		{name: "qualified name conflicts with fixed tail", a: namedPattern("a", []string{"CREATE"}, []string{"AS"}, nil, nil), b: namedPattern("b", []string{"CREATE", "TYPE", "."}, []string{"UNDER"}, nil, nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			forward, err := sqlext.NewSet(extension("example.org/a", "a", shortParser{}, test.a), extension("example.org/b", "a", shortParser{}, test.b))
			c.Assert(err, qt.IsNil)
			c.Assert(forward.Selected(), qt.IsTrue)
			backward, err := sqlext.NewSet(extension("example.org/b", "a", shortParser{}, test.b), extension("example.org/a", "a", shortParser{}, test.a))
			c.Assert(err, qt.IsNil)
			c.Assert(backward.Selected(), qt.IsTrue)
		})
	}
}

// TestNewSet_ShiftedNames_Overlap rejects witnesses whose names consume a
// fixed word of the other pattern, including a qualified name.
func TestNewSet_ShiftedNames_Overlap(t *testing.T) {
	tests := []struct {
		name string
		a, b sqlext.Pattern
	}{
		{name: "shifted FORCE", a: namedPattern("a", []string{"ALTER", "TABLE"}, []string{"NO", "FORCE", "ROW", "LEVEL", "SECURITY"}, nil, nil), b: namedPattern("b", []string{"ALTER", "TABLE", "ONLY"}, []string{"FORCE", "ROW", "LEVEL", "SECURITY"}, nil, nil)},
		{name: "next admits shifted tail", a: namedPattern("a", []string{"CREATE"}, []string{"AS"}, []string{"RANGE"}, nil), b: namedPattern("b", []string{"CREATE", "TYPE"}, []string{"RANGE"}, nil, nil)},
		{name: "unrestricted suffix", a: namedPattern("a", []string{"CREATE"}, []string{"AS"}, nil, nil), b: namedPattern("b", []string{"CREATE", "TYPE"}, []string{"RANGE"}, nil, nil)},
		{name: "qualified name", a: namedPattern("a", []string{"CREATE"}, []string{"AS"}, nil, nil), b: namedPattern("b", []string{"CREATE", "TYPE", "."}, []string{"AS"}, nil, nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			forward, err := sqlext.NewSet(extension("example.org/a", "a", shortParser{}, test.a), extension("example.org/b", "a", shortParser{}, test.b))
			c.Assert(err, qt.ErrorMatches, `duplicate.*can match the same statement syntax`)
			c.Assert(forward.Selected(), qt.IsFalse)
			backward, err := sqlext.NewSet(extension("example.org/b", "a", shortParser{}, test.b), extension("example.org/a", "a", shortParser{}, test.a))
			c.Assert(err, qt.ErrorMatches, `duplicate.*can match the same statement syntax`)
			c.Assert(backward.Selected(), qt.IsFalse)
		})
	}
}
