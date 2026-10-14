package migratorext_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
	"ptah.run/internal/migratorext"
)

func TestContainsIdentifierSequence(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   bool
	}{
		{name: "adjacent", source: "SELECT NEXT VALUE FOR s", want: true},
		{name: "a comment between", source: "SELECT NEXT /* x */ VALUE FOR s", want: true},
		{name: "inside a string", source: "SELECT 'NEXT VALUE FOR'", want: false},
		{name: "interrupted", source: "SELECT NEXT, VALUE FOR s", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migratorext.ContainsIdentifierSequence(test.source, lexer.Options{}, "NEXT", "VALUE", "FOR"), qt.Equals, test.want)
		})
	}
}

// A timeout rounds up, so it is never shorter than asked, and never to zero.
func TestDurationsRoundUp(t *testing.T) {
	c := qt.New(t)
	c.Assert(migratorext.Millis(1500*time.Microsecond), qt.Equals, int64(2))
	c.Assert(migratorext.Seconds(1500*time.Millisecond), qt.Equals, int64(2))
	c.Assert(migratorext.MillisLiteral(time.Second), qt.Equals, "1000ms")
}

func TestReversed_LeavesItsInputAlone(t *testing.T) {
	c := qt.New(t)
	values := []string{"a", "b", "c"}
	c.Assert(migratorext.Reversed(values), qt.DeepEquals, []string{"c", "b", "a"})
	c.Assert(values, qt.DeepEquals, []string{"a", "b", "c"})
}

func TestTable_ConfiguredOrConnectionSchema(t *testing.T) {
	c := qt.New(t)
	c.Assert(migratorext.Table{ConnectionSchema: "conn"}.ConfiguredOrConnectionSchema(), qt.Equals, "conn")
	c.Assert(migratorext.Table{ConfiguredSchema: "cfg", ConnectionSchema: "conn"}.ConfiguredOrConnectionSchema(), qt.Equals, "cfg")
}
