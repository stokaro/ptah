package dbtest_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/dbtest"
)

// atlasForEachDocument renders a one-case suite whose `for_each` is the
// expression given and whose single step carries the iteration's value into its
// SQL, so a parsed case says which element it came from.
func atlasForEachDocument(forEach string) []byte {
	return fmt.Appendf(nil, `
test "schema" "row" {
  for_each = %s
  exec {
    sql = "SELECT '${each.value}'"
  }
}
`, forEach)
}

// atlasExecByCaseName parses a suite and maps each expanded case's name to the
// SQL its step carries.
//
// Names alone cannot show the defect this file is about: a positional and a
// keyed naming both produce two names for a two-entry mapping. What
// distinguishes them is which element a given name resolves to, so the value
// has to travel with the name.
func atlasExecByCaseName(c *qt.C, forEach string) map[string]string {
	cases, err := dbtest.ParseAtlasTestCases(
		atlasForEachDocument(forEach),
		"s.test.hcl",
		dbtest.AtlasTestKindSchema,
	)
	c.Assert(err, qt.IsNil)

	byName := make(map[string]string, len(cases))
	for _, one := range cases {
		c.Assert(one.Steps, qt.HasLen, 1)
		byName[one.Name] = one.Steps[0].Exec
	}
	return byName
}

// TestParseAtlasTestCases_ForEachInstanceNames_HappyPath pins how each kind of
// `for_each` names its expanded instances.
//
// A mapping is named by its key and a collection by its 1-based position. The
// difference is not cosmetic: a collection element has no identity but its
// position, while a mapping key is an identity the author chose, and using the
// position for both throws that away.
func TestParseAtlasTestCases_ForEachInstanceNames_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		forEach string
		want    map[string]string
	}{
		{
			name:    "a mapping is named by its keys",
			forEach: `{ alpha = "a", beta = "b" }`,
			want: map[string]string{
				"row/alpha": "SELECT 'a'",
				"row/beta":  "SELECT 'b'",
			},
		},
		{
			name:    "a collection is named by position, one-based",
			forEach: `["x", "y"]`,
			want: map[string]string{
				"row/1": "SELECT 'x'",
				"row/2": "SELECT 'y'",
			},
		},
		{
			name:    "a mapping key that looks like an ordinal is still a key",
			forEach: `{ "2" = "two", "1" = "one" }`,
			want: map[string]string{
				"row/1": "SELECT 'one'",
				"row/2": "SELECT 'two'",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasExecByCaseName(c, test.forEach)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestParseAtlasTestCases_AMappingInstanceKeepsItsNameWhenAKeyIsAdded is the
// property the naming exists for, and the one a positional naming fails.
//
// A mapping iterates in sorted key order, so a positional name moves whenever a
// key sorting earlier is added. Measured on the implementation before this
// test: `{ alpha = "a", beta = "b" }` named its first instance `row/1`, and
// adding an unrelated `aaa` left `row/1` naming a different case. Nothing said
// so -- a `--run row/1` pinned in continuous integration kept passing against
// something else, and a report naming `row/2` could not be traced back to the
// key whose case failed.
//
// The collection half is the control. There the position IS the identity, so an
// element inserted ahead of another is expected to renumber it, and a fix that
// achieved stability by abandoning positional names everywhere would fail here.
func TestParseAtlasTestCases_AMappingInstanceKeepsItsNameWhenAKeyIsAdded(t *testing.T) {
	t.Run("a mapping key added ahead of another moves neither name", func(t *testing.T) {
		c := qt.New(t)

		before := atlasExecByCaseName(c, `{ alpha = "a", beta = "b" }`)
		after := atlasExecByCaseName(c, `{ alpha = "a", beta = "b", aaa = "inserted" }`)

		c.Assert(before["row/alpha"], qt.Equals, "SELECT 'a'")
		c.Assert(after["row/alpha"], qt.Equals, "SELECT 'a'")
		c.Assert(after["row/beta"], qt.Equals, "SELECT 'b'")
		c.Assert(after["row/aaa"], qt.Equals, "SELECT 'inserted'")
	})

	t.Run("a collection element inserted ahead of another renumbers it", func(t *testing.T) {
		c := qt.New(t)

		before := atlasExecByCaseName(c, `["x", "y"]`)
		after := atlasExecByCaseName(c, `["inserted", "x", "y"]`)

		c.Assert(before["row/1"], qt.Equals, "SELECT 'x'")
		c.Assert(after["row/1"], qt.Equals, "SELECT 'inserted'")
		c.Assert(after["row/2"], qt.Equals, "SELECT 'x'")
	})
}
