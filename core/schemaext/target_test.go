package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
)

func TestTargetSelectionOwnsItsNames(t *testing.T) {
	c := qt.New(t)
	aliases := []string{"alternate", "short"}
	selected := must.Must(schemaext.NewTargetSelection("custom", aliases...))
	aliases[0] = "other"
	names := selected.Names()
	c.Assert(names, qt.DeepEquals, []string{"alternate", "custom", "short"})
	names[0] = "changed"
	c.Assert(selected.Name(), qt.Equals, "custom")
	c.Assert(selected.Validate(), qt.IsNil)
	c.Assert(selected.Includes([]string{" ALTERNATE "}), qt.IsTrue)
	c.Assert(selected.Includes([]string{"other", "changed", "postgres"}), qt.IsFalse)
	c.Assert(selected.Includes(nil), qt.IsTrue)
	c.Assert(selected.Includes([]string{"custom"}), qt.IsTrue)
}

func TestTargetSelectionRejectsUnresolvedAndInvalidNames(t *testing.T) {
	c := qt.New(t)
	var unresolved schemaext.TargetSelection
	c.Assert(unresolved.Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(unresolved.Includes(nil), qt.IsFalse)
	for _, test := range []struct {
		name    string
		aliases []string
	}{
		{}, {name: "UPPER"}, {name: "1custom"}, {name: "bad.name"}, {name: "K"},
		{name: "custom", aliases: []string{"custom"}}, {name: "custom", aliases: []string{"alias", "alias"}},
		{name: "custom", aliases: []string{" ALIAS "}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selected, err := schemaext.NewTargetSelection(test.name, test.aliases...)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(selected.Name(), qt.Equals, "")
			c.Assert(selected.Names(), qt.HasLen, 0)
			c.Assert(selected.Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

func TestTargetSelectionDoesNotFoldUnicodeIntoAnASCIIName(t *testing.T) {
	c := qt.New(t)
	selected := must.Must(schemaext.NewTargetSelection("k"))
	c.Assert(selected.Includes([]string{"K"}), qt.IsTrue)
	c.Assert(selected.Includes([]string{"K"}), qt.IsFalse)
	spelling, err := schemaext.NormalizeTargetSpelling("  MY_TARGET-2 ")
	c.Assert(err, qt.IsNil)
	c.Assert(spelling, qt.Equals, "my_target-2")
}
