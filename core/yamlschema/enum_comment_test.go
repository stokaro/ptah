package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
)

// A YAML enum declares its comment as the other sources do
// (stokaro/ptah#3646). The list form, which has no room for one, still reads.
func TestParse_EnumComment(t *testing.T) {
	c := qt.New(t)
	document := `enums:
  mood:
    values: [ok, bad]
    comment: how it went
  plain: [a, b]
`

	db, err := yamlschema.Parse([]byte(document))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Enums, qt.HasLen, 2)
	c.Assert([]string{db.Enums[0].Name, db.Enums[0].Comment}, qt.DeepEquals, []string{"mood", "how it went"})
	c.Assert([]string{db.Enums[1].Name, db.Enums[1].Comment}, qt.DeepEquals, []string{"plain", ""})
}
