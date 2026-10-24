package yamlschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
)

// platformOwner claims the paint group of a table's platform key.
func platformOwner(decode func(context.Context, yamlext.Document) (yamlext.DocumentResult, error)) yamlext.Extension {
	return yamlext.Extension{Owner: "example.org/paint", Kinds: []schemaext.Kind{shadeKind},
		EntrySections: []yamlext.EntrySection{{Key: "platform.paint"}}, Document: decode}
}

const platformGroupSource = `tables:
  walls:
    columns:
      id: {type: INTEGER}
    platform:
      mysql: {engine: InnoDB}
      paint:
        count: 9007199254740993
        shade: teal
    overrides:
      other: {comment: kept}
  floors:
    columns:
      id: {type: INTEGER}
    platform:
      mysql: {engine: MyISAM}
`

func TestOwnerPlatformGroup_HappyPath(t *testing.T) {
	c := qt.New(t)
	var input yamlext.Document
	owners, err := yamlext.NewSet(platformOwner(func(_ context.Context, document yamlext.Document) (yamlext.DocumentResult, error) {
		input = document
		return yamlext.DocumentResult{}, nil
	}))
	c.Assert(err, qt.IsNil)

	db, err := yamlschema.Parse(t.Context(), owners, []byte(platformGroupSource))

	c.Assert(err, qt.IsNil)
	c.Assert(input.Fragments, qt.HasLen, 1, qt.Commentf("a table without the group declares nothing"))
	fragment := input.Fragments[0]
	c.Assert(fragment.Path, qt.DeepEquals, []string{"tables", "walls", "platform.paint"})
	c.Assert(fragment.Table, qt.Equals, 1)
	c.Assert(fragment.Node.Kind, qt.Equals, yamlext.MappingNode)
	c.Assert(fragment.Node.Content[1].Value, qt.Equals, "9007199254740993")
	c.Assert(fragment.Node.Content[1].Tag, qt.Equals, "!!int")
	c.Assert(fragment.Node.Range.Start.Line, qt.Equals, 8)
	c.Assert(db.Tables[1].Overrides, qt.DeepEquals, map[string]map[string]string{"mysql": {"engine": "InnoDB"}, "other": {"comment": "kept"}},
		qt.Commentf("the claimed group is the owner's; every other group stays"))
	c.Assert(db.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{"mysql": {"engine": "MyISAM"}})
}

// TestOwnerPlatformGroup_KeepsTheGroupWithoutAnOwner is the control: a run that
// selects no owner of the group keeps it in the overrides, so the removal above
// belongs to the claim.
func TestOwnerPlatformGroup_KeepsTheGroupWithoutAnOwner(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(t.Context(), yamlext.None(), []byte(platformGroupSource))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[1].Overrides["paint"], qt.DeepEquals, map[string]string{"count": "9007199254740993", "shade": "teal"})
}

func TestOwnerPlatformGroup_GroupUnderOverridesIsNotDropped(t *testing.T) {
	c := qt.New(t)
	owners, err := yamlext.NewSet(platformOwner(func(context.Context, yamlext.Document) (yamlext.DocumentResult, error) {
		return yamlext.DocumentResult{}, nil
	}))
	c.Assert(err, qt.IsNil)

	db, err := yamlschema.Parse(t.Context(), owners, []byte("tables:\n  walls:\n    columns:\n      id: {type: INTEGER}\n    overrides:\n      paint: {shade: teal}\n"))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{"paint": {"shade": "teal"}},
		qt.Commentf("only the platform key is the owner's, and a group the owner is not handed is kept for the consumer to refuse"))
}
