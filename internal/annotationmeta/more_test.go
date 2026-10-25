package annotationmeta_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/internal/annotationmeta"
)

// gadgetOwner is a set whose one owner declares directive.
func gadgetOwner(c *qt.C, owner, directive string) annotation.Set {
	c.Helper()
	set, err := annotation.NewSet(annotation.Extension{
		Owner:      owner,
		Coverage:   annotation.Unlimited(func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }),
		Directives: []annotation.Directive{{Name: directive, Scopes: []annotation.Scope{annotation.ScopeStruct}}},
		Decode:     func(annotation.Declaration) ([]annotation.Contribution, error) { return nil, nil },
	})
	c.Assert(err, qt.IsNil)
	return set
}

// A catalog over several sets knows the directives and the attributes of each,
// the further sets' after the first's.
func TestNewCatalog_JoinsFurtherSets(t *testing.T) {
	c := qt.New(t)

	catalog, err := annotationmeta.NewCatalog(gadgetOwner(c, "example.org/first", "ptah:schema:gadget"),
		gadgetOwner(c, "example.org/second", "ptah:schema:widget"), shadeOwner(c, "ptah:schema:matview", "shade"))

	c.Assert(err, qt.IsNil)
	_, gadget := catalog.Lookup("ptah:schema:gadget")
	_, widget := catalog.Lookup("ptah:schema:widget")
	c.Assert(gadget, qt.IsTrue)
	c.Assert(widget, qt.IsTrue)
	c.Assert(catalog.AllowsAttribute("ptah:schema:matview", "shade"), qt.IsTrue)
	names := catalog.Directives()
	c.Assert(names[len(names)-2].Name, qt.Equals, "ptah:schema:gadget")
	c.Assert(names[len(names)-1].Name, qt.Equals, "ptah:schema:widget")
}

// Two sets that declare one directive, or add one attribute to a frontend
// directive, are refused rather than joined in order, and an unselected
// further set is refused as an unselected first set is.
func TestNewCatalog_FailurePath_FurtherSets(t *testing.T) {
	c := qt.New(t)

	_, err := annotationmeta.NewCatalog(gadgetOwner(c, "example.org/first", "ptah:schema:gadget"),
		gadgetOwner(c, "example.org/second", "ptah:schema:gadget"))
	c.Assert(err, qt.ErrorMatches, `directive "ptah:schema:gadget" of example.org/second takes the name of another owner's`)

	_, err = annotationmeta.NewCatalog(shadeOwner(c, "ptah:schema:matview", "shade"), shadeOwner(c, "ptah:schema:matview", "shade"))
	c.Assert(err, qt.ErrorMatches, `an owner adds attribute "shade" to "ptah:schema:matview", which declares it already`)

	_, err = annotationmeta.NewCatalog(annotation.None(), gadgetOwner(c, "example.org/first", "ptah:schema:table"))
	c.Assert(err, qt.ErrorMatches, `directive "ptah:schema:table" of example.org/first takes the name of one of the frontend's own`)

	_, err = annotationmeta.NewCatalog(annotation.None(), shadeOwner(c, "ptah:schema:gadget", "shade"))
	c.Assert(err, qt.ErrorMatches, `an owner adds attributes to "ptah:schema:gadget", which is not one of the frontend's own directives`)

	_, err = annotationmeta.NewCatalog(annotation.None(), annotation.Set{})
	c.Assert(err, qt.ErrorIs, annotation.ErrUnselected)
}
