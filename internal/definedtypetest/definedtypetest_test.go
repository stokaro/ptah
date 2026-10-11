package definedtypetest_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/internal/definedtypetest"
)

// TestObject_DefinesItsType pins what a host reads off the object: the type
// under the common kind, in the object's schema, naming the references.
func TestObject_DefinesItsType(t *testing.T) {
	c := qt.New(t)

	objects, err := schemaext.NewObjects(definedtypetest.Object(objectidentity.KindDomain, "app", "zip", "text"))

	c.Assert(err, qt.IsNil)
	defined := schemaext.DefinedTypes(objects)
	c.Assert(defined, qt.HasLen, 1)
	c.Assert(defined[0].ID.Kind, qt.Equals, objectidentity.KindDomain)
	c.Assert(defined[0].ID.Schema.Source, qt.Equals, "app")
	c.Assert(defined[0].ID.Name.Source, qt.Equals, "zip")
	c.Assert(defined[0].References, qt.DeepEquals, []string{"text"})
}
