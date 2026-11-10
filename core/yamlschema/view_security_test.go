package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
)

func TestParseViewSecurity_PreservesInvokerAndRefusesUnknownMode(t *testing.T) {
	c := qt.New(t)
	view, err := yamlschema.Parse(t.Context(), noOwners, []byte("views:\n  v:\n    body: SELECT 1\n    security: invoker\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(view.Views[0].Security, qt.Equals, "INVOKER")

	invalid, err := yamlschema.Parse(t.Context(), noOwners, []byte("views:\n  v:\n    body: SELECT 1\n    security: INVKOER\n"))
	c.Assert(err, qt.ErrorMatches, `(?s).*view "v": security: invalid view SQL SECURITY mode "INVKOER": want DEFINER or INVOKER`)
	c.Assert(invalid, qt.IsNil)
}
