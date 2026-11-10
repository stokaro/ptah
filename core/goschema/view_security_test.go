package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

func TestParseViewSecurity_PreservesInvokerAndRefusesUnknownMode(t *testing.T) {
	c := qt.New(t)
	view, err := goschema.ParseSource(noOwners, "view.go", "package m\n\n//ptah:schema:view name=\"v\" body=\"SELECT 1\" security=\"invoker\"\ntype S struct{}\n")
	c.Assert(err, qt.IsNil)
	c.Assert(view.Views[0].Security, qt.Equals, "INVOKER")

	invalid, err := goschema.ParseSource(noOwners, "view.go", "package m\n\n//ptah:schema:view name=\"v\" body=\"SELECT 1\" security=\"INVKOER\"\ntype S struct{}\n")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(invalid.Views, qt.HasLen, 0)
}
