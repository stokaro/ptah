package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

// TestParseViewCheckOption_HappyPath pins that a view's with_check attribute
// names the level of its check option: true and cascaded are WITH CHECK
// OPTION, local is WITH LOCAL CHECK OPTION.
func TestParseViewCheckOption_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		attribute     string
		wantWithCheck bool
		wantLocal     bool
	}{
		{name: "absent", attribute: ""},
		{name: "false", attribute: ` with_check="false"`},
		{name: "true is the cascaded level", attribute: ` with_check="true"`, wantWithCheck: true},
		{name: "cascaded", attribute: ` with_check="cascaded"`, wantWithCheck: true},
		{name: "local", attribute: ` with_check="local"`, wantWithCheck: true, wantLocal: true},
		{name: "LOCAL in upper case", attribute: ` with_check="LOCAL"`, wantWithCheck: true, wantLocal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(noOwners, "s.go", "package m\n\n//ptah:schema:view name=\"own_rows\" body=\"SELECT 1 AS x\""+test.attribute+"\ntype S struct{}\n")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Views, qt.HasLen, 1)
			c.Assert(db.Views[0].WithCheck, qt.Equals, test.wantWithCheck)
			c.Assert(db.Views[0].LocalCheck, qt.Equals, test.wantLocal)
		})
	}
}

// TestParseViewCheckOption_FailurePath pins that a with_check value naming no
// level is refused by name: read as no check option, it would create a view
// that checks nothing.
func TestParseViewCheckOption_FailurePath(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(noOwners, "s.go", "package m\n\n//ptah:schema:view name=\"own_rows\" body=\"SELECT 1 AS x\" with_check=\"locale\"\ntype S struct{}\n")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `invalid "with_check" value "locale" on //ptah:schema:view at .*: invalid view check option "locale": want true, false, cascaded or local`)
	c.Assert(db.Views, qt.HasLen, 0)
}
