package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
	"ptah.run/internal/viewcheck"
)

// TestParse_ViewCheckOption_HappyPath pins that a YAML view's with_check names
// the level of its check option, as a boolean or as a level.
func TestParse_ViewCheckOption_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		withCheck     string
		wantWithCheck bool
		wantLocal     bool
	}{
		{name: "absent", withCheck: ""},
		{name: "false", withCheck: "\n    with_check: false"},
		{name: "true is the cascaded level", withCheck: "\n    with_check: true", wantWithCheck: true},
		{name: "cascaded", withCheck: "\n    with_check: cascaded", wantWithCheck: true},
		{name: "local", withCheck: "\n    with_check: local", wantWithCheck: true, wantLocal: true},
		{name: "a quoted level", withCheck: "\n    with_check: \"LOCAL\"", wantWithCheck: true, wantLocal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(t.Context(), noOwners, []byte("views:\n  own_rows:\n    body: SELECT 1 AS x"+test.withCheck+"\n"))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Views, qt.HasLen, 1)
			c.Assert(db.Views[0].WithCheck, qt.Equals, test.wantWithCheck)
			c.Assert(db.Views[0].LocalCheck, qt.Equals, test.wantLocal)
		})
	}
}

// TestParse_ViewCheckOption_FailurePath pins that a with_check value naming no
// level is refused by name rather than read as no check option.
func TestParse_ViewCheckOption_FailurePath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(t.Context(), noOwners, []byte("views:\n  own_rows:\n    body: SELECT 1 AS x\n    with_check: locale\n"))

	c.Assert(err, qt.ErrorIs, viewcheck.ErrInvalidLevel)
	c.Assert(err, qt.ErrorMatches, `(?s).*view "own_rows": with_check: invalid view check option "locale": want true, false, cascaded or local`)
	c.Assert(db, qt.IsNil)
}
