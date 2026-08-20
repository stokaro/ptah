package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/matviewrefresh"
)

// TestParseMatView_RefusesTheRetiredRefreshStrategy pins the Go-annotation half
// of the refusal, including the spelling that would otherwise vanish.
//
// The bareword case is the subtle one and it fails OPEN if the registry entry is
// merely deleted rather than marked retired. An attribute written with no
// `=value` is promoted into the key/value map only while it is UNKNOWN, so a
// deleted entry would put `refresh_strategy` in no map at all: never validated,
// never refused, dropped without a word -- which is the defect this change
// exists to remove (stokaro/ptah#1625).
func TestParseMatView_RefusesTheRetiredRefreshStrategy(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
	}{
		{
			name:       "a value the model used to keep",
			annotation: `//ptah:schema:matview name="s" body="SELECT 1" refresh_strategy="concurrently"`,
		},
		{
			name:       "the value the model used to accept silently",
			annotation: `//ptah:schema:matview name="s" body="SELECT 1" refresh_strategy="manual"`,
		},
		{
			name:       "an empty value",
			annotation: `//ptah:schema:matview name="s" body="SELECT 1" refresh_strategy=""`,
		},
		{
			name:       "a bareword with no value at all",
			annotation: `//ptah:schema:matview name="s" body="SELECT 1" refresh_strategy`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := goschema.ParseSource("s.go", "package m\n\n"+test.annotation+"\ntype S struct{}\n")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrRetiredAttribute)
			c.Assert(err, qt.Not(qt.ErrorIs), ptaherr.ErrUnknownAttribute)
			c.Assert(err, qt.ErrorMatches, `.*`+matviewrefresh.Reason[:40]+`.*`)
		})
	}
}

// TestParseMatView_AcceptsAMaterializedViewWithoutTheAttribute keeps the
// refusal scoped to the attribute rather than to the object.
func TestParseMatView_AcceptsAMaterializedViewWithoutTheAttribute(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource("s.go", `package m

//ptah:schema:matview name="user_stats" body="SELECT count(*) FROM users" comment="stats"
type S struct{}
`)

	c.Assert(err, qt.IsNil)
	c.Assert(db.MaterializedViews, qt.HasLen, 1)
	c.Assert(db.MaterializedViews[0].Name, qt.Equals, "user_stats")
	c.Assert(db.MaterializedViews[0].Comment, qt.Equals, "stats")
}
