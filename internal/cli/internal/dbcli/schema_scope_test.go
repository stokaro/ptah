package dbcli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/dbcli"
)

// The selection guard receives decoded names, so a comma or whitespace inside
// a quoted identifier is not another selection or trimmed identifier.
func TestParseSchemasForURLs_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, raw, address string
		want               []string
	}{
		{name: "one database", raw: "m1,m1", address: "mysql://localhost/m1", want: []string{"m1"}},
		{name: "comma", raw: `"m1,m2"`, address: "mysql://localhost/m1%2Cm2", want: []string{"m1,m2"}},
		{name: "space", raw: `" m1"`, address: "mysql://localhost/%20m1", want: []string{" m1"}},
		{name: "server selection", raw: "m1, m2", address: "mariadb://localhost/", want: []string{"m1", "m2"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := dbcli.ParseSchemasForURLs(test.raw, test.address)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// Both sides of a comparison are checked before either is opened. A valid
// first URL cannot hide a conflict in the second one.
func TestParseSchemasForURLs_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := dbcli.ParseSchemasForURLs("m1", "mysql://localhost/m1", "mysql://localhost/m2")

	c.Assert(err, qt.ErrorMatches, `schema selection names database "m1", but the URL is limited to database "m2"; .*`)
	c.Assert(got, qt.IsNil)
}
