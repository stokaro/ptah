package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/atlasfilter"
)

// A database a selection keeps part of is recorded as partial, because DROP
// DATABASE would take the part the selection left out (stokaro/ptah#6797).

// serverRead is a read of a whole MySQL server holding app, with a, and db,
// with t and u. partial is what the read already held only part of.
func serverRead(partial ...string) *catalog.Database {
	return &catalog.Database{
		Schemas:        []catalog.Schema{{Name: "app"}, {Name: "db"}},
		Tables:         []catalog.Table{{Schema: "app", Name: "a"}, {Schema: "db", Name: "t"}, {Schema: "db", Name: "u"}},
		PartialSchemas: partial,
	}
}

func TestExcludeDatabase_PartialSchemas(t *testing.T) {
	tests := []struct {
		name    string
		partial []string
		exclude []string
		want    []string
	}{
		{name: "a table", exclude: []string{"db.t"}, want: []string{"db"}},
		{name: "every table of a database", exclude: []string{"db.*"}, want: []string{"db"}},
		{name: "the database itself", exclude: []string{"db"}},
		{name: "a name the read does not hold", exclude: []string{"db.nope"}},
		{name: "a kind the read leaves out", exclude: []string{"*[type=function]"}, want: []string{"app", "db"}},
		{name: "a database the read held part of", partial: []string{"app"}, exclude: []string{"db.t"}, want: []string{"app", "db"}},
		{name: "a partial database the selection leaves out", partial: []string{"app"}, exclude: []string{"app"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabase(ownerSelections(), serverRead(test.partial...), test.exclude)

			c.Assert(err, qt.IsNil)
			c.Assert(got.PartialSchemas, qt.DeepEquals, test.want)
		})
	}
}

func TestScopeDatabase_PartialSchemas(t *testing.T) {
	tests := []struct {
		name    string
		include []string
		want    []string
	}{
		{name: "a table of a database", include: []string{"db.u"}, want: []string{"db"}},
		{name: "every object of a database", include: []string{"db.*"}},
		{name: "every table of a database, which leaves the other kinds out of the read", include: []string{"db.*[type=table]"}, want: []string{"db"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ScopeDatabase(serverRead(), atlasfilter.Scope{Selections: ownerSelections(), Include: test.include})

			c.Assert(err, qt.IsNil)
			c.Assert(got.PartialSchemas, qt.DeepEquals, test.want)
		})
	}
}
