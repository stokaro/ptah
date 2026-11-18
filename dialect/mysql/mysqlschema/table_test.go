package mysqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// A read records a table's character set as an observation bound to the
// MySQL family, and the declaration it converts to keeps it.
func TestWithObservedTable_HappyPath(t *testing.T) {
	c := qt.New(t)

	facets, err := mysqlschema.WithObservedTable(schemaext.Facets{}, "latin1")

	c.Assert(err, qt.IsNil)
	observed, found, err := schemaext.FacetAs[*mysqlschema.ObservedTable](facets, mysqlschema.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(*observed, qt.Equals, mysqlschema.ObservedTable{Charset: "latin1"})
	c.Assert(*observed.Desired(), qt.Equals, mysqlschema.DesiredTable{Charset: "latin1"})
	c.Assert(facets.TargetScope(mysqlschema.TableKind), qt.DeepEquals, []string{"mariadb", "mysql"})
}

// No character set adds nothing.
func TestWithObservedTable_AnEmptyCharsetAddsNothing(t *testing.T) {
	c := qt.New(t)

	facets, err := mysqlschema.WithObservedTable(schemaext.Facets{}, "")

	c.Assert(err, qt.IsNil)
	c.Assert(facets.IsZero(), qt.IsTrue)
}

// A character set that is not a name, and a second observation of the kind,
// are refused.
func TestWithObservedTable_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		facets  schemaext.Facets
		charset string
		wantErr string
	}{
		{name: "not a name", charset: "utf8mb4; DROP", wantErr: `.*MySQL charset "utf8mb4; DROP" is not a name of letters, digits and underscores`},
		{name: "a second observation", facets: must.Must(mysqlschema.WithObservedTable(schemaext.Facets{}, "utf8mb4")),
			charset: "latin1", wantErr: `.*ptah.run/mysql/table.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			facets, err := mysqlschema.WithObservedTable(test.facets, test.charset)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(facets.IsZero(), qt.IsTrue)
		})
	}
}
