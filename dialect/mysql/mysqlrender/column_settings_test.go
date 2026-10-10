package mysqlrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlrender"
	"ptah.run/dialect/mysql/mysqlschema"
)

type otherValue struct{}

func (*otherValue) Kind() schemaext.Kind               { return "example.org/other" }
func (v *otherValue) Clone() schemaext.Value           { return v }
func (v *otherValue) Equal(other schemaext.Value) bool { return other == schemaext.Value(v) }

func TestColumnClauses_HappyPath(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(mysqlschema.WithObservedColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{Charset: "latin1", OnUpdate: "NOW()"}))

	charset, onUpdate, err := mysqlrender.ColumnClauses(facets)

	c.Assert(err, qt.IsNil)
	c.Assert(charset, qt.Equals, "latin1")
	c.Assert(onUpdate, qt.Equals, "NOW()")
	c.Assert(mysqlrender.ValidateColumnFacets(facets), qt.IsNil)
}

func TestValidateColumnFacets_RefusesAnotherKind(t *testing.T) {
	c := qt.New(t)

	err := mysqlrender.ValidateColumnFacets(must.Must(schemaext.NewFacets(&otherValue{})))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*feature facet "example.org/other" is not registered for MySQL-family columns`)
}
