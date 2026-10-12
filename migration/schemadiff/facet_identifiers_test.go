package schemadiff_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

var errNamesCaptured = errors.New("identifier names captured")

type nameCaptureConnection struct{ names []string }

func (*nameCaptureConnection) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: "sqlserver", Schema: "dbo"}
}
func (c *nameCaptureConnection) ResolveIdentifierSemantics(_ context.Context, names []string) (identifier.Semantics, error) {
	c.names = append([]string(nil), names...)
	return identifier.Semantics{}, errNamesCaptured
}
func (*nameCaptureConnection) WithRolledBackTransaction(context.Context, string, func(context.Context, *sql.Tx) error) (bool, error) {
	return false, nil
}

type identifierFacet struct{ name string }

func (v identifierFacet) Kind() schemaext.Kind             { return "ptah.run/test/identifier" }
func (v identifierFacet) Clone() schemaext.Value           { return v }
func (v identifierFacet) Equal(other schemaext.Value) bool { return other == v }
func (v identifierFacet) IdentifierNames() []string        { return []string{v.name} }

// The public comparison resolves owner names from both representations and
// excludes facets bound to another target before contacting the catalog.
func TestCompareWithDatabase_ResolvesActiveFacetIdentifiers(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope []string
		want  []string
	}{
		{"active", []string{"sqlserver"}, []string{"dbo", "declared", "observed"}},
		{"excluded", []string{"postgres"}, []string{"dbo"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := &nameCaptureConnection{}
			desired := must.Must(schemaext.NewFacets(identifierFacet{"declared"}))
			current := must.Must(schemaext.NewFacets(identifierFacet{"observed"}))
			desired = must.Must(desired.WithTargetScope(identifierFacet{}.Kind(), test.scope...))
			current = must.Must(current.WithTargetScope(identifierFacet{}.Kind(), test.scope...))
			diff, err := schemadiff.CompareWithDatabase(c.Context(), conn,
				&schemamodel.Database{Functions: []schemamodel.Function{{Name: "f", Facets: desired}}},
				&catalog.Database{Functions: []catalog.Function{{Name: "f", Facets: current}}}, nil, must.Must(builtin.New()))
			c.Assert(err, qt.ErrorIs, errNamesCaptured)
			c.Assert(diff, qt.IsNil)
			c.Assert(conn.names, qt.DeepEquals, test.want)
		})
	}
}
