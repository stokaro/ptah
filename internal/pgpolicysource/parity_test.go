package pgpolicysource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

const parityGo = `package models

//ptah:schema:table name="docs" schema="app"
//ptah:schema:rls:enable table="docs" comment="tenants" dialects="postgres"
//ptah:schema:rls:policy name="readers" table="docs" for="UPDATE" to="reader, auditor" using="tenant_id = 1" with_check="true" as="restrictive" comment="read own" dialects="postgres"
type Doc struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`

const parityYAML = `tables:
  docs:
    schema: app
    struct_name: Doc
    columns:
      id: { type: INTEGER, primary: true }
rls_enabled_tables:
  docs:
    table: docs
    comment: tenants
    dialects: [postgres]
rls_policies:
  readers:
    struct_name: Doc
    table: docs
    for: UPDATE
    to: reader, auditor
    using: tenant_id = 1
    with_check: "true"
    as: restrictive
    comment: read own
    dialects: [postgres]
`

// TestReaders_OneDeclarationDecodesAlikeFromGoAndYAML pins that both
// frontends hand the owner one declaration the same way: a policy and a
// table's switches written in Go and in YAML decode to the same objects and
// the same facet, scope included.
func TestReaders_OneDeclarationDecodesAlikeFromGoAndYAML(t *testing.T) {
	c := qt.New(t)

	fromGo := must.Must(goschema.ParseSource(must.Must(annotation.NewSet(pgpolicysource.Annotations())), "docs.go", parityGo))
	fromYAML := must.Must(yamlschema.Parse(must.Must(yamlext.NewSet(pgpolicysource.YAML())), []byte(parityYAML)))

	goObjects := must.Must(fromGo.FeatureObjects.All())
	c.Assert(goObjects, qt.HasLen, 1)
	c.Assert(must.Must(fromYAML.FeatureObjects.All()), qt.DeepEquals, goObjects)
	c.Assert(switchesOf(c, fromYAML), qt.DeepEquals, switchesOf(c, &fromGo))
	c.Assert(fromYAML.Tables[0].Facets.Equal(fromGo.Tables[0].Facets), qt.IsTrue)
}

func switchesOf(c *qt.C, db *schemamodel.Database) *pgpolicy.DesiredTableState {
	c.Helper()
	state, found, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](db.Tables[0].Facets, pgpolicy.TableStateKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return state
}
