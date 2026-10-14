package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/definedtypetest"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A column typed by a domain is compared by the domain's identity, and a
// domain a feature owner's object defines resolves an unqualified reference.
// The database column holds public.status, and the declaration names status.
// When the owner's object puts its status in schema other, the bare
// reference means that one, which is a change. The control shows the same
// comparison without a declaration, which leaves the bare reference to the
// name.
func TestTableColumnsWithSemantics_ResolvesADomainAnOwnerDefines(t *testing.T) {
	tests := []struct {
		name       string
		objects    []schemaext.Object
		wantChange []string
	}{
		{name: "an owner's domain in another schema",
			objects:    []schemaext.Object{definedtypetest.Object(objectidentity.KindDomain, "other", "status", "text")},
			wantChange: []string{"status -> status"}},
		{name: "control: no declaration"},
		{name: "an owner's composite type of the same name is no domain",
			objects: []schemaext.Object{definedtypetest.Object(objectidentity.KindComposite, "other", "status", "text")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := schemamodel.Table{StructName: "T", Name: "t"}
			desired := &schemamodel.Database{
				Tables:         []schemamodel.Table{table},
				Fields:         []schemamodel.Field{{StructName: "T", Name: "s", Type: "status"}},
				FeatureObjects: must.Must(schemaext.NewObjects(test.objects...)),
			}
			current := catalog.Table{Name: "t", Type: "TABLE", Columns: []catalog.Column{{
				Name: "s", DataType: "text", UDTName: "text", FormattedType: "status", DomainName: "status", DomainSchema: "public", IsNullable: "NO",
			}}}

			diff := compare.TableColumnsWithSemantics(table, current, desired, platform.Postgres, identifier.ForDialect(platform.Postgres))

			var changes []string
			for _, column := range diff.ColumnsModified {
				changes = append(changes, column.Changes["type"])
			}
			c.Assert(changes, qt.DeepEquals, test.wantChange)
		})
	}
}
