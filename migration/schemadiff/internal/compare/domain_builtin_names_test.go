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

// A domain declaration must not capture a built-in type of the same name.
// The controls still report either conversion between the two identities.
func TestDomainDeclarationKeepsBuiltinTypeIdentity(t *testing.T) {
	builtin := catalog.Column{Name: "amount", DataType: "money", UDTName: "money", UDTSchema: "pg_catalog", IsNullable: "YES"}
	domain := catalog.Column{Name: "amount", DataType: "numeric", UDTName: "numeric", UDTSchema: "pg_catalog",
		DomainName: "money", DomainSchema: "public", FormattedType: "public.money", IsNullable: "YES"}
	for _, test := range []struct {
		name    string
		current catalog.Column
		desired string
		changes int
	}{
		{name: "bare built-in", current: builtin, desired: "money"},
		{name: "quoted built-in", current: builtin, desired: `"money"`},
		{name: "qualified built-in", current: builtin, desired: "pg_catalog.money"},
		{name: "explicit user domain", current: domain, desired: "public.money"},
		{name: "built-in becomes a user domain", current: builtin, desired: "public.money", changes: 1},
		{name: "user domain becomes a built-in", current: domain, desired: "money", changes: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := schemamodel.Table{StructName: "T", Name: "t", Schema: "public"}
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{table},
				Fields: []schemamodel.Field{{StructName: "T", Name: "amount", Type: test.desired, Nullable: true}},
				FeatureObjects: must.Must(schemaext.NewObjects(
					definedtypetest.Object(objectidentity.KindDomain, "public", "money", "numeric"))),
			}
			current := catalog.Table{Name: "t", Schema: "public", Type: "TABLE", Columns: []catalog.Column{test.current}}

			diff := compare.TableColumnsWithSemantics(table, current, desired, platform.Postgres, identifier.ForDialect(platform.Postgres))

			c.Assert(diff.ColumnsModified, qt.HasLen, test.changes)
		})
	}
}
