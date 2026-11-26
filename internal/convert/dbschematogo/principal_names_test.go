package dbschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// Database export must name the holder the catalog reported, including a
// space that distinguishes a role from PUBLIC or from an unpadded role.
func TestConvertDBSchemaToGoSchema_KeepsPrincipalNames(t *testing.T) {
	for _, test := range []struct{ dialect, role, quoted string }{
		{dialect: "postgres", role: " reader ", quoted: `" reader "`},
		{dialect: "mariadb", role: " reader", quoted: "` reader`"},
		{dialect: "mariadb", role: "reader\t", quoted: "`reader\t`"},
		{dialect: "oracle", role: " reader ", quoted: `" reader "`},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{StoredNames: true,
				Grants: []catalog.Grant{{Role: test.role, Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}},
			}

			desired, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), current, test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(desired.Grants, qt.HasLen, 1)
			c.Assert(desired.Grants[0].Role, qt.Equals, test.role)
			statements, err := builtin.GetOrderedCreateStatements(desired, test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, " TO "+test.quoted+";")
		})
	}
}
