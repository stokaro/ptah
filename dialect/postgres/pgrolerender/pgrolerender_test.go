package pgrolerender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgrole"
	"ptah.run/dialect/postgres/pgrolerender"
)

// A new role takes its declared values in the statements after CREATE ROLE,
// and the facet is consumed: nothing is left for CREATE ROLE to carry.
func TestLowerRoleFacets_HappyPath(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(pgrole.WithSettings(schemaext.Facets{}, []pgrole.Setting{{Name: "statement_timeout", Value: "'30s'"}, {Name: "work_mem", Reset: true}}))

	payloads, rest, err := pgrolerender.LowerRoleFacets("app", facets)

	c.Assert(err, qt.IsNil)
	c.Assert(rest.IsZero(), qt.IsTrue)
	c.Assert(payloads, qt.HasLen, 1)
	statements, err := payloads[0].(*pgrole.AlterSettings).Statements()
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{`ALTER ROLE "app" SET "statement_timeout" = '30s';`})
}

// A declaration of resets alone asks nothing of a new role, which holds no
// setting.
func TestLowerRoleFacets_ResetsOnly(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(pgrole.WithSettings(schemaext.Facets{}, []pgrole.Setting{{Name: "work_mem", Reset: true}}))

	payloads, rest, err := pgrolerender.LowerRoleFacets("app", facets)

	c.Assert(err, qt.IsNil)
	c.Assert(rest.IsZero(), qt.IsTrue)
	c.Assert(payloads, qt.IsNil)
}

func TestLowerRoleFacets_FailurePath(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(schemaext.NewFacets(&pgrole.DesiredSettings{Settings: []pgrole.Setting{{Name: "work_mem", Value: "1MB"}}}))

	payloads, rest, err := pgrolerender.LowerRoleFacets("app", facets)

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(payloads, qt.IsNil)
	c.Assert(rest.IsZero(), qt.IsTrue)
}

func TestHandlers_HappyPath(t *testing.T) {
	c := qt.New(t)
	handler := pgrolerender.Handlers()[0]
	operation := &pgrole.AlterSettings{Role: "app", Change: pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "work_mem", Before: new("64MB")}}}}
	extension := renderer.ExtensionContext{Target: "postgres"}

	c.Assert(handler.Role, qt.Equals, ast.StatementExtension)
	c.Assert(handler.Validate(extension, operation), qt.IsNil)
	statements, err := handler.Render(extension, operation)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{`ALTER ROLE "app" RESET "work_mem";`})
}

// No target but PostgreSQL renders the statement: CockroachDB and YugabyteDB
// keep role settings in catalogs no reader of theirs promises.
func TestHandlers_FailurePath(t *testing.T) {
	for _, target := range []string{"cockroachdb", "yugabytedb", "mysql"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			operation := &pgrole.AlterSettings{Role: "app", Change: pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "work_mem", Before: new("64MB")}}}}
			err := pgrolerender.Handlers()[0].Validate(renderer.ExtensionContext{Target: target}, operation)
			c.Assert(err, qt.ErrorMatches, `.*`+target+`.*`)
		})
	}
}
