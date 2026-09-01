package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaprep"
)

func TestEffectiveFieldForPlatform(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	field := schemamodel.Field{
		Type:    "SERIAL",
		Default: "1",
		Overrides: map[string]map[string]string{
			"pgx": {
				"type":         "BIGINT",
				"default_expr": "next_value()",
			},
		},
	}

	got := schemaprep.EffectiveFieldForPlatform(field, platform.Postgres)
	c.Assert(got.Type, qt.Equals, "BIGINT")
	c.Assert(got.Default, qt.Equals, "")
	c.Assert(got.DefaultSet, qt.IsFalse)
	c.Assert(got.DefaultExpr, qt.Equals, "next_value()")
	c.Assert(field.Type, qt.Equals, "SERIAL")
}

func TestEffectiveFieldForPlatformKeepsRawType(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	for _, field := range []schemamodel.Field{
		{Type: "TEXT", TypeRawSQL: true},
		{Type: "TEXT", TypeIsDeclaredText: true},
	} {
		c.Assert(schemaprep.EffectiveFieldForPlatform(field, platform.SQLServer).Type, qt.Equals, "TEXT")
	}
}

func TestEffectiveFieldForPlatformMapsPortableTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		dialect string
		field   string
		want    string
	}{
		{platform.MySQL, "SERIAL", "INT"},
		{platform.MariaDB, "BIGSERIAL", "BIGINT"},
		{platform.SQLServer, "SERIAL", "INT"},
		{platform.SQLServer, "TEXT", "NVARCHAR(MAX)"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.field, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			got := schemaprep.EffectiveFieldForPlatform(
				schemamodel.Field{Type: test.field},
				test.dialect,
			)
			c.Assert(got.Type, qt.Equals, test.want)
		})
	}
}

func TestEffectiveFieldForPlatformDefaultsAreMutuallyExclusive(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	literal := schemaprep.EffectiveFieldForPlatform(schemamodel.Field{
		DefaultExpr: "old()",
		Overrides: map[string]map[string]string{
			platform.Postgres: {"default": "new"},
		},
	}, platform.Postgres)
	c.Assert(literal.Default, qt.Equals, "new")
	c.Assert(literal.DefaultSet, qt.IsTrue)
	c.Assert(literal.DefaultExpr, qt.Equals, "")

	expression := schemaprep.EffectiveFieldForPlatform(schemamodel.Field{
		Default:    "old",
		DefaultSet: true,
		Overrides: map[string]map[string]string{
			platform.Postgres: {"default_expr": "new()"},
		},
	}, platform.Postgres)
	c.Assert(expression.Default, qt.Equals, "")
	c.Assert(expression.DefaultSet, qt.IsFalse)
	c.Assert(expression.DefaultExpr, qt.Equals, "new()")
}
