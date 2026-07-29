package annotationmeta_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/annotationmeta"
)

func TestAllowsAttributeValidatesPlatformOverrideShape(t *testing.T) {
	c := qt.New(t)

	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform.mysql.type"), qt.IsTrue)
	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform.mysql.generated.kind"), qt.IsTrue)
	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform.mysql"), qt.IsFalse)
	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform..type"), qt.IsFalse)
	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform.mysql.type-name"), qt.IsFalse)
	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform.mysql.тип"), qt.IsFalse)
}

func TestAllowsAttribute_AcceptsRetainedPlatformOverrides(t *testing.T) {
	c := qt.New(t)

	directives := []string{
		"ptah:schema:field",
		"ptah:embedded",
		"ptah:schema:table",
	}

	for _, directive := range directives {
		c.Run(directive, func(c *qt.C) {
			c.Assert(annotationmeta.AllowsAttribute(directive, "platform.postgres.type"), qt.IsTrue)
		})
	}
}

func TestAllowsAttribute_RejectsDroppedCompatibilitySyntax(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		directive string
		attribute string
	}{
		{
			name:      "field nullable",
			directive: "ptah:schema:field",
			attribute: "nullable",
		},
		{
			name:      "field autoincrement",
			directive: "ptah:schema:field",
			attribute: "autoincrement",
		},
		{
			name:      "field index",
			directive: "ptah:schema:field",
			attribute: "index",
		},
		{
			name:      "embedded not null",
			directive: "ptah:embedded",
			attribute: "not_null",
		},
		{
			name:      "embedded index",
			directive: "ptah:embedded",
			attribute: "index",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(annotationmeta.AllowsAttribute(test.directive, test.attribute), qt.IsFalse)
		})
	}
}

func TestAllowsAttribute_RejectsPlatformOverridesWithoutRuntimeSupport(t *testing.T) {
	c := qt.New(t)

	directives := []string{
		"ptah:schema:index",
		"ptah:schema:schema",
		"ptah:schema:view",
		"ptah:schema:matview",
		"ptah:schema:trigger",
	}

	for _, directive := range directives {
		c.Run(directive, func(c *qt.C) {
			c.Assert(annotationmeta.AllowsAttribute(directive, "platform.postgres.type"), qt.IsFalse)
		})
	}
}

func TestDetachedFileScopesMatchParserSupport(t *testing.T) {
	c := qt.New(t)

	for _, directive := range annotationmeta.Directives() {
		for _, scope := range directive.Scopes {
			if scope != annotationmeta.ScopeFile {
				continue
			}
			c.Assert(directive.Name, qt.Matches, `ptah:schema:rls:(policy|enable)`)
		}
	}
}
