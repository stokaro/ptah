package annotationmeta_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/annotationmeta"
)

func TestAllowsAttributeValidatesPlatformOverrideShape(t *testing.T) {
	c := qt.New(t)

	c.Assert(annotationmeta.AllowsAttribute("migrator:schema:field", "platform.mysql.type"), qt.IsTrue)
	c.Assert(annotationmeta.AllowsAttribute("migrator:schema:field", "platform.mysql.generated.kind"), qt.IsTrue)
	c.Assert(annotationmeta.AllowsAttribute("migrator:schema:field", "platform.mysql"), qt.IsFalse)
	c.Assert(annotationmeta.AllowsAttribute("migrator:schema:field", "platform..type"), qt.IsFalse)
	c.Assert(annotationmeta.AllowsAttribute("migrator:schema:field", "platform.mysql.type-name"), qt.IsFalse)
	c.Assert(annotationmeta.AllowsAttribute("migrator:schema:field", "platform.mysql.тип"), qt.IsFalse)
}

func TestDetachedFileScopesMatchParserSupport(t *testing.T) {
	c := qt.New(t)

	for _, directive := range annotationmeta.Directives() {
		for _, scope := range directive.Scopes {
			if scope != annotationmeta.ScopeFile {
				continue
			}
			c.Assert(directive.Name, qt.Matches, `migrator:schema:rls:(policy|enable)`)
		}
	}
}
