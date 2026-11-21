package annotationmeta_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/internal/annotationmeta"
)

// attributeOwner is a set whose one owner adds attribute to the frontend's
// matview directive and declares a gadget directive holding it too.
func attributeOwner(c *qt.C, onMatview, onGadget []annotation.Attribute) annotation.Set {
	c.Helper()
	set, err := annotation.NewSet(annotation.Extension{
		Owner:    "example.org/paint",
		Coverage: annotation.Unlimited(func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }),
		Attributes: []annotation.DirectiveAttributes{{
			Directive: "ptah:schema:matview", Attributes: onMatview,
			Decode: func(map[string]string) (schemaext.Facets, error) { return schemaext.Facets{}, nil },
		}},
		Directives: []annotation.Directive{{
			Name: "ptah:schema:gadget", Scopes: []annotation.Scope{annotation.ScopeStruct}, Attributes: onGadget,
		}},
		Decode: func(annotation.Declaration) ([]annotation.Contribution, error) { return nil, nil },
	})
	c.Assert(err, qt.IsNil)
	return set
}

// A boolean attribute declared Boolean joins the catalog, on a frontend
// directive and on an owner's, and may then be written bare.
func TestNewCatalog_BooleanDeclaration_HappyPath(t *testing.T) {
	c := qt.New(t)
	glossy := annotation.Attribute{Name: "glossy", Value: "boolean", Boolean: true}

	catalog, err := annotationmeta.NewCatalog(attributeOwner(c, []annotation.Attribute{glossy}, []annotation.Attribute{glossy}))

	c.Assert(err, qt.IsNil)
	onMatview, _ := catalog.Attribute("ptah:schema:matview", "glossy")
	onGadget, _ := catalog.Attribute("ptah:schema:gadget", "glossy")
	c.Assert(onMatview.Boolean, qt.IsTrue)
	c.Assert(onGadget.Boolean, qt.IsTrue)
}

// An attribute whose Value and Boolean disagree is refused, wherever it is
// declared. A boolean without Boolean would refuse the bare form every other
// boolean takes; Boolean on another value would read a bare word as "true".
func TestNewCatalog_BooleanDeclaration_FailurePath(t *testing.T) {
	notBare := annotation.Attribute{Name: "glossy", Value: "boolean"}
	bareString := annotation.Attribute{Name: "glossy", Value: "string", Boolean: true}
	tests := []struct {
		name      string
		onMatview []annotation.Attribute
		onGadget  []annotation.Attribute
		wantErr   string
	}{
		{name: "a boolean not declared Boolean on a frontend directive", onMatview: []annotation.Attribute{notBare},
			wantErr: `attribute "glossy" of "ptah:schema:matview" holds a boolean and is not declared Boolean; every boolean attribute may be written bare`},
		{name: "a boolean not declared Boolean on an owner's directive", onGadget: []annotation.Attribute{notBare},
			wantErr: `attribute "glossy" of "ptah:schema:gadget" holds a boolean and is not declared Boolean; every boolean attribute may be written bare`},
		{name: "a string declared Boolean", onMatview: []annotation.Attribute{bareString},
			wantErr: `attribute "glossy" of "ptah:schema:matview" is declared Boolean and holds a "string" value; only a boolean attribute may be written bare`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			catalog, err := annotationmeta.NewCatalog(attributeOwner(c, test.onMatview, test.onGadget))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(catalog.Directives(), qt.HasLen, 0)
		})
	}
}
