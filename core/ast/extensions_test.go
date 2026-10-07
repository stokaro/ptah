package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

type brokenClone struct {
	kind  schemaext.Kind
	clone ast.ExtensionPayload
}

func (p *brokenClone) Kind() schemaext.Kind                 { return p.kind }
func (p *brokenClone) CloneExtension() ast.ExtensionPayload { return p.clone }

func TestCloneExtensionPayload_RefusesInvalidClone(t *testing.T) {
	const kind schemaext.Kind = "example.org/feature"
	tests := []struct {
		name  string
		clone ast.ExtensionPayload
	}{
		{name: "nil"},
		{name: "typed nil", clone: (*brokenClone)(nil)},
		{name: "changed kind", clone: &brokenClone{kind: "example.org/other"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cloned, err := ast.CloneExtensionPayload(&brokenClone{kind: kind, clone: test.clone})
			c.Assert(err, qt.ErrorMatches, `extension "example.org/feature" returned an invalid clone`)
			c.Assert(cloned, qt.IsNil)
		})
	}
}
