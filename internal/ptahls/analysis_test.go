package ptahls_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/annotationparse"
	"go.5x5.cz/ptah/internal/ptahls"
)

func TestAnalyzeReportsUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	diagnostics := ptahls.Analyze(`package test

type User struct {
	//ptah:schema:field name="x" defaul="now()"
	Name string
}`)

	c.Assert(diagnostics, qt.HasLen, 1)
	c.Assert(diagnostics[0].Code, qt.Equals, "PTAH002")
	c.Assert(diagnostics[0].Message, qt.Equals, `unknown attribute "defaul" on //ptah:schema:field`)
	c.Assert(diagnostics[0].Range.Start.Line, qt.Equals, 3)
	c.Assert(diagnostics[0].Range.Start.Character, qt.Equals, 30)
}

func TestHoverShowsRLSPolicyAttributes(t *testing.T) {
	c := qt.New(t)

	hover, ok := ptahls.Hover(
		`//ptah:schema:rls:policy name="tenant" table="users"`,
		annotationparse.Position{Line: 0, Character: 20},
	)

	c.Assert(ok, qt.IsTrue)
	c.Assert(hover, qt.Contains, "`//ptah:schema:rls:policy`")
	c.Assert(hover, qt.Contains, "`using`")
	c.Assert(hover, qt.Contains, "`with_check`")
}

func TestCompleteReturnsUnusedAttributes(t *testing.T) {
	c := qt.New(t)

	items := ptahls.Complete(
		`//ptah:schema:field name="email" `,
		annotationparse.Position{Line: 0, Character: 20},
	)

	var labels []string
	for _, item := range items {
		labels = append(labels, item.Label)
	}
	c.Assert(labels, qt.Contains, "default_expr")
	c.Assert(labels, qt.Not(qt.Contains), "name")
}

func TestCompleteSuppressesAttributeNamesInsideValues(t *testing.T) {
	c := qt.New(t)

	items := ptahls.Complete(
		`//ptah:schema:field name="email" default="now()"`,
		annotationparse.Position{Line: 0, Character: 45},
	)

	c.Assert(items, qt.HasLen, 0)
}
