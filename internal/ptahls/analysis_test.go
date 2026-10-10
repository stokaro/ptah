package ptahls_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/annotationmeta"
	"ptah.run/internal/annotationparse"
	"ptah.run/internal/ptahls"
)

func TestAnalyzeReportsUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	diagnostics := ptahls.Analyze(annotationmeta.Common(), `package test

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

	hover, ok := ptahls.Hover(annotationmeta.Common(),
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

	items := ptahls.Complete(annotationmeta.Common(),
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

	items := ptahls.Complete(annotationmeta.Common(),
		`//ptah:schema:field name="email" default="now()"`,
		annotationparse.Position{Line: 0, Character: 45},
	)

	c.Assert(items, qt.HasLen, 0)
}

// A YDB model is an ordinary Ptah model to the language server: YDB type
// names, a `platform.ydb.*` override and a `dialects` scope naming YDB draw no
// diagnostic, and a field's hover names the override form. The metadata holds
// no list of dialects or types, so none needs a YDB entry; this pins that a
// YDB model is not reported as unknown.
func TestAnalyzeAcceptsAYDBModel(t *testing.T) {
	c := qt.New(t)
	const source = `package models

//ptah:schema:view name="active" body="SELECT 1 AS a" dialects="ydb,postgres"
type Views struct{}

//ptah:schema:table name="orders" schema="shop/archive" primary_key="tenant,id"
type Order struct {
	//ptah:schema:field name="tenant" type="Utf8" not_null="true" platform.ydb.type="String"
	Tenant string
	//ptah:schema:field name="id" type="Uint64" not_null="true" platform.ydb.default="0"
	ID uint64
	//ptah:schema:field name="at" type="TIMESTAMP" platform.ydb.type="Timestamp64" platform.postgres.type="TIMESTAMPTZ"
	At string
}`

	diagnostics := ptahls.Analyze(annotationmeta.Common(), source)
	hover, ok := ptahls.Hover(annotationmeta.Common(), source, annotationparse.Position{Line: 7, Character: 10})

	c.Assert(diagnostics, qt.HasLen, 0)
	c.Assert(ok, qt.IsTrue)
	c.Assert(hover, qt.Contains, "`platform.<dialect>.<key>`: dialect-specific override attributes.")
}
