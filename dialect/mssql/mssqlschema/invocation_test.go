package mssqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

func TestParseInvocation_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		text      string
		function  mssqlschema.ObjectName
		arguments []string
	}{
		{name: "a declaration", text: "dbo.fn_tenant(tenant_id)",
			function: mssqlschema.ObjectName{Schema: "dbo", Name: "fn_tenant"}, arguments: []string{"tenant_id"}},
		{name: "the catalog's spelling", text: "([dbo].[fn_tenant]([tenant_id]))",
			function: mssqlschema.ObjectName{Schema: "dbo", Name: "fn_tenant"}, arguments: []string{"[tenant_id]"}},
		{name: "a rewritten cast", text: "([rls].[fn]([tenant],CONVERT([int],[owner])+(0)))",
			function: mssqlschema.ObjectName{Schema: "rls", Name: "fn"}, arguments: []string{"[tenant]", "CONVERT([int],[owner])+(0)"}},
		{name: "quoted parts and spacing", text: ` "my schema" . [fn]]x] ( a , 'x,y' , N'(' ) `,
			function: mssqlschema.ObjectName{Schema: "my schema", Name: "fn]x"}, arguments: []string{"a", "'x,y'", "N'('"}},
		{name: "no arguments", text: "dbo.fn()", function: mssqlschema.ObjectName{Schema: "dbo", Name: "fn"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			function, arguments, err := mssqlschema.ParseInvocation(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(function, qt.DeepEquals, test.function)
			c.Assert(arguments, qt.DeepEquals, test.arguments)
		})
	}
}

func TestParseInvocation_FailurePath(t *testing.T) {
	for _, text := range []string{
		"tenant_id = 1",
		"fn_tenant(tenant_id)",
		"db.dbo.fn(tenant_id)",
		"dbo.fn(tenant_id) AND 1 = 1",
		"dbo.fn(tenant_id",
		"dbo.fn(a,,b)",
		"dbo.fn('unterminated)",
		"[].fn(a)",
		"",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			function, arguments, err := mssqlschema.ParseInvocation(text)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(function, qt.Equals, mssqlschema.ObjectName{})
			c.Assert(arguments, qt.IsNil)
		})
	}
}

func TestPredicateInvocation_ReadsBack(t *testing.T) {
	c := qt.New(t)
	predicate := mssqlschema.Predicate{Function: mssqlschema.ObjectName{Schema: "rls", Name: "fn]x"},
		Arguments: []string{"tenant_id", "CAST(owner AS int)"}}

	function, arguments, err := mssqlschema.ParseInvocation(predicate.Invocation())

	c.Assert(predicate.Invocation(), qt.Equals, "[rls].[fn]]x](tenant_id, CAST(owner AS int))")
	c.Assert(err, qt.IsNil)
	c.Assert(function, qt.DeepEquals, predicate.Function)
	c.Assert(arguments, qt.DeepEquals, predicate.Arguments)
}
