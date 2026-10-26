package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/deporder"
)

// TestNamesRowType pins which type spellings are a relation's row type. A
// spelling missed is a composite type, a domain or a routine the drop of the
// relation fails on with 2BP01; one matched wrongly costs only a CASCADE or a
// routine kept in its own step.
func TestNamesRowType(t *testing.T) {
	sessionPath := identifier.Semantics{DefaultSchema: "public", SearchPath: []string{"public"}}
	tests := []struct {
		name      string
		spelled   string
		relation  string
		semantics identifier.Semantics
		want      bool
	}{
		{name: "the bare name", spelled: "archive", relation: "archive", want: true},
		{name: "an array of the row type", spelled: "archive[]", relation: "archive", want: true},
		{name: "a two-dimensional array", spelled: "archive[] []", relation: "archive", want: true},
		{name: "a quoted name in another case", spelled: `"Archive"`, relation: "archive", want: true},
		{name: "the relation's schema written out", spelled: "public.archive", relation: "public.archive", want: true},
		{name: "another relation", spelled: "archives", relation: "archive"},
		{name: "a built-in type", spelled: "character varying(10)", relation: "varying"},
		{name: "a qualified type in another schema", spelled: "audit.archive", relation: "public.archive"},
		{name: "an unknown default schema matches any schema",
			spelled: "audit.archive", relation: "archive", want: true},
		{name: "a qualified type and a relation in the default schema",
			spelled: "public.archive", relation: "archive", semantics: sessionPath, want: true},
		{name: "a qualified type and a relation outside the default schema",
			spelled: "audit.archive", relation: "archive", semantics: sessionPath},
		{name: "an unqualified type reaches a relation on the search path",
			spelled: "archive", relation: "reporting.archive",
			semantics: identifier.Semantics{DefaultSchema: "public", SearchPath: []string{"public", "reporting"}}, want: true},
		{name: "an unqualified type does not reach a relation off the search path",
			spelled: "archive", relation: "audit.archive", semantics: sessionPath},
		{name: "an unknown search path reaches any schema",
			spelled: "archive", relation: "audit.archive", semantics: identifier.Semantics{DefaultSchema: "public"}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			got := deporder.NamesRowType(test.spelled, test.relation, test.semantics)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
