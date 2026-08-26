package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// TestConvert_ADomainDefaultKeepsItsKind pins which of the two default fields a
// catalog answer lands in, and it is the difference between a default and a
// string that looks like one.
//
// The renderer honors the distinction: an expression becomes `sql(...)` and a
// literal becomes a quoted string. Assigning the catalog's answer to the
// literal field wrote a quoted default, which reads back as text, so `apply`
// planned a SET DEFAULT of that text and the domain's default became the
// SOURCE of the old expression. Measured on PostgreSQL 17.11, a column of that
// type then defaulted to that source, and each further inspect-and-apply cycle
// wrapped it again: 26, 49, 76, 111 characters (stokaro/ptah#2037).
func TestConvert_ADomainDefaultKeepsItsKind(t *testing.T) {
	tests := []struct {
		name     string
		catalog  string
		wantExpr string
		wantLit  string
	}{
		{
			// What PostgreSQL answers for `DEFAULT 'x@y.z'` on a varchar
			// domain: the literal with a cast, which is an expression.
			name:     "the cast the catalog adds",
			catalog:  "'x@y.z'::character varying",
			wantExpr: "'x@y.z'::character varying",
		},
		{
			name:     "a function call",
			catalog:  "now()",
			wantExpr: "now()",
		},
		{
			// A bare quoted literal is not an expression, and the field that
			// takes it renders it quoted.
			name:    "a bare literal",
			catalog: "'x@y.z'",
			wantLit: "'x@y.z'",
		},
		{
			name: "no default at all",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted := dbschematogo.ConvertDBSchemaToGoSchema(&catalog.Database{
				Domains: []catalog.Domain{{
					Name: "email", Schema: "app", BaseType: "character varying(120)",
					Default: test.catalog,
				}},
			})

			c.Assert(converted.Domains, qt.DeepEquals, []schemamodel.Domain{{
				Name: "email", Schema: "app", BaseType: "character varying(120)",
				DefaultExpr: test.wantExpr, Default: test.wantLit,
			}})
		})
	}
}
