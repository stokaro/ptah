package oracle

// White-box testing required: which of a domain's CHECK constraints is the
// server's own restatement of NOT NULL is decided inside the reader, and the
// exported read returns the same shape whether it was recognized or not --
// the symptom is a plan that carries the same change forever, with a different
// constraint name each time.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

// TestNotNullRestatement_MatchesTheColumnTheCatalogNames pins the recognition,
// and the column name is the whole point.
//
// Measured on 23.26.2.0.0: `CREATE DOMAIN email_d AS VARCHAR2(255) NOT NULL`
// stores its column as EMAIL_D, while `CREATE DOMAIN score_d AS NUMBER(5,2)
// CHECK (VALUE BETWEEN 0 AND 100)` stores it as VALUE -- the declaration's own
// reference to VALUE decides it. A rule written against the literal "VALUE"
// would leave the first domain reporting SYS_DOMAIN_C0043 as a declared CHECK
// (stokaro/ptah#1920).
func TestNotNullRestatement_MatchesTheColumnTheCatalogNames(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		columnName string
		want       bool
	}{
		{
			name:       "the domain-named column",
			expression: `"EMAIL_D" IS NOT NULL`,
			columnName: "EMAIL_D",
			want:       true,
		},
		{
			name:       "the VALUE column",
			expression: `"VALUE" IS NOT NULL`,
			columnName: "VALUE",
			want:       true,
		},
		{
			name:       "spacing the server chose",
			expression: "\"VALUE\"   IS\tNOT  NULL",
			columnName: "value",
			want:       true,
		},
		{
			name:       "a NOT NULL on another column entirely",
			expression: `"OTHER" IS NOT NULL`,
			columnName: "VALUE",
			want:       false,
		},
		{
			name:       "a user CHECK that merely mentions NULL",
			expression: `VALUE IS NOT NULL OR VALUE > 0`,
			columnName: "VALUE",
			want:       false,
		},
		{
			name:       "an ordinary user CHECK",
			expression: "VALUE BETWEEN 0 AND 100",
			columnName: "VALUE",
			want:       false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(notNullRestatement(test.expression, test.columnName), qt.Equals, test.want)
		})
	}
}

// TestDeclaredDomainChecks_KeepsWhatADeclarationCouldHaveWritten is the same
// rule at the level the reader uses it.
//
// The name of the dropped constraint is per-database -- SYS_DOMAIN_C0043 on
// one server and a different number on the next -- which is why reporting it
// would make the plan churn rather than merely be noisy.
func TestDeclaredDomainChecks_KeepsWhatADeclarationCouldHaveWritten(t *testing.T) {
	c := qt.New(t)

	kept := declaredDomainChecks([]catalog.DomainCheck{
		{Name: "SYS_DOMAIN_C0043", Expression: `"EMAIL_D" IS NOT NULL`},
		{Name: "SYS_DOMAIN_C0045", Expression: "VALUE <> 'zzz'"},
		{Name: "SCORE_RANGE", Expression: "VALUE BETWEEN 0 AND 100"},
	}, "EMAIL_D")

	// The unnamed user CHECK stays: GENERATED does not separate it from the
	// NOT NULL restatement -- measured, an unnamed user CHECK is also
	// `GENERATED NAME` -- so the condition is what decides.
	c.Assert(kept, qt.DeepEquals, []catalog.DomainCheck{
		{Name: "SYS_DOMAIN_C0045", Expression: "VALUE <> 'zzz'"},
		{Name: "SCORE_RANGE", Expression: "VALUE BETWEEN 0 AND 100"},
	})
}

// TestDomainQuery_AsksTheCatalogThatHasTheAnswer holds the two views apart.
//
// USER_DOMAINS carries no base type at all; USER_DOMAIN_COLS does. And the two
// spell the owner differently -- OWNER against DOMAIN_OWNER -- which is a
// difference a reader that assumed one name would find at runtime.
func TestDomainQuery_AsksTheCatalogThatHasTheAnswer(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		fragment string
	}{
		{name: "the base type", query: domainQuery, fragment: "c.data_type"},
		{name: "the nullability", query: domainQuery, fragment: "c.nullable"},
		{name: "the column count", query: domainQuery, fragment: "d.cols"},
		{name: "the cols owner", query: domainQuery, fragment: "c.owner = d.owner"},
		{name: "the constraint owner", query: domainConstraintQuery, fragment: "c.domain_owner = :1"},
		{name: "checks only", query: domainConstraintQuery, fragment: "c.constraint_type = 'C'"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.query, qt.Contains, test.fragment)
		})
	}
}
