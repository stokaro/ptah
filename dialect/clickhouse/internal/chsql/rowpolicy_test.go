package chsql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chsql"
)

// Filters compare by tokens with parentheses around the whole condition
// ignored, which is what 24.10, 26.9 and a policy created by hand differ by.
// Literals, case and inner grouping still count.
func TestSameFilter(t *testing.T) {
	for _, test := range []struct {
		name string
		a, b string
		want bool
	}{
		{"spacing", "tenant=1", "tenant = 1", true},
		{"enclosing parentheses", "(tenant = 1)", "tenant = 1", true},
		{"two enclosing pairs", "((tenant = 1))", "tenant = 1", true},
		{"a pair that encloses part of it", "(a = 1) AND (b = 2)", "a = 1 AND b = 2", false},
		{"another literal", "name = 'x'", "name = 'X'", false},
		{"another column case", "Tenant = 1", "tenant = 1", false},
		{"another order", "a = 1 AND b = 2", "b = 2 AND a = 1", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(chsql.SameFilter(test.a, test.b), qt.Equals, test.want)
			c.Assert(chsql.SameFilter(test.b, test.a), qt.Equals, test.want)
		})
	}
}

// A declaration matches an observation when its composition, with the
// permissive default resolved, its users and its filter match; the filter is
// the server's spelling where one is attached.
func TestSameRowPolicy(t *testing.T) {
	observed := &chschema.ObservedRowPolicy{Filter: new("(id > 1) AND (tenant < 3)"), Composition: chschema.Permissive,
		Roles: chschema.RoleSelection{Names: []string{"a", "b"}}}
	for _, test := range []struct {
		name     string
		declared *chschema.DesiredRowPolicy
		want     bool
	}{
		{"the default composition and the server's spelling", &chschema.DesiredRowPolicy{Filter: new("id>1 AND tenant<3"),
			NormalizedFilter: new("(id > 1) AND (tenant < 3)"), Roles: chschema.RoleSelection{Names: []string{"b", "a"}}}, true},
		{"a spelling no server attached", &chschema.DesiredRowPolicy{Filter: new("id>1 AND tenant<3"),
			Roles: chschema.RoleSelection{Names: []string{"a", "b"}}}, false},
		{"restrictive", &chschema.DesiredRowPolicy{Filter: new("(id > 1) AND (tenant < 3)"), Composition: chschema.Restrictive,
			Roles: chschema.RoleSelection{Names: []string{"a", "b"}}}, false},
		{"other users", &chschema.DesiredRowPolicy{Filter: new("(id > 1) AND (tenant < 3)"), Roles: chschema.RoleSelection{All: true}}, false},
		{"no filter", &chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{Names: []string{"a", "b"}}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(chsql.SameRowPolicy(test.declared, observed), qt.Equals, test.want)
		})
	}
}
