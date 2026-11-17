package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgrole"
)

// withSettings is a role's facets declaring statement_timeout.
func withSettings(value string) schemaext.Facets {
	return must.Must(pgrole.WithSettings(schemaext.Facets{}, []pgrole.Setting{{Name: "statement_timeout", Value: value}}))
}

func TestValidateRoles_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name  string
		roles []schemamodel.Role
	}{
		{name: "no roles"},
		{name: "two roles", roles: []schemamodel.Role{{Name: "app", Login: true}, {Name: "reader"}}},
		{name: "one role declared twice alike", roles: []schemamodel.Role{
			{Name: "app", Login: true, Facets: withSettings("'30s'")}, {Name: "app", Login: true, Facets: withSettings("'30s'")},
		}},
		// The struct a declaration is attached to is not an attribute of the
		// role, so two structs may declare it.
		{name: "two structs", roles: []schemamodel.Role{{StructName: "A", Name: "app"}, {StructName: "B", Name: "app"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemamodel.ValidateRoles(&schemamodel.Database{Roles: test.roles}), qt.IsNil)
		})
	}
}

func TestValidateRoles_NilDatabase(t *testing.T) {
	c := qt.New(t)
	c.Assert(schemamodel.ValidateRoles(nil), qt.IsNil)
}

func TestValidateRoles_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		roles []schemamodel.Role
		want  string
	}{
		{name: "an attribute", roles: []schemamodel.Role{{Name: "app", Login: true}, {Name: "app"}},
			want: `conflicting role "app" definitions`},
		{name: "a setting", roles: []schemamodel.Role{{Name: "app", Facets: withSettings("'30s'")}, {Name: "app", Facets: withSettings("'45s'")}},
			want: `conflicting role "app" definitions`},
		{name: "settings on one declaration only", roles: []schemamodel.Role{{Name: "app", Facets: withSettings("'30s'")}, {Name: "app"}},
			want: `conflicting role "app" definitions`},
		{name: "the structs that declare it", roles: []schemamodel.Role{{StructName: "A", Name: "app"}, {StructName: "B", Name: "app", Superuser: true}},
			want: `conflicting role "app" definitions on structs A and B`},
		{name: "a third declaration", roles: []schemamodel.Role{{Name: "app"}, {Name: "reader"}, {Name: "app"}, {Name: "app", CreateDB: true}},
			want: `conflicting role "app" definitions`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemamodel.ValidateRoles(&schemamodel.Database{Roles: test.roles}), qt.ErrorMatches, test.want)
		})
	}
}

// Merge combines sources that may each declare a role. Declarations that agree
// are one role; a setting one source gives and another does not is a
// disagreement, refused rather than kept from whichever source came first.
func TestMerge_DuplicateRoles_HappyPath(t *testing.T) {
	c := qt.New(t)
	first := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", Facets: withSettings("'30s'")}}}
	second := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", Facets: withSettings("'30s'")}}}

	merged, err := schemamodel.Merge(first, second)

	c.Assert(err, qt.IsNil)
	c.Assert(merged.Roles, qt.HasLen, 1)
	settings, _, err := pgrole.SettingsOf(merged.Roles[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(settings, qt.DeepEquals, []pgrole.Setting{{Name: "statement_timeout", Value: "'30s'"}})
}

func TestMerge_DuplicateRoles_FailurePath(t *testing.T) {
	c := qt.New(t)
	first := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", Facets: withSettings("'30s'")}}}
	second := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", Facets: withSettings("'45s'")}}}

	merged, err := schemamodel.Merge(first, second)

	c.Assert(err, qt.ErrorMatches, `conflicting role "app" definitions`)
	c.Assert(merged, qt.IsNil)
}
