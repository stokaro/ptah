package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasfilter"
)

func TestScopeResourcesKeepsWriterOnlyKindsSelectable(t *testing.T) {
	resources := []atlasfilter.Resource{
		{Types: []string{"table"}, Schema: "public", Name: "users"},
		{Types: []string{"function", "procedure"}, Schema: "public", Name: "refresh_users"},
		{Types: []string{"function", "aggregate"}, Schema: "public", Name: "sum_balance"},
		{Types: []string{"table", "foreign_table"}, Schema: "public", Name: "archive_users"},
		{Types: []string{"collation"}, Schema: "public", Name: "app_collation"},
		{Types: []string{"default_privilege"}, Schema: "public", Name: "owner:r:PUBLIC"},
		{Types: []string{"event"}, Schema: "public", Name: "archive_users_daily"},
		{Types: []string{"composite_type", "composite"}, Schema: "public", Name: "postal_address"},
	}
	tests := []struct {
		name  string
		scope atlasfilter.Scope
		want  []bool
	}{
		{
			name: "include procedure by exact type",
			scope: atlasfilter.Scope{
				Include: []string{"refresh_*[type=procedure]"}, DefaultSchema: "public",
			},
			want: []bool{false, true, false, false, false, false, false, false},
		},
		{
			name: "generic function selector includes procedure",
			scope: atlasfilter.Scope{
				Include: []string{"refresh_*[type=function]"}, DefaultSchema: "public",
			},
			want: []bool{false, true, false, false, false, false, false, false},
		},
		{
			name: "aggregate keeps its function alias",
			scope: atlasfilter.Scope{
				Include: []string{"sum_*[type=function]"}, DefaultSchema: "public",
			},
			want: []bool{false, false, true, false, false, false, false, false},
		},
		{
			name: "foreign table keeps its table alias",
			scope: atlasfilter.Scope{
				Include: []string{"archive_users[type=table]"}, DefaultSchema: "public",
			},
			want: []bool{false, false, false, true, false, false, false, false},
		},
		{
			name: "default privilege type glob matches selector-safe identity",
			scope: atlasfilter.Scope{
				Include: []string{"*[type=default_privilege]"}, DefaultSchema: "public",
			},
			want: []bool{false, false, false, false, false, true, false, false},
		},
		{
			name: "event remains selectable by type",
			scope: atlasfilter.Scope{
				Include: []string{"archive_*[type=event]"}, DefaultSchema: "public",
			},
			want: []bool{false, false, false, false, false, false, true, false},
		},
		{
			name: "composite uses the Atlas type spelling",
			scope: atlasfilter.Scope{
				Include: []string{"postal_*[type=composite_type]"}, DefaultSchema: "public",
			},
			want: []bool{false, false, false, false, false, false, false, true},
		},
		{
			name: "exclude subtracts writer-only identity",
			scope: atlasfilter.Scope{
				Exclude: []string{"app_collation[type=collation]"}, DefaultSchema: "public",
			},
			want: []bool{true, true, true, true, false, true, true, true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := atlasfilter.ScopeResources(resources, test.scope)

			qt.Assert(t, err, qt.IsNil)
			qt.Assert(t, got, qt.DeepEquals, test.want)
		})
	}
}

func TestDatabaseIncludeValidationDoesNotClaimWriterOnlyKinds(t *testing.T) {
	err := atlasfilter.ValidateIncludeSelectors([]string{"refresh_users[type=procedure]"})

	qt.Assert(t, err, qt.ErrorMatches,
		`unsupported Atlas include resource type "procedure" in selector "refresh_users\[type=procedure\]"`)
}
