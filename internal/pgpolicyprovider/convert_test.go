package pgpolicyprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// TestConvertFeatures_HappyPath pins the projections: a declaration predicts
// its observation with PostgreSQL's defaults resolved, through the server's
// spelling where a probe attached one, and an observation becomes the
// declaration that asks for exactly what it holds.
func TestConvertFeatures_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		from, to schemaext.Representation
		value    schemaext.Value
		want     schemaext.Value
	}{
		{name: "a declaration taking every default", from: schemaext.Desired, to: schemaext.Observed,
			value: &pgpolicy.DesiredPolicy{Using: new("tenant_id = 1"), Comment: "c", StructName: "Order"},
			want: &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{public}, Using: new("tenant_id = 1"),
				Composition: pgpolicy.Permissive, Comment: "c"}},
		{name: "a declaration with the server's spelling", from: schemaext.Desired, to: schemaext.Observed,
			value: &pgpolicy.DesiredPolicy{Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}},
				Using: new("owner = 'x'"), Composition: pgpolicy.Restrictive,
				Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{app}, Using: new("((owner)::text = 'x'::text)")}},
			want: &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{app},
				Using: new("((owner)::text = 'x'::text)"), Composition: pgpolicy.Restrictive}},
		{name: "an observation", from: schemaext.Observed, to: schemaext.Desired,
			value: &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandInsert, Roles: []pgpolicy.RoleSelector{reader}, WithCheck: new("true"),
				Composition: pgpolicy.Permissive, Comment: "c"},
			want: &pgpolicy.DesiredPolicy{Command: pgpolicy.CommandInsert, Roles: []pgpolicy.RoleSelector{reader}, WithCheck: new("true"),
				Composition: pgpolicy.Permissive, Comment: "c"}},
		{name: "declared switches", from: schemaext.Desired, to: schemaext.Observed,
			value: &pgpolicy.DesiredTableState{Enabled: true, Comment: "c"}, want: &pgpolicy.ObservedTableState{Enabled: true}},
		{name: "observed switches", from: schemaext.Observed, to: schemaext.Desired,
			value: &pgpolicy.ObservedTableState{Forced: true}, want: &pgpolicy.DesiredTableState{Forced: true}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := newRuntime(c).ConvertFeatures(t.Context(),
				schemaext.ConversionRequest{Target: "postgres", From: test.from, To: test.to, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.IsNil)
			c.Assert(converted, qt.DeepEquals, []schemaext.Value{test.want})
		})
	}
}

// TestConvertFeatures_FailurePath pins the projections that have no answer: a
// role keyword nobody resolved, which the catalog records as the role it
// resolved to when the policy was created, and a target without row security.
func TestConvertFeatures_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		want    error
		message string
	}{
		{name: "an unresolved role keyword", target: "postgres", want: schemaext.ErrInvalidValue,
			message: `.*a policy TO CURRENT_USER has no observation until a server resolves the keyword`},
		{name: "a target without row security", target: "spanner", want: ptaherr.ErrUnsupportedFeature, message: `.*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value := &pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}}, Using: new("true")}

			converted, err := newRuntime(c).ConvertFeatures(t.Context(),
				schemaext.ConversionRequest{Target: test.target, From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{value}})

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(converted, qt.IsNil)
		})
	}
}
