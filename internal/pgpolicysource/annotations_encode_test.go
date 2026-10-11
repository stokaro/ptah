package pgpolicysource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

func encodeRowSecurity(c *qt.C, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	c.Helper()
	return pgpolicysource.Annotations().Encoder.EncodeAnnotations(c.Context(), request)
}

func switchSlot(table annotation.Table, state pgpolicy.DesiredTableState, targets ...string) annotation.SlotFacets {
	facets := must.Must(schemaext.NewFacets(&state))
	if len(targets) > 0 {
		facets = must.Must(facets.WithTargetScope(pgpolicy.TableStateKind, targets...))
	}
	return annotation.SlotFacets{Slot: annotation.Slot{Directive: "ptah:schema:table", Table: table}, Facets: facets}
}

func TestEncodeAnnotations_HappyPath(t *testing.T) {
	orders := annotation.Table{Schema: "app", Name: "orders", Struct: "Order"}
	tenant := "tenant_id = 1"
	full := must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("app", "orders", "tenant"), pgpolicy.DesiredPolicy{
		Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: &tenant,
		Composition: pgpolicy.Restrictive, Comment: "by tenant",
	}))
	full.Targets = []string{"postgres"}
	public := must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("app", "orders", "open"), pgpolicy.DesiredPolicy{}))
	tests := []struct {
		name    string
		request annotation.EncodeRequest
		want    []annotation.EncodedDirective
	}{
		{name: "a policy with every setting", request: annotation.EncodeRequest{Objects: []schemaext.Object{full}, Tables: annotation.Tables{orders}},
			want: []annotation.EncodedDirective{{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterPolicies, Table: &orders,
				Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "tenant"}, {Name: "table", Value: "app.orders"},
					{Name: "for", Value: "SELECT"}, {Name: "to", Value: "reader"}, {Name: "using", Value: "tenant_id = 1"},
					{Name: "as", Value: "RESTRICTIVE"}, {Name: "comment", Value: "by tenant"}}, Targets: []string{"postgres"}}}},
		{name: "a policy stating nothing but its name",
			request: annotation.EncodeRequest{Objects: []schemaext.Object{public}, Tables: annotation.Tables{orders}},
			want: []annotation.EncodedDirective{{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterPolicies, Table: &orders,
				Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "open"}, {Name: "table", Value: "app.orders"}}}}},
		{name: "a switch that is on, forced and scoped", request: annotation.EncodeRequest{Tables: annotation.Tables{orders},
			Slots: []annotation.SlotFacets{switchSlot(orders, pgpolicy.DesiredTableState{Enabled: true, Forced: true, Comment: "on"}, "postgres")}},
			want: []annotation.EncodedDirective{{Directive: "ptah:schema:rls:enable", Placement: annotation.AfterSwitches, Table: &orders,
				Attributes: []annotation.EncodedAttribute{{Name: "table", Value: "app.orders"}, {Name: "force", Value: "true"},
					{Name: "comment", Value: "on"}}, Targets: []string{"postgres"}}}},
		{name: "a switch that is on", request: annotation.EncodeRequest{Tables: annotation.Tables{orders},
			Slots: []annotation.SlotFacets{switchSlot(orders, pgpolicy.DesiredTableState{Enabled: true})}},
			want: []annotation.EncodedDirective{{Directive: "ptah:schema:rls:enable", Placement: annotation.AfterSwitches, Table: &orders,
				Attributes: []annotation.EncodedAttribute{{Name: "table", Value: "app.orders"}}}}},
		{name: "a switch that is off", request: annotation.EncodeRequest{Tables: annotation.Tables{orders},
			Slots: []annotation.SlotFacets{switchSlot(orders, pgpolicy.DesiredTableState{})}}},
		{name: "nothing of the owner's", request: annotation.EncodeRequest{Coverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeRowSecurity(c, test.request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Directives, qt.DeepEquals, test.want)
		})
	}
}

// TestEncodeAnnotations_MatchesATableAsAPolicyNamesIt pins that a policy is
// placed beside the declared table its own identity names, under
// PostgreSQL's rules, which trim a name.
func TestEncodeAnnotations_MatchesATableAsAPolicyNamesIt(t *testing.T) {
	c := qt.New(t)
	spaced := annotation.Table{Name: "orders ", Struct: "Order"}
	policy := must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "orders", "tenant"), pgpolicy.DesiredPolicy{}))

	result, err := encodeRowSecurity(c, annotation.EncodeRequest{Objects: []schemaext.Object{policy}, Tables: annotation.Tables{spaced}})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Directives, qt.HasLen, 1)
	c.Assert(result.Directives[0].Table, qt.DeepEquals, &spaced)
}

func TestEncodeAnnotations_FailurePath(t *testing.T) {
	orders := annotation.Table{Name: "orders", Struct: "Order"}
	elsewhere := must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "invoices", "tenant"), pgpolicy.DesiredPolicy{}))
	uninspected := must.Must(schemaext.NewCoverage(schemaext.Desired, must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)).KindRecords(),
		[]schemaext.SubjectCoverage{{Kind: pgpolicy.PolicyKind, Subject: pgpolicy.PolicyRef("", "orders", "hidden"),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "no grant"}}}))
	tests := []struct {
		name    string
		request annotation.EncodeRequest
		wantIs  error
		wantErr string
	}{
		{name: "a policy the read could not describe", request: annotation.EncodeRequest{Coverage: uninspected},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*hidden.* cannot be exported without losing its coverage record.*no grant`},
		{name: "a switch forced without enabling", request: annotation.EncodeRequest{Tables: annotation.Tables{orders},
			Slots: []annotation.SlotFacets{switchSlot(orders, pgpolicy.DesiredTableState{Forced: true})}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*cannot force row-level security on table orders without enabling it`},
		{name: "a switch whose comment holds a NUL", request: annotation.EncodeRequest{Tables: annotation.Tables{orders},
			Slots: []annotation.SlotFacets{switchSlot(orders, pgpolicy.DesiredTableState{Enabled: true, Comment: "a\x00b"})}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `desired model "ptah.run/pgpolicy/table-state": .*comment.*`},
		{name: "a switch on an index", request: annotation.EncodeRequest{Tables: annotation.Tables{orders},
			Slots: []annotation.SlotFacets{{Slot: annotation.Slot{Directive: "ptah:schema:index", Table: orders, Name: "i"},
				Facets: switchSlot(orders, pgpolicy.DesiredTableState{Enabled: true}).Facets}}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/pgpolicy/table-state"`},
		{name: "a switch on a primary key", request: annotation.EncodeRequest{Tables: annotation.Tables{orders},
			Slots: []annotation.SlotFacets{{Slot: annotation.Slot{Directive: "ptah:schema:table", Table: orders, PrimaryKey: true},
				Facets: switchSlot(orders, pgpolicy.DesiredTableState{Enabled: true}).Facets}}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/pgpolicy/table-state"`},
		{name: "a policy on a table the schema does not declare", request: annotation.EncodeRequest{Objects: []schemaext.Object{elsewhere},
			Tables: annotation.Tables{orders}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*policy .*tenant.* names a table the export does not declare`},
		{name: "a policy that is not a declaration", request: annotation.EncodeRequest{Objects: []schemaext.Object{{
			Ref: pgpolicy.PolicyRef("", "orders", "tenant"), Value: &pgpolicy.ObservedPolicy{}}}, Tables: annotation.Tables{orders}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature object .*tenant.* with value \*pgpolicy.ObservedPolicy`},
		{name: "a policy checking a SELECT", request: annotation.EncodeRequest{Objects: []schemaext.Object{{
			Ref: pgpolicy.PolicyRef("", "orders", "tenant"), Value: &pgpolicy.DesiredPolicy{Command: pgpolicy.CommandSelect, WithCheck: new("true")}}},
			Tables: annotation.Tables{orders}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*a SELECT policy takes no WITH CHECK expression`},
		{name: "a reference without a table", request: annotation.EncodeRequest{Objects: []schemaext.Object{{
			Ref: pgpolicy.PolicyRef("", "", "tenant"), Value: &pgpolicy.DesiredPolicy{}}},
			Tables: annotation.Tables{orders}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*a row-security policy requires a schema, a table and a name`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeRowSecurity(c, test.request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Directives, qt.IsNil)
		})
	}
}
