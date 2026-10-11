package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
)

func encodeClickHouse(c *qt.C, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	c.Helper()
	return chsource.Annotations().Encoder.EncodeAnnotations(c.Context(), request)
}

func TestEncodeAnnotations_HappyPath(t *testing.T) {
	orders := annotation.Table{Schema: "shop", Name: "orders", Struct: "Order"}
	hourly := annotation.Slot{Directive: chsource.MaterializedViewDirective, Name: "hourly"}
	tenant := must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("shop", "orders", "tenant"), chschema.DesiredRowPolicy{
		Filter: new("tenant_id = 1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}},
	}))
	open := must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("shop", "orders", "open"), chschema.DesiredRowPolicy{}))
	tests := []struct {
		name    string
		request annotation.EncodeRequest
		want    annotation.EncodeResult
	}{
		{name: "a refresh schedule",
			request: annotation.EncodeRequest{Slots: []annotation.SlotFacets{{Slot: hourly, Facets: must.Must(chsource.RefreshFacets("EVERY 1 HOUR"))}}},
			want: annotation.EncodeResult{Attributes: []annotation.SlotAttributes{{Slot: hourly,
				Attributes: []annotation.EncodedAttribute{{Name: "refresh", Value: "EVERY 1 HOUR"}}}}}},
		{name: "a policy with every setting", request: annotation.EncodeRequest{Objects: []schemaext.Object{tenant}, Tables: annotation.Tables{orders}},
			want: annotation.EncodeResult{Directives: []annotation.EncodedDirective{{Directive: chsource.RowPolicyDirective,
				Placement: annotation.AfterPolicies, Table: &orders, Attributes: []annotation.EncodedAttribute{
					{Name: "name", Value: "tenant"}, {Name: "table", Value: "shop.orders"}, {Name: "using", Value: "tenant_id = 1"},
					{Name: "to", Value: "ALL EXCEPT admin"}, {Name: "as", Value: "RESTRICTIVE"}}}}}},
		{name: "a policy stating nothing", request: annotation.EncodeRequest{Objects: []schemaext.Object{open}, Tables: annotation.Tables{orders}},
			want: annotation.EncodeResult{Directives: []annotation.EncodedDirective{{Directive: chsource.RowPolicyDirective,
				Placement: annotation.AfterPolicies, Table: &orders, Attributes: []annotation.EncodedAttribute{
					{Name: "name", Value: "open"}, {Name: "table", Value: "shop.orders"}}}}}},
		{name: "nothing of the owner's", request: annotation.EncodeRequest{Tables: annotation.Tables{orders}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeClickHouse(c, test.request)

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, test.want)
		})
	}
}

// TestEncodeAnnotations_PlacesAPolicyBesideItsTableVerbatim pins that a
// policy is matched to its table as ClickHouse names it, without trimming: a
// space that ends a name is part of it.
func TestEncodeAnnotations_PlacesAPolicyBesideItsTableVerbatim(t *testing.T) {
	c := qt.New(t)
	spaced := annotation.Table{Name: "orders ", Struct: "Spaced"}
	trimmed := annotation.Table{Name: "orders", Struct: "Trimmed"}
	policy := must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("", "orders ", "p"), chschema.DesiredRowPolicy{}))

	result, err := encodeClickHouse(c, annotation.EncodeRequest{Objects: []schemaext.Object{policy}, Tables: annotation.Tables{trimmed, spaced}})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Directives, qt.HasLen, 1)
	c.Assert(result.Directives[0].Table, qt.DeepEquals, &spaced)
}

func TestEncodeAnnotations_FailurePath(t *testing.T) {
	orders := annotation.Table{Name: "orders", Struct: "Order"}
	schedule := must.Must(chsource.RefreshFacets("EVERY 1 HOUR"))
	view := objectidentity.ID{Kind: objectidentity.KindView, Name: objectidentity.Part{Source: "hourly", Normalized: "hourly"}}
	uncaptured := must.Must(chschema.RefreshCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: chschema.RefreshKind, Subject: view, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "not read"}}}))
	undescribed := must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "no grant"}, nil))
	elsewhere := must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("", "invoices", "tenant"), chschema.DesiredRowPolicy{}))
	tests := []struct {
		name    string
		request annotation.EncodeRequest
		wantIs  error
		wantErr string
	}{
		{name: "a schedule on a table", request: annotation.EncodeRequest{Slots: []annotation.SlotFacets{{
			Slot: annotation.Slot{Directive: "ptah:schema:table", Table: orders}, Facets: schedule}}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/clickhouse/refresh"`},
		{name: "a schedule that is not a declaration", request: annotation.EncodeRequest{Slots: []annotation.SlotFacets{{
			Slot: annotation.Slot{Directive: chsource.MaterializedViewDirective, Name: "hourly"}, Facets: must.Must(schemaext.NewFacets(&chschema.DesiredRefresh{}))}}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `materialized view "hourly": .*`},
		{name: "a schedule the read did not capture", request: annotation.EncodeRequest{Coverage: uncaptured},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*materialized view hourly: its refresh schedule was not captured \(not read\).*`},
		{name: "row policies the read did not describe", request: annotation.EncodeRequest{Coverage: undescribed},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*ClickHouse row policies cannot be exported: the read did not describe them \(no grant\)`},
		{name: "a policy on a table the schema does not declare", request: annotation.EncodeRequest{Objects: []schemaext.Object{elsewhere},
			Tables: annotation.Tables{orders}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*row policy .*tenant.* names a table the export does not declare`},
		{name: "a database-wide policy", request: annotation.EncodeRequest{Objects: []schemaext.Object{{
			Ref: chschema.RowPolicyRef("", "", "tenant"), Value: &chschema.DesiredRowPolicy{}}}, Tables: annotation.Tables{orders}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*ClickHouse row policy "tenant" has no table; a database-wide policy .*`},
		{name: "a policy with an empty filter", request: annotation.EncodeRequest{Objects: []schemaext.Object{{
			Ref: chschema.RowPolicyRef("", "orders", "tenant"), Value: &chschema.DesiredRowPolicy{Filter: new("")}}}, Tables: annotation.Tables{orders}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*a row policy filter cannot be empty.*`},
		{name: "a policy that is not a declaration", request: annotation.EncodeRequest{Objects: []schemaext.Object{{
			Ref: chschema.RowPolicyRef("", "orders", "tenant"), Value: &chschema.DesiredRefresh{}}}, Tables: annotation.Tables{orders}},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature object .*tenant.* with value \*chschema.DesiredRefresh`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeClickHouse(c, test.request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
		})
	}
}
