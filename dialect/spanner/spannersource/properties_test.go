package spannersource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/spanner/spannersource"
)

func decode(ctx context.Context, properties ...map[string]string) ([]schemaext.Value, error) {
	request := schemaext.PropertyDecodeRequest{Target: "spanner", Format: schemaext.TablePlatformProperties}
	for _, fragment := range properties {
		request.Fragments = append(request.Fragments, schemaext.PropertyFragment{Kind: spannerschema.RowDeletionKind, Properties: fragment})
	}
	return spannersource.Service{}.DecodeProperties(ctx, request)
}

// TestDefinitions_ClaimThePolicyAndItsPrefix pins the keys the owner claims:
// the column and the interval, and every key beginning with row_deletion, so a
// misspelled or miscased property and a unit reach the decoder and are
// refused by name.
func TestDefinitions_ClaimThePolicyAndItsPrefix(t *testing.T) {
	c := qt.New(t)

	definitions := spannersource.Definitions()

	c.Assert(definitions, qt.HasLen, 1)
	c.Assert(definitions[0].Kind, qt.Equals, spannerschema.RowDeletionKind)
	c.Assert(definitions[0].Keys, qt.DeepEquals, []string{"row_deletion_column", "row_deletion_interval"})
	c.Assert(definitions[0].Prefixes, qt.DeepEquals, []string{"row_deletion"})
	c.Assert(definitions[0].Claims("ROW_DELETION_UNIT"), qt.IsTrue)
}

// TestProperties_RoundTrip pins that what Go export writes is what a Go
// annotation source decodes back, the server's spelling of the interval
// included.
func TestProperties_RoundTrip(t *testing.T) {
	c := qt.New(t)

	declared := &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"}}
	fragments, err := spannersource.Service{}.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{
		Target: "spanner", Format: schemaext.TablePlatformProperties, Values: []schemaext.Value{declared},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(fragments, qt.HasLen, 1)
	c.Assert(fragments[0].Properties, qt.DeepEquals, map[string]string{"row_deletion_column": "created_at", "row_deletion_interval": "4 WEEKS 2 DAYS"})
	values, err := decode(t.Context(), fragments[0].Properties)
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].Equal(declared), qt.IsTrue)
}

func TestDecodeProperties_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name       string
		ctx        context.Context
		properties map[string]string
		wantErr    error
	}{
		{name: "a unit", ctx: t.Context(), properties: map[string]string{"row_deletion_column": "ts", "row_deletion_interval": "1 days", "row_deletion_unit": "SECONDS"}, wantErr: schemaext.ErrInvalidValue},
		{name: "no interval", ctx: t.Context(), properties: map[string]string{"row_deletion_column": "ts"}, wantErr: schemaext.ErrInvalidValue},
		{name: "an interval the server refuses", ctx: t.Context(), properties: map[string]string{"row_deletion_column": "ts", "row_deletion_interval": "36 hours"}, wantErr: schemaext.ErrInvalidValue},
		{name: "YDB's spelling", ctx: t.Context(), properties: map[string]string{"row_deletion_column": "ts", "row_deletion_interval": "P30D"}, wantErr: schemaext.ErrInvalidValue},
		{name: "canceled", ctx: canceled, properties: map[string]string{"row_deletion_column": "ts", "row_deletion_interval": "1 days"}, wantErr: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := decode(test.ctx, map[string]string{"row_deletion_column": "created_at", "row_deletion_interval": "30 days"}, test.properties)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}

func TestPropertyRequests_RefuseAnotherTargetOrFormat(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		format  schemaext.PropertyFormat
		wantErr error
	}{
		{name: "another target", target: "ydb", format: schemaext.TablePlatformProperties, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "another format", target: "spanner", format: "example.org/format", wantErr: ptaherr.ErrUnsupportedFeature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := spannersource.Service{}.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: test.target, Format: test.format})
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(values, qt.IsNil)
			fragments, err := spannersource.Service{}.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{Target: test.target, Format: test.format})
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(fragments, qt.IsNil)
		})
	}
}

// TestCoverage_MakesAMissingPolicyARequestForNone pins the knowledge a source
// with platform properties holds: complete, so a table without a policy
// requests none.
func TestCoverage_MakesAMissingPolicyARequestForNone(t *testing.T) {
	c := qt.New(t)

	coverage, err := spannersource.Coverage()

	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Representation(), qt.Equals, schemaext.Desired)
	c.Assert(coverage.KindRecords(), qt.HasLen, 1)
	c.Assert(coverage.KindRecords()[0].Knowledge.State, qt.Equals, schemaext.Complete)
}
