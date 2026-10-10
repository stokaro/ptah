package ydbsource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbsource"
)

func decodeTTL(ctx context.Context, properties ...map[string]string) ([]schemaext.Value, error) {
	request := schemaext.PropertyDecodeRequest{Target: "ydb", Format: schemaext.TablePlatformProperties}
	for _, fragment := range properties {
		request.Fragments = append(request.Fragments, schemaext.PropertyFragment{Kind: ydbschema.TTLKind, Properties: fragment})
	}
	return ydbsource.TTLService{}.DecodeProperties(ctx, request)
}

// TestTTLDefinitions_ClaimTheTTLAndItsPrefix pins the keys the owner claims:
// the column, the interval and the unit, and every key beginning with
// row_deletion, so a misspelled or miscased property is refused by name.
func TestTTLDefinitions_ClaimTheTTLAndItsPrefix(t *testing.T) {
	c := qt.New(t)

	definitions := ydbsource.TTLDefinitions()

	c.Assert(definitions, qt.HasLen, 1)
	c.Assert(definitions[0].Kind, qt.Equals, ydbschema.TTLKind)
	c.Assert(definitions[0].Keys, qt.DeepEquals, []string{"row_deletion_column", "row_deletion_interval", "row_deletion_unit"})
	c.Assert(definitions[0].Claims("Row_Deletion_Intervall"), qt.IsTrue)
}

// TestTTLProperties_RoundTrip pins that what Go export writes is what a Go
// annotation source decodes back, and that a unit is read in any case and
// kept in capitals.
func TestTTLProperties_RoundTrip(t *testing.T) {
	c := qt.New(t)

	values, err := decodeTTL(t.Context(), map[string]string{"row_deletion_column": "expires", "row_deletion_interval": "PT1H", "row_deletion_unit": "milliseconds"})
	c.Assert(err, qt.IsNil)
	declared := &ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "expires", Interval: "PT1H", Unit: "MILLISECONDS"}}
	c.Assert(values, qt.DeepEquals, []schemaext.Value{declared})
	fragments, err := ydbsource.TTLService{}.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{
		Target: "ydb", Format: schemaext.TablePlatformProperties, Values: []schemaext.Value{declared},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(fragments, qt.HasLen, 1)
	c.Assert(fragments[0].Properties, qt.DeepEquals, map[string]string{"row_deletion_column": "expires", "row_deletion_interval": "PT1H", "row_deletion_unit": "MILLISECONDS"})
}

func TestTTLDecodeProperties_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name       string
		ctx        context.Context
		properties map[string]string
		wantErr    error
	}{
		{name: "a unit YDB does not take", ctx: t.Context(), properties: map[string]string{"row_deletion_column": "e", "row_deletion_interval": "P1D", "row_deletion_unit": "DAYS"}, wantErr: schemaext.ErrInvalidValue},
		{name: "Spanner's spelling", ctx: t.Context(), properties: map[string]string{"row_deletion_column": "ts", "row_deletion_interval": "30 days"}, wantErr: schemaext.ErrInvalidValue},
		{name: "no column", ctx: t.Context(), properties: map[string]string{"row_deletion_interval": "P1D"}, wantErr: schemaext.ErrInvalidValue},
		{name: "a misspelled name", ctx: t.Context(), properties: map[string]string{"row_deletion_column": "ts", "row_deletion_intervall": "P1D"}, wantErr: schemaext.ErrInvalidValue},
		{name: "canceled", ctx: canceled, properties: map[string]string{"row_deletion_column": "ts", "row_deletion_interval": "P1D"}, wantErr: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := decodeTTL(test.ctx, map[string]string{"row_deletion_column": "created_at", "row_deletion_interval": "P30D"}, test.properties)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}

func TestTTLPropertyRequests_RefuseAnotherTargetOrFormat(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		format  schemaext.PropertyFormat
		wantErr error
	}{
		{name: "another target", target: "spanner", format: schemaext.TablePlatformProperties, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "another format", target: "ydb", format: "example.org/format", wantErr: ptaherr.ErrUnsupportedFeature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := ydbsource.TTLService{}.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: test.target, Format: test.format})
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(values, qt.IsNil)
			fragments, err := ydbsource.TTLService{}.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{Target: test.target, Format: test.format})
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(fragments, qt.IsNil)
		})
	}
}

// TestCoverage_EnrollsTheTTL pins that every source using Coverage can declare
// a TTL, so a table without one requests none.
func TestCoverage_EnrollsTheTTL(t *testing.T) {
	c := qt.New(t)

	coverage, err := ydbsource.Coverage(ydbsource.Limits{})

	c.Assert(err, qt.IsNil)
	events := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "events")
	c.Assert(coverage.Lookup(ydbschema.TTLKind, events).State, qt.Equals, schemaext.Complete)
}
