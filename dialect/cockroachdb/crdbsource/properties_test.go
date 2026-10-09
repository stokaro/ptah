package crdbsource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/cockroachdb/crdbsource"
)

func decode(ctx context.Context, properties ...map[string]string) ([]schemaext.Value, error) {
	request := schemaext.PropertyDecodeRequest{Target: "cockroachdb", Format: schemaext.TablePlatformProperties}
	for _, fragment := range properties {
		request.Fragments = append(request.Fragments, schemaext.PropertyFragment{Kind: crdbschema.RowTTLKind, Properties: fragment})
	}
	return crdbsource.Service{}.DecodeProperties(ctx, request)
}

// TestDefinitions_ClaimEveryParameterAndTheMarker pins the keys the owner
// claims: every managed parameter, so a declared one reaches the decoder, and
// the derived marker, so declaring it is refused with the server's reason
// rather than passed through as an unrelated table option.
func TestDefinitions_ClaimEveryParameterAndTheMarker(t *testing.T) {
	c := qt.New(t)

	definitions := crdbsource.Definitions()

	c.Assert(definitions, qt.HasLen, 1)
	c.Assert(definitions[0].Kind, qt.Equals, crdbschema.RowTTLKind)
	c.Assert(definitions[0].Keys, qt.DeepEquals, append([]string{"ttl"}, crdbschema.ManagedParameters()...))
}

// TestProperties_RoundTripEveryParameter pins that what Go export writes is
// what a Go annotation source decodes back, every parameter included.
func TestProperties_RoundTripEveryParameter(t *testing.T) {
	c := qt.New(t)

	declared := &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{
		ExpirationExpression: "expires_at + INTERVAL '1 day'", ExpireAfter: "3 days", RowStatsPollInterval: "10m",
		JobCron: "@daily", SelectBatchSize: new(int64(500)), DeleteBatchSize: new(int64(100)),
		SelectRateLimit: new(int64(200)), DeleteRateLimit: new(int64(300)),
		Pause: true, LabelMetrics: true, DisableChangefeedReplication: true,
	}}
	fragments, err := crdbsource.Service{}.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{
		Target: "cockroachdb", Format: schemaext.TablePlatformProperties, Values: []schemaext.Value{declared},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(fragments, qt.HasLen, 1)
	c.Assert(fragments[0].Properties["ttl_select_batch_size"], qt.Equals, "500")
	c.Assert(fragments[0].Properties["ttl_pause"], qt.Equals, "true")
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
		{name: "the derived marker", ctx: t.Context(), properties: map[string]string{"ttl": "on"}, wantErr: schemaext.ErrInvalidValue},
		{name: "a knob without an expiry", ctx: t.Context(), properties: map[string]string{"ttl_job_cron": "@daily"}, wantErr: schemaext.ErrInvalidValue},
		{name: "a zero count", ctx: t.Context(), properties: map[string]string{"ttl_expire_after": "1 day", "ttl_delete_batch_size": "0"}, wantErr: schemaext.ErrInvalidValue},
		{name: "canceled", ctx: canceled, properties: map[string]string{"ttl_expire_after": "1 day"}, wantErr: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := decode(test.ctx, map[string]string{"ttl_expiration_expression": "expires_at"}, test.properties)
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
		{name: "another target", target: "postgres", format: schemaext.TablePlatformProperties, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "another format", target: "cockroachdb", format: "example.org/format", wantErr: ptaherr.ErrUnsupportedFeature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := crdbsource.Service{}.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: test.target, Format: test.format})
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(values, qt.IsNil)
			fragments, err := crdbsource.Service{}.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{Target: test.target, Format: test.format})
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

	coverage, err := crdbsource.Coverage()

	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Representation(), qt.Equals, schemaext.Desired)
	c.Assert(coverage.KindRecords(), qt.HasLen, 1)
	c.Assert(coverage.KindRecords()[0].Knowledge.State, qt.Equals, schemaext.Complete)
}
