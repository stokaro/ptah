package providersource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/yamlext"
	"ptah.run/internal/providersource"
)

func TestSourceRequestsCarryIndependentNegotiatedProfiles(t *testing.T) {
	for _, row := range []struct {
		name    string
		capture func(context.Context, *protocol.Engine) (*protocol.DecodeSourceRequest, error)
	}{
		{"YAML", func(ctx context.Context, engine *protocol.Engine) (*protocol.DecodeSourceRequest, error) {
			return providersource.YAML(ctx, engine, yamlext.Document{})
		}},
		{"Go", func(ctx context.Context, engine *protocol.Engine) (*protocol.DecodeSourceRequest, error) {
			return providersource.GoAnnotations(ctx, engine, nil, nil)
		}},
		{"HCL", func(ctx context.Context, engine *protocol.Engine) (*protocol.DecodeSourceRequest, error) {
			return providersource.HCL(ctx, engine, nil, nil)
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			engine := &protocol.Engine{Id: "example", Features: []string{provider.ValueContextFeature, provider.DatabaseContextFeature}}
			request, err := row.capture(c.Context(), engine)
			c.Assert(err, qt.IsNil)
			c.Assert(request.GetTargetFeatures(), qt.DeepEquals, []string{provider.DatabaseContextFeature, provider.ValueContextFeature})
			c.Assert(engine.GetFeatures(), qt.DeepEquals, []string{provider.ValueContextFeature, provider.DatabaseContextFeature})
			engine.Features[0] = "other"
			c.Assert(request.GetTargetFeatures(), qt.DeepEquals, []string{provider.DatabaseContextFeature, provider.ValueContextFeature})
		})
	}
}
