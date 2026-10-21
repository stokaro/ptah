package backend

import (
	"context"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// ConvertValues reinterprets retention with the source representation's defaults.
func (p *Provider) ConvertValues(ctx context.Context, request *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	return ConvertValues(ctx, request, p)
}

// ConvertValues shares retention semantics while each fixture keeps its own
// identifier normalization. Supplied projections never replace payload parsing.
func ConvertValues(ctx context.Context, request *protocol.ConvertValuesRequest, owner provider.ValueNormalizer) (*protocol.NormalizedValues, error) {
	destination := &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures()}
	for _, source := range request.GetValues() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		seconds, err := retentionSeconds(source.GetValue().GetPayload(), request.GetFrom())
		if err != nil {
			return nil, err
		}
		value := proto.CloneOf(source.GetValue())
		value.Payload = []byte(fmt.Sprintf(`{"seconds":%d}`, seconds))
		destination.Values = append(destination.Values, &protocol.ValueInput{Id: source.GetId(), Representation: request.GetTo(), Value: value})
	}
	return owner.NormalizeValues(ctx, destination)
}
