package providersource

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// sourceRequest carries the negotiated profile with every frontend capture.
// Context readiness and source limits are validated against this exact profile.
func sourceRequest(engine *protocol.Engine, document *protocol.SourceDocument) *protocol.DecodeSourceRequest {
	features := slices.Clone(engine.GetFeatures())
	slices.Sort(features)
	return &protocol.DecodeSourceRequest{Engine: engine.GetId(), TargetFeatures: slices.Compact(features), Documents: []*protocol.SourceDocument{document}}
}
