package providerhost

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemacomparison"
)

// viewQueryService asks the provider whether a declared view's query and the
// query of the view the database holds under its name are one query
// (provider.ViewQueryEquivalenceFeature), in place of the host's comparison
// of their texts. Each held view travels as the read reported it, without
// facets, which the question does not read. Under
// provider.ViewQueryContextFeature the request carries the columns of every
// declared table, and the provider may leave a pair undecided, which the
// comparison then reads by its own folds.
type viewQueryService struct{ target *target }

// viewReferenceKinds are the reference kinds a held view travels under.
var viewReferenceKinds = map[objectidentity.Kind]string{objectidentity.KindView: "view", objectidentity.KindMatView: "materialized_view"}

func (s viewQueryService) EquivalentViewQueries(ctx context.Context, request schemacomparison.ViewQueriesRequest) ([]schemacomparison.ViewQueryAnswer, error) {
	service := modelComparer{target: s.target}
	profile, err := service.request(request.Target, request.Identifiers, request.Capabilities, nil)
	if err != nil {
		return nil, err
	}
	remote := &protocol.CompareViewQueriesRequest{Engine: profile.Engine, Identifiers: profile.Identifiers, Capabilities: profile.Capabilities,
		TargetFeatures: featureSet(s.target.engine.GetFeatures())}
	for _, pair := range request.Pairs {
		kind, known := viewReferenceKinds[pair.Kind]
		if !known {
			return nil, fmt.Errorf("provider view query comparison names %s %q, which is no view", pair.Kind, pair.Name)
		}
		remote.Pairs = append(remote.Pairs, &protocol.ViewQueryPair{
			Current:  &protocol.ObservedView{Identity: &protocol.Reference{Engine: profile.Engine, Kind: kind, Schema: pair.Schema, Name: pair.Name}, Body: pair.Held},
			Declared: pair.Declared,
		})
	}
	if s.target.negotiated(provider.ViewQueryContextFeature) && slices.Contains(remote.GetTargetFeatures(), provider.ViewQueryContextFeature) {
		for _, column := range request.Columns {
			remote.DeclaredColumns = append(remote.DeclaredColumns, &protocol.ComparisonColumn{Table: comparisonIdentity(column.Table), Name: column.Name, Type: column.Type})
		}
	}
	output, err := s.target.host.client.CompareViewQueries(ctx, remote)
	if err != nil {
		return nil, err
	}
	answers := make([]schemacomparison.ViewQueryAnswer, len(request.Pairs))
	for i, answer := range output.GetQueries() {
		switch {
		case answer.GetUndecided():
			answers[i] = schemacomparison.ViewQueryUndecided
		case answer.GetEquivalent():
			answers[i] = schemacomparison.ViewQueryEquivalent
		default:
			answers[i] = schemacomparison.ViewQueryDifferent
		}
	}
	return answers, nil
}
