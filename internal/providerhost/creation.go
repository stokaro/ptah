package providerhost

import (
	"context"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemaprojection"
)

func (t *target) projectOwnedTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	service := modelComparer{target: t}
	profile, err := service.request(request.Target, request.Identifiers, request.Capabilities, nil)
	if err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	profile.TargetFeatures = featureSet(t.engine.GetFeatures())
	capture := planningCapture{comparisonCapture: comparisonCapture{target: t, request: profile}, identifiers: request.Identifiers}
	remote := &protocol.ProjectTableCreationsRequest{Engine: profile.Engine, Identifiers: profile.Identifiers, Capabilities: profile.Capabilities}
	for _, table := range request.Tables {
		declared, err := capture.declaration(table.Subject, table.Declaration)
		if err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		owners, err := table.FacetOwners(request.Identifiers)
		if err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		remote.Tables = append(remote.Tables, &protocol.TableCreationInput{
			Subject: comparisonIdentity(table.Subject), Declaration: declared, FacetOwners: comparisonIdentities(owners),
		})
	}
	remote.TargetFeatures = slices.Clone(profile.TargetFeatures)
	projected, err := t.host.client.ProjectTableCreations(ctx, remote)
	if err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	result := schemaprojection.TableCreationResult{Complete: true}
	for _, output := range projected.Tables {
		created := schemaprojection.TableCreation{Subject: boundIdentity(output.Subject),
			ColumnPrimaryKeysPrepared: output.ColumnPrimaryKeysPrepared, ColumnPrimaryKeys: slices.Clone(output.ColumnPrimaryKeys)}
		created.ColumnsPrepared = slices.Contains(t.engine.GetFeatures(), provider.ColumnsFeature)
		for _, column := range output.Columns {
			predicted, err := projectedColumn(column)
			if err != nil {
				return schemaprojection.TableCreationResult{}, err
			}
			created.Columns = append(created.Columns, predicted)
		}
		created.Facets, err = service.resolvedFacetRecords(profile, schemaext.Desired, output.DesiredFacets)
		if err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		created.Observed, err = service.resolvedFacetRecords(profile, schemaext.Observed, output.ObservedFacets)
		if err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		result.Tables = append(result.Tables, created)
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	return result, nil
}
