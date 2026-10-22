// Package main exercises owned CREATE prediction through an independent process.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-table-creations"
	description.Features = append(description.Features, provider.ProjectTableCreationsFeature)
	engine := description.Engines[0]
	engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == provider.CommonTableIdentityFeature })
	engine.Features = append(engine.Features, provider.ProjectTableCreationsFeature)
	owner := &projector{Provider: &backend.Provider{}, description: description}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, TableCreations: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type projector struct {
	*backend.Provider
	description *protocol.Description
}

func (p *projector) ProjectTableCreations(ctx context.Context, request *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	result := &protocol.ProjectedTableCreations{Complete: true}
	for _, input := range request.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if input.Declaration.Name == "records" {
			table := input.Declaration
			if len(table.Columns) != 3 || table.Columns[0].Comment != "keep column" || len(table.Indexes) != 1 ||
				len(table.Indexes[0].Parts) != 1 || table.Indexes[0].Parts[0].Prefix != "0008" || len(table.Constraints) != 1 || table.Constraints[0].CheckExpression != "first > 0" {
				return nil, fmt.Errorf("incomplete rich CREATE capture")
			}
		}
		output := &protocol.ProjectedTableCreation{Subject: proto.CloneOf(input.Subject), ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: slices.Clone(input.Declaration.PrimaryKey)}
		for _, owner := range input.FacetOwners {
			excluded := slices.ContainsFunc(input.Declaration.Features.Facets.Records, func(record *protocol.ComparisonRecord) bool {
				return record.Kind == "example.org/fixture/retention" && proto.Equal(record.Subject, owner) && record.Value == nil
			})
			if excluded {
				continue
			}
			desired, err := p.record(ctx, request, owner, protocol.ValueInput_DESIRED, 60)
			if err != nil {
				return nil, err
			}
			observed, err := p.record(ctx, request, owner, protocol.ValueInput_OBSERVED, 90)
			if err != nil {
				return nil, err
			}
			output.DesiredFacets = append(output.DesiredFacets, desired)
			output.ObservedFacets = append(output.ObservedFacets, observed)
		}
		if input.Declaration.Name == "invalid" {
			output.ColumnPrimaryKeys = []string{"missing"}
		}
		result.Tables = append(result.Tables, output)
	}
	return result, nil
}

func (p *projector) record(ctx context.Context, request *protocol.ProjectTableCreationsRequest, subject *protocol.ComparisonIdentity,
	representation protocol.ValueInput_Representation, seconds int,
) (*protocol.ComparisonRecord, error) {
	model := p.description.Engines[0].ValueModels[0]
	source := reference(subject, (*protocol.ComparisonPart).GetSource)
	normalized, err := p.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: request.Engine, TargetFeatures: request.TargetFeatures,
		Values: []*protocol.ValueInput{{Id: subject.Kind + "/" + subject.Name.Source, Representation: representation, Value: &protocol.Envelope{
			ProviderId: p.description.ProviderId, Kind: model.Kind, SchemaVersion: model.SchemaVersion, Encoding: model.Encoding, DefinitionSha256: model.DefinitionSha256,
			Owner: source, Payload: []byte(`{"seconds":` + strconv.Itoa(seconds) + `}`),
		}}}})
	if err != nil {
		return nil, err
	}
	normalized.Values[0].NormalizedOwner = reference(subject, (*protocol.ComparisonPart).GetNormalized)
	return &protocol.ComparisonRecord{Kind: model.Kind, Subject: proto.CloneOf(subject), Value: normalized.Values[0]}, nil
}

func reference(subject *protocol.ComparisonIdentity, part func(*protocol.ComparisonPart) string) *protocol.Reference {
	owner := &protocol.Reference{Engine: "fixture", Kind: subject.Kind, Database: part(subject.Database), Catalog: part(subject.Catalog), Schema: part(subject.Schema), Name: part(subject.Name)}
	for _, namespace := range subject.Path {
		owner.Path = append(owner.Path, part(namespace))
	}
	if parent := part(subject.Parent); parent != "" {
		owner.Path = append(owner.Path, parent)
	}
	return owner
}
