package providerhost

import (
	"context"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemacomparison"
)

type columnService struct{ target *target }

var columnAspects = map[schemacomparison.ColumnAspect]protocol.ColumnAspect{
	schemacomparison.ColumnType:        protocol.ColumnAspect_COLUMN_TYPE,
	schemacomparison.ColumnDefault:     protocol.ColumnAspect_COLUMN_DEFAULT,
	schemacomparison.ColumnNullability: protocol.ColumnAspect_COLUMN_NULLABILITY,
	schemacomparison.ColumnSequence:    protocol.ColumnAspect_COLUMN_SEQUENCE,
}

func (s columnService) CompareColumns(ctx context.Context, request schemacomparison.ColumnsRequest) (schemacomparison.ColumnsResult, error) {
	service := modelComparer{target: s.target}
	profile, err := service.request(request.Target, request.Identifiers, request.Capabilities, nil)
	if err != nil {
		return schemacomparison.ColumnsResult{}, err
	}
	profile.TargetFeatures = featureSet(s.target.engine.GetFeatures())
	capture := planningCapture{comparisonCapture: comparisonCapture{target: s.target, request: profile}, identifiers: request.Identifiers}
	remote := &protocol.CompareColumnsRequest{Engine: profile.Engine, Identifiers: profile.Identifiers, Capabilities: profile.Capabilities}
	for _, table := range request.Tables {
		desired, err := capture.declaration(table.DesiredSubject, table.Desired)
		if err != nil {
			return schemacomparison.ColumnsResult{}, err
		}
		current, err := capture.observation(table.CurrentSubject, table.Current)
		if err != nil {
			return schemacomparison.ColumnsResult{}, err
		}
		input := &protocol.ColumnComparisonTable{Subject: columnParentIdentity(table.DesiredSubject), CurrentSubject: columnParentIdentity(table.CurrentSubject), Desired: desired, Current: current}
		for _, pair := range table.Pairs {
			value := &protocol.ColumnComparisonPair{Subject: comparisonIdentity(pair.Desired), CurrentSubject: comparisonIdentity(pair.Current),
				DesiredColumn: pair.Desired.Name.Source, CurrentColumn: pair.Current.Name.Source}
			for _, aspect := range pair.Aspects {
				value.Aspects = append(value.Aspects, columnAspects[aspect])
			}
			input.Pairs = append(input.Pairs, value)
		}
		remote.Tables = append(remote.Tables, input)
	}
	remote.TargetFeatures = slices.Clone(profile.TargetFeatures)
	output, err := s.target.host.client.CompareColumns(ctx, remote)
	if err != nil {
		return schemacomparison.ColumnsResult{}, err
	}
	return comparedColumns(output), nil
}

func comparedColumns(source *protocol.ComparedColumns) schemacomparison.ColumnsResult {
	result := schemacomparison.ColumnsResult{Complete: source.GetComplete()}
	for _, table := range source.GetTables() {
		output := schemacomparison.ColumnTableReceipt{Desired: boundIdentity(table.Subject), Current: boundIdentity(table.CurrentSubject)}
		for _, column := range table.Columns {
			receipt := schemacomparison.ColumnReceipt{Desired: boundIdentity(column.Subject), Current: boundIdentity(column.CurrentSubject)}
			for _, aspect := range column.Aspects {
				receipt.Aspects = append(receipt.Aspects, comparedColumnAspect(aspect))
			}
			output.Columns = append(output.Columns, receipt)
		}
		result.Tables = append(result.Tables, output)
	}
	return result
}

func comparedColumnAspect(source *protocol.ColumnAspectReceipt) schemacomparison.ColumnAspectReceipt {
	var result schemacomparison.ColumnAspectReceipt
	for aspect, wire := range columnAspects {
		if wire == source.GetAspect() {
			result.Aspect = aspect
		}
	}
	switch result.Aspect {
	case schemacomparison.ColumnType, schemacomparison.ColumnDefault:
		difference := source.GetDifference()
		relation := schemacomparison.ColumnSame
		if difference.GetRelation() == protocol.ColumnDifference_CHANGED {
			relation = schemacomparison.ColumnChanged
		}
		result.Difference = &schemacomparison.ColumnDifference{Relation: relation, Current: difference.GetCurrent(), Desired: difference.GetDesired()}
	case schemacomparison.ColumnNullability:
		result.Nullable = copyPlanningPointer(source.GetNullability().Nullable)
	case schemacomparison.ColumnSequence:
		result.Sequence = &schemacomparison.ColumnSequenceChanges{}
		for _, change := range source.GetSequence().GetChanges() {
			attribute := schemacomparison.SequenceStart
			if change.Attribute == protocol.ColumnSequenceDifference_IDENTITY_INCREMENT {
				attribute = schemacomparison.SequenceIncrement
			}
			result.Sequence.Changes = append(result.Sequence.Changes, schemacomparison.ColumnSequenceDifference{Attribute: attribute, Current: change.Current, Desired: change.Desired})
		}
	}
	return result
}

func (s columnService) ReadColumnDefaults(ctx context.Context, request schemacomparison.ColumnDefaultsRequest) (schemacomparison.ColumnDefaultsResult, error) {
	service := modelComparer{target: s.target}
	profile, err := service.request(request.Target, request.Identifiers, request.Capabilities, nil)
	if err != nil {
		return schemacomparison.ColumnDefaultsResult{}, err
	}
	remote := &protocol.ReadColumnDefaultsRequest{Engine: profile.Engine, Identifiers: profile.Identifiers, Capabilities: profile.Capabilities, TargetFeatures: featureSet(s.target.engine.GetFeatures())}
	for _, input := range request.Columns {
		column, err := observedPlanningColumn(input.Column)
		if err != nil {
			return schemacomparison.ColumnDefaultsResult{}, err
		}
		remote.Columns = append(remote.Columns, &protocol.ObservedColumnDefaultInput{Subject: comparisonIdentity(input.Subject), Column: column})
	}
	output, err := s.target.host.client.ReadColumnDefaults(ctx, remote)
	if err != nil {
		return schemacomparison.ColumnDefaultsResult{}, err
	}
	kinds := map[protocol.ColumnDefaultReceipt_Kind]schemacomparison.ColumnDefaultKind{
		protocol.ColumnDefaultReceipt_ABSENT: schemacomparison.DefaultAbsent, protocol.ColumnDefaultReceipt_LITERAL: schemacomparison.DefaultLiteral,
		protocol.ColumnDefaultReceipt_EXPRESSION: schemacomparison.DefaultExpression,
	}
	result := schemacomparison.ColumnDefaultsResult{Complete: output.Complete}
	for _, value := range output.Columns {
		result.Columns = append(result.Columns, schemacomparison.ColumnDefaultReceipt{Subject: boundIdentity(value.Subject), Kind: kinds[value.Kind], Value: value.Value})
	}
	return result, nil
}

// The column contract distinguishes a parent identity from a child. The host's
// zero Part is absence, not a child with an empty parent name.
func columnParentIdentity(subject objectidentity.ID) *protocol.ComparisonIdentity {
	result := comparisonIdentity(subject)
	if subject.Parent == (objectidentity.Part{}) {
		result.Parent = nil
	}
	return result
}
