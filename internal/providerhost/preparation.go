package providerhost

import (
	"context"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
)

// prepareOwnedTables transports captured state before common comparison. The
// owner can resolve declared facets and key membership; the host retains the
// original declarations, observations, and existence evidence.
func (t *target) prepareOwnedTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	service := modelComparer{target: t}
	profile, err := service.request(request.Target, request.Identifiers, request.Capabilities, nil)
	if err != nil {
		return schemapreparation.Result{}, err
	}
	profile.TargetFeatures = featureSet(t.engine.GetFeatures())
	capture := planningCapture{comparisonCapture: comparisonCapture{target: t, request: profile}, identifiers: request.Identifiers}
	remote := &protocol.PrepareTablesRequest{Engine: profile.Engine, Identifiers: profile.Identifiers, Capabilities: profile.Capabilities}
	for _, table := range request.Tables {
		desired, err := capture.declaration(table.Subject, table.Desired)
		if err != nil {
			return schemapreparation.Result{}, err
		}
		input := &protocol.TablePreparationInput{Subject: comparisonIdentity(table.Subject), Desired: desired, CurrentKnowledge: comparisonKnowledge(table.CurrentKnowledge)}
		if table.Current.HasTable() {
			input.Current, err = capture.observation(table.Subject, table.Current)
			if err != nil {
				return schemapreparation.Result{}, err
			}
		} else if err := onlyFields(table.Current); err != nil {
			return schemapreparation.Result{}, err
		}
		remote.Tables = append(remote.Tables, input)
	}
	remote.TargetFeatures = slices.Clone(profile.TargetFeatures)
	prepared, err := t.host.client.PrepareTables(ctx, remote)
	if err != nil {
		return schemapreparation.Result{}, err
	}
	result := schemapreparation.Result{Complete: true, Tables: request.Clone().Tables}
	for i, output := range prepared.Tables {
		table := &result.Tables[i]
		table.ColumnPrimaryKeysPrepared = output.ColumnPrimaryKeysPrepared
		if output.ColumnPrimaryKeysPrepared {
			for j := range table.Desired.Fields {
				table.Desired.Fields[j].Primary = slices.Contains(output.ColumnPrimaryKeys, table.Desired.Fields[j].Name)
			}
		}
		table.ResolvedFacets, err = service.resolvedFacetRecords(profile, schemaext.Desired, output.ResolvedFacets)
		if err != nil {
			return schemapreparation.Result{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	return result, nil
}

// These adjustments have no target bindings: the accepting core boundary keeps
// the source's bindings and exclusions when applying them.
func (s modelComparer) resolvedFacetRecords(profile *protocol.CompareModelsRequest, representation schemaext.Representation, records []*protocol.ComparisonRecord) ([]schemaext.FacetRecord, error) {
	byOwner := make(map[objectidentity.Key]schemaext.FacetRecord)
	for _, captured := range records {
		subject := boundIdentity(captured.Subject)
		record := byOwner[subject.Key()]
		record.Subject = subject
		value, err := s.resolved(profile, representation, captured.Value)
		if err != nil {
			return nil, err
		}
		record.Values, err = record.Values.With(value)
		if err != nil {
			return nil, err
		}
		byOwner[subject.Key()] = record
	}
	result := make([]schemaext.FacetRecord, 0, len(byOwner))
	for _, record := range byOwner {
		result = append(result, record)
	}
	slices.SortFunc(result, func(a, b schemaext.FacetRecord) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	return result, nil
}
