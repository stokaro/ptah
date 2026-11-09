package providerhost

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// checkService asks the provider whether a declared CHECK and the CHECK a
// table holds are one constraint (provider.CheckEquivalenceFeature), in place
// of the host's comparison of their texts. Each table of the request travels
// once, as held: its columns, which the expressions read, and the constraints
// the read reported.
type checkService struct{ target *target }

// checkPosition is where one pair of a host request sits in the provider
// request: its table and its place among the table's pairs.
type checkPosition struct{ table, pair int }

func (s checkService) EquivalentChecks(ctx context.Context, request schemacomparison.ChecksRequest) ([]bool, error) {
	if request.Current == nil {
		return nil, fmt.Errorf("provider CHECK comparison requires the held database")
	}
	service := modelComparer{target: s.target}
	profile, err := service.request(request.Target, request.Identifiers, request.Capabilities, nil)
	if err != nil {
		return nil, err
	}
	remote := &protocol.CompareChecksRequest{Engine: profile.Engine, Identifiers: profile.Identifiers, Capabilities: profile.Capabilities,
		TargetFeatures: featureSet(s.target.engine.GetFeatures())}
	builder := objectidentity.NewBuilder(request.Identifiers)
	tables := make(map[objectidentity.Key]int)
	positions := make([]checkPosition, len(request.Pairs))
	for i, pair := range request.Pairs {
		subject := builder.TableParts(pair.Held.Schema, pair.Held.TableName)
		index, found := tables[subject.Key()]
		if !found {
			current, err := heldCheckTable(request.Current, pair.Held, request.Identifiers)
			if err != nil {
				return nil, err
			}
			index = len(remote.Tables)
			tables[subject.Key()] = index
			remote.Tables = append(remote.Tables, &protocol.CheckComparisonTable{Subject: columnParentIdentity(subject), Current: current})
		}
		table := remote.Tables[index]
		positions[i] = checkPosition{table: index, pair: len(table.Pairs)}
		table.Pairs = append(table.Pairs, &protocol.CheckComparisonPair{Constraint: pair.Held.Name, Declared: pair.Declared})
	}
	output, err := s.target.host.client.CompareChecks(ctx, remote)
	if err != nil {
		return nil, err
	}
	answers := make([]bool, len(request.Pairs))
	for i, position := range positions {
		answers[i] = output.GetTables()[position.table].GetChecks()[position.pair].GetEquivalent()
	}
	return answers, nil
}

// heldCheckTable is the table that holds a CHECK, as the provider receives it:
// the table's name, its columns and every constraint the database reports on
// it, without captured features.
func heldCheckTable(current *catalog.Database, held catalog.Constraint, semantics identifier.Semantics) (*protocol.ObservedTable, error) {
	key := semantics.QualifiedTableIdentityKey(held.QualifiedTableName())
	for _, table := range current.Tables {
		if semantics.QualifiedTableIdentityKey(table.QualifiedName()) != key {
			continue
		}
		result := &protocol.ObservedTable{Name: table.Name, Schema: table.Schema}
		for _, column := range table.Columns {
			observed, err := observedPlanningColumn(column)
			if err != nil {
				return nil, err
			}
			result.Columns = append(result.Columns, observed)
		}
		for _, constraint := range current.Constraints {
			if semantics.QualifiedTableIdentityKey(constraint.QualifiedTableName()) != key {
				continue
			}
			observed, err := observedPlanningConstraint(constraint)
			if err != nil {
				return nil, err
			}
			result.Constraints = append(result.Constraints, observed)
		}
		return result, nil
	}
	return nil, fmt.Errorf("provider CHECK comparison names table %s, which the held database does not hold", held.QualifiedTableName())
}
