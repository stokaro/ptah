package compare

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/columnsemantics"
	"ptah.run/migration/internal/generatedschema"
)

// ColumnFacts contains accepted owner receipts. The common comparison consumes
// these values without performing I/O or changing authored operands.
type ColumnFacts struct {
	Compared map[objectidentity.Key]map[schemacomparison.ColumnAspect]schemacomparison.ColumnAspectReceipt
	Defaults map[columnsemantics.Column]*schemacomparison.ColumnDefaultReceipt
}

// ResolveColumnFacts invokes the exclusive owner after parent preparation and
// before common comparison. Unknown or incomplete facts return no partial map.
func ResolveColumnFacts(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics,
	caps capability.Capabilities, prepared map[objectidentity.Key]schemapreparation.Table, service schemacomparison.ColumnService,
) (ColumnFacts, error) {
	if service == nil {
		return ColumnFacts{}, nil
	}
	if err := ctx.Err(); err != nil {
		return ColumnFacts{}, err
	}
	defaults, err := columnsemantics.Defaults(ctx, service, target, semantics, caps, current.Tables)
	if err != nil {
		return ColumnFacts{}, err
	}
	request := columnServiceRequest(desired, current, target, semantics, caps, prepared)
	result, err := service.CompareColumns(ctx, request.Clone())
	if err != nil {
		return ColumnFacts{}, err
	}
	if err := ctx.Err(); err != nil {
		return ColumnFacts{}, err
	}
	accepted, err := schemacomparison.AcceptColumns(request, result)
	if err != nil {
		return ColumnFacts{}, err
	}
	facts := ColumnFacts{Defaults: defaults, Compared: make(map[objectidentity.Key]map[schemacomparison.ColumnAspect]schemacomparison.ColumnAspectReceipt)}
	for _, table := range accepted.Tables {
		for _, column := range table.Columns {
			aspects := make(map[schemacomparison.ColumnAspect]schemacomparison.ColumnAspectReceipt)
			for _, receipt := range column.Aspects {
				aspects[receipt.Aspect] = receipt
			}
			if _, duplicate := facts.Compared[column.Desired.Key()]; duplicate {
				return ColumnFacts{}, fmt.Errorf("%w: duplicate compared column", schemacomparison.ErrColumns)
			}
			facts.Compared[column.Desired.Key()] = aspects
		}
	}
	return facts, nil
}

func columnServiceRequest(desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics,
	caps capability.Capabilities, prepared map[objectidentity.Key]schemapreparation.Table,
) schemacomparison.ColumnsRequest {
	request := schemacomparison.ColumnsRequest{Target: target, Identifiers: semantics, Capabilities: caps}
	builder := objectidentity.NewBuilder(semantics)
	observed := make(map[tableIdentity]catalog.Table)
	for _, table := range current.Tables {
		observed[tableMapIdentity(table.Schema, table.Name, target, semantics)] = table
	}
	domains := desiredDomainIdentities(desired, semantics)
	for _, table := range desired.Tables {
		identity := tableMapIdentity(table.Schema, table.Name, target, semantics)
		held, found := observed[identity]
		if !found || tableDefinesColumns(table.Facets, held.Facets) {
			continue
		}
		capture := schemacomparison.ColumnTable{DesiredSubject: builder.TableParts(table.Schema, table.Name), CurrentSubject: builder.TableParts(held.Schema, held.Name),
			Desired: schemacapture.DeclareTable(desired, table, semantics), Current: difftypes.TableObservationFor(current, held, target, semantics)}
		_, heldColumns := tableColumnMaps(desired, table, held, semantics)
		keys := prepared[identity]
		for i, field := range capture.Desired.Fields {
			column, found := heldColumns[semantics.ColumnIdentityKey(field.Name)]
			if !found || column.GeneratedKind != "" && field.GeneratedExpression == "" {
				continue
			}
			if keys.ColumnPrimaryKeysPrepared {
				for _, key := range keys.Desired.Fields {
					if semantics.ColumnIdentityKey(key.Name) == semantics.ColumnIdentityKey(field.Name) {
						capture.Desired.Fields[i].Primary = key.Primary
					}
				}
			}
			pair := schemacomparison.ColumnPair{Desired: builder.ColumnParts(table.Schema, table.Name, field.Name), Current: builder.ColumnParts(held.Schema, held.Name, column.Name),
				Aspects: []schemacomparison.ColumnAspect{schemacomparison.ColumnDefault, schemacomparison.ColumnNullability}}
			if columnTypeOwned(field, column, target, domains, caps) {
				pair.Aspects = append(pair.Aspects, schemacomparison.ColumnType)
			}
			if caps.Has(capability.SerialSequenceOptions) {
				pair.Aspects = append(pair.Aspects, schemacomparison.ColumnSequence)
			}
			capture.Pairs = append(capture.Pairs, pair)
		}
		request.Tables = append(request.Tables, capture)
	}
	return request
}

// Shared pairing keeps the service request and common comparison on the same
// folded column inventory. Neither path independently interprets a name list.
func tableColumnMaps(desired *schemamodel.Database, table schemamodel.Table, observed catalog.Table, semantics identifier.Semantics) (map[string]schemamodel.Field, map[string]catalog.Column) {
	declared := make(map[string]schemamodel.Field)
	for _, field := range generatedschema.FieldsForTable(desired, table) {
		declared[semantics.ColumnIdentityKey(field.Name)] = field
	}
	current := make(map[string]catalog.Column)
	for _, column := range observed.Columns {
		current[semantics.ColumnIdentityKey(column.Name)] = column
	}
	return declared, current
}

func tableDefinesColumns(desired, current schemaext.Facets) bool {
	_, declared := schemaext.DefiningKind(desired)
	_, observed := schemaext.DefiningKind(current)
	return declared || observed
}

// columnTypeOwned and columnTypeChange share this decision so a provider never
// receives a type aspect that the common domain owner will also interpret.
func columnTypeOwned(desired schemamodel.Field, current catalog.Column, target string, domains map[string]domainIdentity, caps capability.Capabilities) bool {
	// A formatted built-in type is not domain evidence on a target without
	// domains. Its registered owner decides type equivalence.
	if !caps.Has(capability.DomainTypes) {
		return true
	}
	_, owned := commonColumnTypeChange(desired, current, current.RawType(), target, domains)
	return !owned
}

func commonColumnTypeChange(desired schemamodel.Field, current catalog.Column, currentType, target string, domains map[string]domainIdentity) (string, bool) {
	held, currentDomain := dbColumnDomainIdentity(current)
	declared, desiredDomain := desiredColumnDomainIdentity(desired.Type, domains)
	if currentDomain || desiredDomain {
		if currentDomain && domainIdentitiesMatch(held, declared) {
			return "", true
		}
		return fmt.Sprintf("%s -> %s", currentType, strings.TrimSpace(desired.Type)), true
	}
	if sameQualifiedTypeName(desired.Type, currentType, current.UDTSchema, target) {
		return "", true
	}
	return "", false
}
