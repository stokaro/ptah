package providerhost

import (
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
)

type planningCapture struct {
	comparisonCapture
	identifiers identifier.Semantics
}

func (c *planningCapture) table(table featureplan.Table) (*protocol.PlanningTable, error) {
	if err := onlyFields(table, "Subject", "Action", "Desired", "Current"); err != nil {
		return nil, err
	}
	action, err := planningParentAction(table.Action)
	if err != nil {
		return nil, err
	}
	result := &protocol.PlanningTable{Subject: comparisonIdentity(table.Subject), Action: action}
	if table.Desired.HasTable() {
		result.Desired, err = c.declaration(table.Subject, table.Desired)
		if err != nil {
			return nil, err
		}
	} else if err := onlyFields(table.Desired); err != nil {
		return nil, err
	}
	if table.Current.HasTable() {
		result.Current, err = c.observation(table.Subject, table.Current)
		if err != nil {
			return nil, err
		}
	} else if err := onlyFields(table.Current); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *planningCapture) features(representation schemaext.Representation, objects schemaext.Objects, coverage schemaext.Coverage, records []schemaext.FacetRecord) (*protocol.CapturedFeatures, error) {
	// A common table capture can carry knowledge from every runtime owner.
	// Route only this provider's model knowledge into its wire context. Keep
	// unknown models claiming this owner so codec validation still refuses them;
	// actual payloads remain subject to the same strict ownership checks.
	var kinds []schemaext.Kind
	for key := range c.target.host.models {
		if key.representation == representation {
			kinds = append(kinds, key.kind)
		}
	}
	for _, record := range coverage.KindRecords() {
		if record.Model.Owner == c.target.host.description.GetProviderId() {
			kinds = append(kinds, record.Model.Kind)
		}
	}
	coverage = coverage.SelectKinds(kinds)
	owned, err := c.objects(representation, schemaext.ObjectState{Objects: objects, Coverage: coverage})
	if err != nil {
		return nil, err
	}
	facets, err := c.facets(representation, schemaext.FacetState{Records: records, Coverage: coverage})
	if err != nil {
		return nil, err
	}
	return &protocol.CapturedFeatures{Objects: owned, Facets: facets}, nil
}

func (c *planningCapture) operandFacets(facets schemaext.Facets) ([]*protocol.CapturedFacet, error) {
	result := make([]*protocol.CapturedFacet, 0, len(facets.DeclaredKinds()))
	for i, kind := range facets.DeclaredKinds() {
		record := &protocol.CapturedFacet{Kind: string(kind), Targets: facets.TargetScope(kind)}
		value, found, err := facets.Get(kind)
		if err != nil {
			return nil, err
		}
		if found {
			record.Value, err = c.value(schemaext.Desired, value, strconv.Itoa(i))
			if err != nil {
				return nil, err
			}
		}
		result = append(result, record)
	}
	return result, nil
}

func mapPlanningDefinitions[T any, U any](source []T, convert func(T) (U, error)) ([]U, error) {
	result := make([]U, len(source))
	for i, value := range source {
		converted, err := convert(value)
		if err != nil {
			return nil, err
		}
		result[i] = converted
	}
	return result, nil
}

func addPlanningFacet(records *[]schemaext.FacetRecord, subject objectidentity.ID, facets schemaext.Facets) error {
	if len(facets.DeclaredKinds()) == 0 {
		return nil
	}
	if subject.Name.Empty() {
		return fmt.Errorf("%w: captured planning facet has no common owner", schemaext.ErrInvalidValue)
	}
	if slices.ContainsFunc(*records, func(record schemaext.FacetRecord) bool { return record.Subject.Key() == subject.Key() }) {
		return fmt.Errorf("%w: multiple captured facet slots name the same owner", schemaext.ErrInvalidValue)
	}
	*records = append(*records, schemaext.FacetRecord{Subject: subject, Values: facets})
	return nil
}

func planningChild(parent, child objectidentity.ID) objectidentity.ID {
	child.Database, child.Catalog, child.Namespace, child.Schema = parent.Database, parent.Catalog, parent.Namespace, parent.Schema
	if !child.Parent.Empty() {
		child.Parent = parent.Name
	}
	return child
}

func (c *planningCapture) declaration(parent objectidentity.ID, source schemacapture.TableDeclaration) (*protocol.DeclaredTable, error) {
	if err := onlyFields(source, "Table", "Fields", "Enums", "Constraints", "Indexes", "Triggers", "OwnedObjects", "FeatureCoverage"); err != nil {
		return nil, err
	}
	table := source.Table
	if err := onlyFields(
		table, "StructName", "Name", "APIName", "APINames", "Schema", "Engine", "Collate", "Comment", "PrimaryKey", "PrimaryKeyName", "PrimaryKeyParts",
		"PrimaryKeyInclude", "PrimaryKeyMethod", "PrimaryKeyFacets", "PrimaryKeyComment", "PrimaryKeyDeferrable", "PrimaryKeyInitially", "Checks", "CustomSQL",
		"DependsOn", "Facets",
	); err != nil {
		return nil, err
	}
	result := &protocol.DeclaredTable{
		Name:                 table.Name,
		Schema:               table.Schema,
		Engine:               table.Engine,
		Collation:            table.Collate,
		Comment:              table.Comment,
		PrimaryKey:           slices.Clone(table.PrimaryKey),
		PrimaryKeyName:       table.PrimaryKeyName,
		PrimaryKeyInclude:    slices.Clone(table.PrimaryKeyInclude),
		PrimaryKeyMethod:     table.PrimaryKeyMethod,
		PrimaryKeyComment:    table.PrimaryKeyComment,
		PrimaryKeyDeferrable: table.PrimaryKeyDeferrable,
		PrimaryKeyInitially:  table.PrimaryKeyInitially,
		Checks:               slices.Clone(table.Checks),
		CustomSql:            table.CustomSQL,
		DependsOn:            slices.Clone(table.DependsOn),
	}
	for _, part := range table.PrimaryKeyParts {
		result.PrimaryKeyParts = append(result.PrimaryKeyParts, &protocol.DeclaredKeyPart{Column: part.Name, Prefix: part.Prefix, Descending: part.Desc})
	}
	var err error
	result.Columns, err = mapPlanningDefinitions(source.Fields, declaredPlanningColumn)
	if err != nil {
		return nil, err
	}
	result.Enums, err = mapPlanningDefinitions(source.Enums, declaredPlanningEnum)
	if err != nil {
		return nil, err
	}
	result.Constraints, err = mapPlanningDefinitions(source.Constraints, declaredPlanningConstraint)
	if err != nil {
		return nil, err
	}
	result.Indexes, err = mapPlanningDefinitions(source.Indexes, declaredPlanningIndex)
	if err != nil {
		return nil, err
	}
	result.Triggers, err = mapPlanningDefinitions(source.Triggers, declaredPlanningTrigger)
	if err != nil {
		return nil, err
	}
	facets, err := c.declaredFacets(parent, source)
	if err != nil {
		return nil, err
	}
	result.Features, err = c.features(schemaext.Desired, source.OwnedObjects, source.FeatureCoverage, facets)
	return result, err
}

func (c *planningCapture) observation(parent objectidentity.ID, source schemacapture.TableObservation) (*protocol.ObservedTable, error) {
	if err := onlyFields(source, "Table", "Constraints", "Indexes", "Triggers", "OwnedObjects", "FeatureCoverage"); err != nil {
		return nil, err
	}
	table := source.Table
	if err := onlyFields(table, "Facets", "Name", "Schema", "Type", "Comment", "Charset", "Collate", "RowFormat", "Columns", "EstimatedRows", "RowStatsUnknown", "Partitioned", "RLSEnabled", "RLSForced"); err != nil {
		return nil, err
	}
	result := &protocol.ObservedTable{
		Name:            table.Name,
		Schema:          table.Schema,
		Type:            table.Type,
		Comment:         table.Comment,
		Charset:         table.Charset,
		Collation:       table.Collate,
		RowFormat:       table.RowFormat,
		EstimatedRows:   table.EstimatedRows,
		RowStatsUnknown: table.RowStatsUnknown,
		Partitioned:     table.Partitioned,
		RlsEnabled:      table.RLSEnabled,
		RlsForced:       table.RLSForced,
	}
	var err error
	result.Columns, err = mapPlanningDefinitions(table.Columns, observedPlanningColumn)
	if err != nil {
		return nil, err
	}
	result.Constraints, err = mapPlanningDefinitions(source.Constraints, observedPlanningConstraint)
	if err != nil {
		return nil, err
	}
	result.Indexes, err = mapPlanningDefinitions(source.Indexes, observedPlanningIndex)
	if err != nil {
		return nil, err
	}
	result.Triggers, err = mapPlanningDefinitions(source.Triggers, observedPlanningTrigger)
	if err != nil {
		return nil, err
	}
	facets, err := c.observedFacets(parent, source)
	if err != nil {
		return nil, err
	}
	result.Features, err = c.features(schemaext.Observed, source.OwnedObjects, source.FeatureCoverage, facets)
	return result, err
}
