package ydb

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbchangefeed"
)

// rebuildChangefeeds requires knowledge of the entire namespace: every stream
// must be dropped before a rename, including streams whose values did not change.
func (p *Planner) rebuildChangefeeds(rebuild *tableRebuild) (current, desired []ydbschema.ChangefeedSpec, err error) {
	subject := rebuildSubject(rebuild.name)
	if !rebuild.observation.HasTable() {
		return nil, nil, refuseFact(subject, "the plan carries no observed table state")
	}
	if err := refuseUnhandledRebuildFacets(rebuild); err != nil {
		return nil, nil, err
	}
	observed, declared := rebuild.observation.Table, rebuild.declaration.Table
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	parent := builder.TableParts(declared.Schema, declared.Name)
	if parent.Key() != builder.TableParts(observed.Schema, observed.Name).Key() {
		return nil, nil, refuseFact(subject, "the captured feature operands name different tables")
	}
	if err := completeChangefeedNamespace(rebuild.observation.FeatureCoverage, parent, schemaext.Observed); err != nil {
		return nil, nil, err
	}
	if err := completeChangefeedNamespace(rebuild.declaration.FeatureCoverage, parent, schemaext.Desired); err != nil {
		return nil, nil, err
	}
	if err := reconstructibleChangefeeds(rebuild.observation.OwnedObjects, rebuild.observation.FeatureCoverage, parent); err != nil {
		return nil, nil, err
	}
	if err := reconstructibleChangefeeds(rebuild.declaration.OwnedObjects, rebuild.declaration.FeatureCoverage, parent); err != nil {
		return nil, nil, err
	}
	current, err = ydbschema.ObservedChangefeeds(rebuild.observation.OwnedObjects, observed.Schema, observed.Name)
	if err != nil {
		return nil, nil, err
	}
	desired, err = ydbschema.DesiredChangefeeds(rebuild.declaration.OwnedObjects, declared.Schema, declared.Name)
	if err != nil {
		return nil, nil, err
	}
	indexes := make([]string, 0, len(rebuild.declaration.Indexes))
	for _, index := range rebuild.declaration.Indexes {
		indexes = append(indexes, index.Name)
	}
	if err := p.checkRebuiltChangefeeds(rebuild, current, desired, indexes); err != nil {
		return nil, nil, err
	}
	return current, desired, nil
}

func completeChangefeedNamespace(coverage schemaext.Coverage, parent objectidentity.ID, representation schemaext.Representation) error {
	if coverage.Representation() != representation {
		return refuseFact("rebuilding "+parent.String(), "the changefeed namespace has no matching source coverage")
	}
	knowledge := coverage.Lookup(ydbschema.ChangefeedKind, parent)
	if knowledge.State != schemaext.Complete {
		return refuseFact("rebuilding "+parent.String(), "the changefeed namespace is not fully described: "+knowledge.Reason)
	}
	for _, record := range coverage.ForParent(parent).SubjectRecords() {
		if record.Kind == ydbschema.ChangefeedKind && record.Knowledge.State != schemaext.Complete && record.Knowledge.State != schemaext.Absent {
			return refuseFact("rebuilding "+parent.String(), "a changefeed is not fully described: "+record.Knowledge.Reason)
		}
	}
	return nil
}

func (p *Planner) checkRebuiltChangefeeds(rebuild *tableRebuild, current, desired []ydbschema.ChangefeedSpec, indexes []string) error {
	subject := rebuildSubject(rebuild.name)
	if (len(current) > 0 || len(desired) > 0) && !p.caps.Has(capability.Changefeeds) {
		return refuseKey(capability.Changefeeds, subject)
	}
	if reason := ydbchangefeed.NameRefusal(desired, indexes); reason != "" {
		return refuseFact(subject, reason)
	}
	keyType := ydbchangefeed.FirstKeyType(rebuild.declaration, p.caps)
	for _, stream := range desired {
		if err := ydbchangefeed.CheckPlanned(rebuild.name, stream, keyType, p.caps); err != nil {
			return err
		}
	}
	return nil
}

func reconstructibleChangefeeds(objects schemaext.Objects, coverage schemaext.Coverage, parent objectidentity.ID) error {
	values, err := objects.All()
	if err != nil {
		return err
	}
	for _, object := range values {
		owner := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: object.Ref.Catalog, Schema: object.Ref.Schema, Name: object.Ref.Parent}
		if object.Ref.Kind != objectidentity.Kind(ydbschema.ChangefeedKind) || owner.Key() != parent.Key() {
			return refuseFact("rebuilding "+parent.String(), "no rebuild handler for the captured feature object "+object.Ref.String())
		}
		if err := ydbchangefeed.ValidateOperand(object.Ref, object.Value, objects, coverage); err != nil {
			return err
		}
	}
	return nil
}

// No facet strategy is installed in this planner yet. A rebuild must not erase
// an unchanged facet just because it produced no individual change record.
func refuseUnhandledRebuildFacets(rebuild *tableRebuild) error {
	desired, current := rebuild.declaration, rebuild.observation
	facets := []schemaext.Facets{desired.Table.Facets, current.Table.Facets}
	for _, field := range desired.Fields {
		facets = append(facets, field.Facets)
	}
	for _, column := range current.Table.Columns {
		facets = append(facets, column.Facets)
	}
	for _, index := range desired.Indexes {
		facets = append(facets, index.Facets)
	}
	for _, index := range current.Indexes {
		facets = append(facets, index.Facets)
	}
	for _, constraint := range desired.Constraints {
		facets = append(facets, constraint.Facets)
	}
	for _, constraint := range current.Constraints {
		facets = append(facets, constraint.Facets)
	}
	for _, trigger := range desired.Triggers {
		facets = append(facets, trigger.Facets)
	}
	for _, trigger := range current.Triggers {
		facets = append(facets, trigger.Facets)
	}
	for _, attached := range facets {
		if attached.Len() > 0 {
			return refuseFact(rebuildSubject(rebuild.name), fmt.Sprintf("no rebuild handler for feature facets %v", attached.Kinds()))
		}
	}
	return nil
}
