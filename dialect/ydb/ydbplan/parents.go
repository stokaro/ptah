package ydbplan

import (
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbchangefeed"
)

func planParents(request featureplan.Request) ([]featureplan.ParentPlan, error) {
	var result []featureplan.ParentPlan
	for _, table := range request.Tables {
		if table.Action == "" {
			continue
		}
		if len(request.ParentKinds) == 0 {
			return nil, refuseFact(table.Subject.String(), "parent planning requires assigned model kinds")
		}
		for _, kind := range request.ParentKinds {
			if kind != ydbschema.ChangefeedKind {
				return nil, refuseFact(table.Subject.String(), fmt.Sprintf("no parent handler for model %q", kind))
			}
			if err := validateParent(table, request.Capabilities); err != nil {
				return nil, err
			}
			strategy := "restore declared changefeeds through the parent rebuild; stream state is lost"
			if table.Action == featureplan.DropTable {
				strategy = "remove observed changefeeds with the table; stream state is lost"
			}
			result = append(result, featureplan.ParentPlan{Subject: table.Subject, Kind: kind, Action: table.Action, Strategy: strategy})
		}
	}
	return result, nil
}

func validateParent(table featureplan.Table, caps capability.Capabilities) error {
	subject := string(table.Action) + " " + table.Subject.String()
	if !table.Current.HasTable() {
		return refuseFact(subject, "the plan carries no observed table state")
	}
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	if builder.TableParts(table.Current.Table.Schema, table.Current.Table.Name).Key() != table.Subject.Key() {
		return refuseFact(subject, "the captured feature operands name different tables")
	}
	if err := refuseChangefeedFacets(table); err != nil {
		return err
	}
	if err := completeChangefeedNamespace(table.Action, table.Current.FeatureCoverage, table.Subject, schemaext.Observed); err != nil {
		return err
	}
	if err := reconstructibleChangefeeds(table.Action, table.Current.OwnedObjects, table.Current.FeatureCoverage, table.Subject); err != nil {
		return err
	}
	switch table.Action {
	case featureplan.DropTable:
		if table.Desired.HasTable() {
			return refuseFact(subject, "a removed table carries a declaration")
		}
		return nil
	case featureplan.RebuildTable:
		return validateRebuiltParent(table, caps)
	default:
		return refuseFact(subject, "unknown parent operation")
	}
}

func validateRebuiltParent(table featureplan.Table, caps capability.Capabilities) error {
	subject := "rebuilding " + table.Subject.String()
	declared := table.Desired.Table
	if !table.Desired.HasTable() {
		return refuseFact(subject, "the plan carries no declared table state")
	}
	if objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts(declared.Schema, declared.Name).Key() != table.Subject.Key() {
		return refuseFact(subject, "the captured feature operands name different tables")
	}
	if err := completeChangefeedNamespace(table.Action, table.Desired.FeatureCoverage, table.Subject, schemaext.Desired); err != nil {
		return err
	}
	if err := reconstructibleChangefeeds(table.Action, table.Desired.OwnedObjects, table.Desired.FeatureCoverage, table.Subject); err != nil {
		return err
	}
	desired, err := ydbschema.DesiredChangefeeds(table.Desired.OwnedObjects, declared.Schema, declared.Name)
	if err != nil {
		return err
	}
	current, err := ydbschema.ObservedChangefeeds(table.Current.OwnedObjects, table.Current.Table.Schema, table.Current.Table.Name)
	if err != nil {
		return err
	}
	if (len(current) > 0 || len(desired) > 0) && !caps.Has(capability.Changefeeds) {
		return refuseKey(capability.Changefeeds, subject)
	}
	indexes := make([]string, 0, len(table.Desired.Indexes))
	for _, index := range table.Desired.Indexes {
		indexes = append(indexes, index.Name)
	}
	if reason := ydbchangefeed.NameRefusal(desired, indexes); reason != "" {
		return refuseFact(subject, reason)
	}
	keyType := ydbchangefeed.FirstKeyType(table.Desired, caps)
	for _, stream := range desired {
		if err := ydbchangefeed.CheckPlanned(declared.QualifiedName(), stream, keyType, caps); err != nil {
			return err
		}
	}
	return nil
}

// A changefeed is a named object. Using its codec in a facet does not create
// another supported placement, even when the target knows the model kind.
func refuseChangefeedFacets(table featureplan.Table) error {
	d, o := table.Desired, table.Current
	declared := schemamodel.Database{Tables: []schemamodel.Table{d.Table}, Fields: d.Fields, Enums: d.Enums, Constraints: d.Constraints, Indexes: d.Indexes, Triggers: d.Triggers}
	observed := catalog.Database{Tables: []catalog.Table{o.Table}, Indexes: o.Indexes, Constraints: o.Constraints, Triggers: o.Triggers}
	for _, slots := range [][]*schemaext.Facets{declared.FacetSlots(), observed.FacetSlots()} {
		for _, facets := range slots {
			if slices.Contains(facets.Kinds(), ydbschema.ChangefeedKind) {
				return refuseFact(table.Subject.String(), "changefeeds require named objects, not feature facets")
			}
		}
	}
	return nil
}

func completeChangefeedNamespace(action featureplan.ParentAction, coverage schemaext.Coverage, parent objectidentity.ID, representation schemaext.Representation) error {
	if coverage.Representation() != representation {
		return refuseFact(string(action)+" "+parent.String(), "the changefeed namespace has no matching source coverage")
	}
	knowledge := coverage.Lookup(ydbschema.ChangefeedKind, parent)
	if knowledge.State != schemaext.Complete {
		return refuseFact(string(action)+" "+parent.String(), "the changefeed namespace is not fully described: "+knowledge.Reason)
	}
	for _, record := range coverage.ForParent(parent).SubjectRecords() {
		if record.Kind == ydbschema.ChangefeedKind && record.Knowledge.State != schemaext.Complete && record.Knowledge.State != schemaext.Absent {
			return refuseFact(string(action)+" "+parent.String(), "a changefeed is not fully described: "+record.Knowledge.Reason)
		}
	}
	return nil
}

func reconstructibleChangefeeds(action featureplan.ParentAction, objects schemaext.Objects, coverage schemaext.Coverage, parent objectidentity.ID) error {
	values, err := objects.All()
	if err != nil {
		return err
	}
	for _, object := range values {
		owner := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: object.Ref.Catalog, Schema: object.Ref.Schema, Name: object.Ref.Parent}
		if object.Value.Kind() != ydbschema.ChangefeedKind {
			continue
		}
		switch value := object.Value.(type) {
		case *ydbschema.ObservedChangefeed:
			if value.Replication != nil {
				return refuseFact(string(action)+" "+parent.String(), "a replication-managed changefeed depends on this table; its controller must release it before the parent changes")
			}
		case *ydbschema.DesiredChangefeed:
			if value.RetainedReplication != nil {
				return refuseFact(string(action)+" "+parent.String(), "a retained replication-managed changefeed cannot be recreated with its parent")
			}
		}
		if object.Ref.Kind != objectidentity.Kind(ydbschema.ChangefeedKind) || owner.Key() != parent.Key() {
			return refuseFact(string(action)+" "+parent.String(), "no rebuild handler for the captured feature object "+object.Ref.String())
		}
		if err := ydbchangefeed.ValidateOperand(object.Ref, object.Value, objects, coverage); err != nil {
			return err
		}
	}
	return nil
}
