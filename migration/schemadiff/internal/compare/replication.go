package compare

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/migration/schemadiff/difftypes"
)

// Features carries every named feature object of both sides on diff, changed
// or not, for a planner whose rule about one object depends on others the
// change set does not name: the replica tables a YDB async replication owns,
// and the table and the topic a transfer depends on.
func Features(desired *schemamodel.Database, database *catalog.Database, diff *difftypes.SchemaDiff) {
	diff.Features = difftypes.FeatureContext{
		DesiredObjects:  desired.FeatureObjects,
		CurrentObjects:  database.FeatureObjects,
		CurrentCoverage: database.FeatureCoverage,
	}
}

// AdoptTransferConsumers returns desired with each changefeed and each topic it
// declares given the consumers the database's transfers read it through, where
// desired does not declare them. desired is not modified.
//
// YDB gives a transfer that names no consumer one of its own, an ordinary
// consumer of the topic with a generated name (measured on 26.2.1.14: the
// same `_service_type` attribute as one ALTER TOPIC adds), and DROP TRANSFER
// drops it again. A declaration of the changefeed or the topic cannot name
// that consumer, so without this the comparison would plan `ALTER TOPIC ...
// DROP CONSUMER` under a running transfer. A consumer a transfer reads through is kept for as
// long as the database holds the transfer, the plan that drops the transfer
// included: the drop removes a consumer YDB created and leaves one the
// transfer was given, which the next plan then drops.
func AdoptTransferConsumers(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) (*schemamodel.Database, error) {
	if desired == nil || database == nil {
		return desired, nil
	}
	transfers, err := heldTransfers(database.FeatureObjects)
	if err != nil || len(transfers) == 0 {
		return desired, err
	}
	adopted := *desired
	changefeeds, err := adoptChangefeedConsumers(&adopted, database, transfers, dialect, semantics)
	if err != nil {
		return nil, err
	}
	topics, err := adoptTopicConsumers(&adopted, database, transfers)
	if err != nil {
		return nil, err
	}
	if !changefeeds && !topics {
		return desired, nil
	}
	return &adopted, nil
}

// adoptChangefeedConsumers preserves consumers used by transfers without
// mutating either source's immutable named objects.
func adoptChangefeedConsumers(adopted *schemamodel.Database, database *catalog.Database, transfers []ydbreplication.TransferSpec, dialect string, semantics identifier.Semantics) (bool, error) {
	held := make(map[tableIdentity]catalog.Table, len(database.Tables))
	for _, table := range database.Tables {
		held[tableMapIdentity(table.Schema, table.Name, dialect, semantics)] = table
	}
	changed := false
	for _, table := range adopted.Tables {
		current, found := held[tableMapIdentity(table.Schema, table.Name, dialect, semantics)]
		if !found {
			continue
		}
		declared, err := ydbschema.DesiredChangefeeds(adopted.FeatureObjects, table.Schema, table.Name)
		if err != nil {
			return false, err
		}
		observed, err := ydbschema.ObservedChangefeeds(database.FeatureObjects, current.Schema, current.Name)
		if err != nil {
			return false, err
		}
		for _, feed := range declared {
			heldFeed, exists := changefeedNamed(observed, feed.Name)
			if !exists {
				continue
			}
			topic := ydbreplication.TablePath(table.Schema, table.Name) + "/" + feed.Name
			missing := transferConsumers(transfers, topic, feed.Consumers, heldFeed.Consumers)
			if len(missing) == 0 {
				continue
			}
			feed.Consumers = append(feed.Consumers, missing...)
			object, _, err := adopted.FeatureObjects.Get(ydbschema.ChangefeedRef(table.Schema, table.Name, feed.Name))
			if err != nil {
				return false, err
			}
			// Get returns an independent value. Preserve its retention binding:
			// adding a transfer consumer cannot take ownership from replication.
			object.Value.(*ydbschema.DesiredChangefeed).Spec = feed
			adopted.FeatureObjects, err = adopted.FeatureObjects.Replace(object)
			if err != nil {
				return false, err
			}
			changed = true
		}
	}
	return changed, nil
}

// adoptTopicConsumers gives each of adopted's topics the consumers the
// database's transfers read it through, and reports whether it gave any.
// adopted's objects are replaced, never changed in place.
func adoptTopicConsumers(adopted *schemamodel.Database, database *catalog.Database, transfers []ydbreplication.TransferSpec) (bool, error) {
	changed := false
	for _, ref := range adopted.FeatureObjects.Refs() {
		if ref.Kind != objectidentity.Kind(ydbtopic.Kind) {
			continue
		}
		object, _, err := adopted.FeatureObjects.Get(ref)
		if err != nil {
			return false, err
		}
		declared, isDesired := object.Value.(*ydbtopic.Desired)
		current, held, err := database.FeatureObjects.Get(ref)
		if err != nil {
			return false, err
		}
		observed, isObserved := current.Value.(*ydbtopic.Observed)
		if !isDesired || !held || !isObserved {
			continue
		}
		missing := transferConsumers(transfers, ydbreplication.TablePath(ref.Schema.Source, ref.Name.Source),
			declared.Spec.Consumers, observed.Spec.Consumers)
		if len(missing) == 0 {
			continue
		}
		declared.Spec.Consumers = append(declared.Spec.Consumers, missing...)
		adopted.FeatureObjects, err = adopted.FeatureObjects.Replace(object)
		if err != nil {
			return false, err
		}
		changed = true
	}
	return changed, nil
}

// heldTransfers are the specs of the transfers objects holds, a read's
// observations.
func heldTransfers(objects schemaext.Objects) ([]ydbreplication.TransferSpec, error) {
	selected, err := objects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind == objectidentity.Kind(ydbreplication.TransferKind)
	}).All()
	if err != nil {
		return nil, err
	}
	var specs []ydbreplication.TransferSpec
	for _, object := range selected {
		if held, ok := object.Value.(*ydbreplication.ObservedTransfer); ok {
			specs = append(specs, held.Spec)
		}
	}
	return specs, nil
}

// transferConsumers returns the consumers of topic, a changefeed's path, that
// a transfer of this database reads through, the database holds and declared
// does not name.
func transferConsumers(
	transfers []ydbreplication.TransferSpec,
	topic string,
	declared, held []ydbtopic.ConsumerSpec,
) []ydbtopic.ConsumerSpec {
	var missing []ydbtopic.ConsumerSpec
	for _, transfer := range transfers {
		if !ydbreplication.LocalSource(transfer) || transfer.Consumer == "" ||
			ydbreplication.SourceKey(transfer.Source, "") != topic {
			continue
		}
		name := transfer.Consumer
		if slices.ContainsFunc(declared, func(consumer ydbtopic.ConsumerSpec) bool { return consumer.Name == name }) ||
			slices.ContainsFunc(missing, func(consumer ydbtopic.ConsumerSpec) bool { return consumer.Name == name }) {
			continue
		}
		if index := slices.IndexFunc(held, func(consumer ydbtopic.ConsumerSpec) bool {
			return consumer.Name == name
		}); index >= 0 {
			missing = append(missing, held[index].Clone())
		}
	}
	return missing
}

// changefeedNamed finds a changefeed by name.
func changefeedNamed(changefeeds []ydbschema.ChangefeedSpec, name string) (ydbschema.ChangefeedSpec, bool) {
	index := slices.IndexFunc(changefeeds, func(changefeed ydbschema.ChangefeedSpec) bool { return changefeed.Name == name })
	if index < 0 {
		return ydbschema.ChangefeedSpec{}, false
	}
	return changefeeds[index], true
}
