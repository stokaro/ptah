package compare

import (
	"slices"
	"sort"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbreplication"
	"ptah.run/migration/schemadiff/difftypes"
)

// Replications compares declared YDB async replications and transfers against
// the ones the database reports, by directory and name.
//
// A replication or transfer both sides hold is compared through
// [ydbreplication]: each side resolved as YDB keeps it, so a declaration
// naming a default and one leaving it out are the same object. One only the
// database holds is a removal only where the desired state claims to describe
// the kind, and one only the declaration holds is a creation only where the
// read looked; neither CREATE carries a guard, so an undecided creation is
// withheld and recorded rather than planned.
//
// Every replication and transfer of each side is carried on the diff as well,
// changed or not: a plan reads them to keep the tables and topics they own or
// depend on.
func Replications(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	diff.Replications = difftypes.ReplicationContext{
		DesiredObjects:       desired.FeatureObjects,
		CurrentCoverage:      database.FeatureCoverage,
		CurrentReplications:  cloneNonEmpty(database.AsyncReplications),
		CurrentTransfers:     cloneNonEmpty(database.Transfers),
		DeclaredReplications: cloneNonEmpty(desired.AsyncReplications),
		DeclaredTransfers:    cloneNonEmpty(desired.Transfers),
	}
	for _, topic := range database.Topics {
		diff.Replications.CurrentTopics = append(diff.Replications.CurrentTopics,
			ydbreplication.TablePath(topic.Schema, topic.Name))
	}
	for _, topic := range desired.Topics {
		diff.Replications.DeclaredTopics = append(diff.Replications.DeclaredTopics,
			ydbreplication.TablePath(topic.Schema, topic.Name))
	}
	compareAsyncReplications(desired, database, diff, cov)
	compareTransfers(desired, database, diff, cov)
}

// compareAsyncReplications fills the replication lists of diff.
func compareAsyncReplications(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	held := make(map[string]catalog.AsyncReplication, len(database.AsyncReplications))
	for _, replication := range database.AsyncReplications {
		held[replication.QualifiedName()] = replication
	}
	var added difftypes.AsyncReplicationChanges
	declared := make(map[string]bool, len(desired.AsyncReplications))
	for _, replication := range desired.AsyncReplications {
		name := replication.QualifiedName()
		declared[name] = true
		current, exists := held[name]
		if !exists {
			added = append(added, replication)
			continue
		}
		if change, differs := difftypes.NewAsyncReplicationDiff(name, replication.Spec, current.Spec,
			current.State); differs {
			diff.AsyncReplicationsModified = append(diff.AsyncReplicationsModified, change)
		}
	}
	for name, replication := range held {
		if declared[name] || !cov.PlansRemoval(coverage.Replication, replication.Schema, replication.Name, name) {
			continue
		}
		diff.AsyncReplicationsRemoved = append(diff.AsyncReplicationsRemoved, schemamodel.AsyncReplication{
			Name: replication.Name, Schema: replication.Schema, Spec: replication.Spec.Clone(),
		})
	}
	kept, withheld := keepPlannedAdditions(cov, coverage.Replication, added,
		func(replication schemamodel.AsyncReplication) (string, []string) {
			return replication.Schema, []string{replication.Name, replication.QualifiedName()}
		},
		func(replication schemamodel.AsyncReplication) string { return replication.QualifiedName() },
		unguardedCreations(),
	)
	cov.recordUndecidedAdditions(withheld)
	diff.AsyncReplicationsAdded = kept
	byName := func(list difftypes.AsyncReplicationChanges) {
		sort.Slice(list, func(i, j int) bool { return list[i].QualifiedName() < list[j].QualifiedName() })
	}
	byName(diff.AsyncReplicationsAdded)
	byName(diff.AsyncReplicationsRemoved)
	sort.Slice(diff.AsyncReplicationsModified, func(i, j int) bool {
		return diff.AsyncReplicationsModified[i].Name < diff.AsyncReplicationsModified[j].Name
	})
}

// compareTransfers fills the transfer lists of diff.
func compareTransfers(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	held := make(map[string]catalog.Transfer, len(database.Transfers))
	for _, transfer := range database.Transfers {
		held[transfer.QualifiedName()] = transfer
	}
	var added difftypes.TransferChanges
	declared := make(map[string]bool, len(desired.Transfers))
	for _, transfer := range desired.Transfers {
		name := transfer.QualifiedName()
		declared[name] = true
		current, exists := held[name]
		if !exists {
			added = append(added, transfer)
			continue
		}
		if change, differs := difftypes.NewTransferDiff(name, transfer.Spec, current.Spec, current.State); differs {
			diff.TransfersModified = append(diff.TransfersModified, change)
		}
	}
	for name, transfer := range held {
		if declared[name] || !cov.PlansRemoval(coverage.Transfer, transfer.Schema, transfer.Name, name) {
			continue
		}
		diff.TransfersRemoved = append(diff.TransfersRemoved, schemamodel.Transfer{
			Name: transfer.Name, Schema: transfer.Schema, Spec: transfer.Spec,
		})
	}
	kept, withheld := keepPlannedAdditions(cov, coverage.Transfer, added,
		func(transfer schemamodel.Transfer) (string, []string) {
			return transfer.Schema, []string{transfer.Name, transfer.QualifiedName()}
		},
		func(transfer schemamodel.Transfer) string { return transfer.QualifiedName() },
		unguardedCreations(),
	)
	cov.recordUndecidedAdditions(withheld)
	diff.TransfersAdded = kept
	byName := func(list difftypes.TransferChanges) {
		sort.Slice(list, func(i, j int) bool { return list[i].QualifiedName() < list[j].QualifiedName() })
	}
	byName(diff.TransfersAdded)
	byName(diff.TransfersRemoved)
	sort.Slice(diff.TransfersModified, func(i, j int) bool {
		return diff.TransfersModified[i].Name < diff.TransfersModified[j].Name
	})
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
	if desired == nil || database == nil || len(database.Transfers) == 0 {
		return desired, nil
	}
	adopted := *desired
	changefeeds, err := adoptChangefeedConsumers(&adopted, database, dialect, semantics)
	if err != nil {
		return nil, err
	}
	topics := adoptTopicConsumers(&adopted, database)
	if !changefeeds && !topics {
		return desired, nil
	}
	return &adopted, nil
}

// adoptChangefeedConsumers preserves consumers used by transfers without
// mutating either source's immutable named objects.
func adoptChangefeedConsumers(adopted *schemamodel.Database, database *catalog.Database, dialect string, semantics identifier.Semantics) (bool, error) {
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
			missing := transferConsumers(database.Transfers, topic, feed.Consumers, heldFeed.Consumers)
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
// database's transfers read it through, and reports whether it gave any. It
// copies the topics before it changes one.
func adoptTopicConsumers(adopted *schemamodel.Database, database *catalog.Database) bool {
	held := make(map[string]catalog.Topic, len(database.Topics))
	for _, topic := range database.Topics {
		held[topic.QualifiedName()] = topic
	}
	declared := adopted.Topics
	changed := false
	for i, topic := range declared {
		current, holds := held[topic.QualifiedName()]
		if !holds {
			continue
		}
		missing := transferConsumers(database.Transfers, ydbreplication.TablePath(topic.Schema, topic.Name),
			topic.Spec.Consumers, current.Spec.Consumers)
		if len(missing) == 0 {
			continue
		}
		if !changed {
			adopted.Topics = slices.Clone(declared)
			changed = true
		}
		spec := topic.Spec.Clone()
		spec.Consumers = append(spec.Consumers, missing...)
		adopted.Topics[i].Spec = spec
	}
	return changed
}

// transferConsumers returns the consumers of topic, a changefeed's path, that
// a transfer of this database reads through, the database holds and declared
// does not name.
func transferConsumers(
	transfers []catalog.Transfer,
	topic string,
	declared, held []ast.TopicConsumerSpec,
) []ast.TopicConsumerSpec {
	var missing []ast.TopicConsumerSpec
	for _, transfer := range transfers {
		if !ydbreplication.LocalSource(transfer.Spec) || transfer.Spec.Consumer == "" ||
			ydbreplication.SourceKey(transfer.Spec.Source, "") != topic {
			continue
		}
		name := transfer.Spec.Consumer
		if slices.ContainsFunc(declared, func(consumer ast.TopicConsumerSpec) bool { return consumer.Name == name }) ||
			slices.ContainsFunc(missing, func(consumer ast.TopicConsumerSpec) bool { return consumer.Name == name }) {
			continue
		}
		if index := slices.IndexFunc(held, func(consumer ast.TopicConsumerSpec) bool {
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

// cloneNonEmpty copies values, and answers nil for none, so a side that
// declares no replication carries the same context whether its loader made
// an empty list or none.
func cloneNonEmpty[T any](values []T) []T {
	if len(values) == 0 {
		return nil
	}
	return slices.Clone(values)
}
