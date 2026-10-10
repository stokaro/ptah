package ydb

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbpath"
	"ptah.run/migration/schemadiff/difftypes"
)

// The replication owner plans, renders and refuses each statement on an async
// replication or a transfer on its own (see dialect/ydb/ydbplan). What it
// cannot see is the rest of the plan: the tables a replication owns and the
// tables, topics and changefeeds a transfer depends on. The rules here read
// the owned objects of [difftypes.FeatureContext] and the plan's table
// changes, and refuse a plan the two together would break.

// replication is an async replication of one side, by its canonical
// reference, with the state the database reported, empty for a declaration.
type replication struct {
	name  string
	spec  ydbreplication.ReplicationSpec
	state string
}

// transfer is a transfer of one side, by its canonical reference.
type transfer struct {
	name string
	spec ydbreplication.TransferSpec
}

// replicationsOf lists the replications objects holds, in either
// representation, in reference order.
func replicationsOf(objects schemaext.Objects) []replication {
	var found []replication
	for _, object := range ownedObjects(objects, ydbreplication.ReplicationKind) {
		name := ydbreplication.Reference(object.Ref.Schema.Source, object.Ref.Name.Source)
		switch value := object.Value.(type) {
		case *ydbreplication.DesiredReplication:
			found = append(found, replication{name: name, spec: value.Spec})
		case *ydbreplication.ObservedReplication:
			found = append(found, replication{name: name, spec: value.Spec, state: value.State})
		}
	}
	return found
}

// transfersOf lists the transfers objects holds, in either representation, in
// reference order.
func transfersOf(objects schemaext.Objects) []transfer {
	var found []transfer
	for _, object := range ownedObjects(objects, ydbreplication.TransferKind) {
		name := ydbreplication.Reference(object.Ref.Schema.Source, object.Ref.Name.Source)
		switch value := object.Value.(type) {
		case *ydbreplication.DesiredTransfer:
			found = append(found, transfer{name: name, spec: value.Spec})
		case *ydbreplication.ObservedTransfer:
			found = append(found, transfer{name: name, spec: value.Spec})
		}
	}
	return found
}

// ownedObjects is every object of kind in objects, in reference order. A
// collection that cannot list its objects lists none: the owner refused it
// before planning.
func ownedObjects(objects schemaext.Objects, kind schemaext.Kind) []schemaext.Object {
	selected, err := objects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(kind) }).All()
	if err != nil {
		return nil
	}
	return selected
}

// replicationChanges are the replication changes of the diff, by kind of
// change.
type replicationChanges struct {
	created, removed []*ydbdiff.AsyncReplication
	names            map[*ydbdiff.AsyncReplication]string
}

func changedReplications(diff *difftypes.SchemaDiff) replicationChanges {
	changes := replicationChanges{names: make(map[*ydbdiff.AsyncReplication]string)}
	for _, record := range diff.FeatureChanges {
		change, ok := record.Value.(*ydbdiff.AsyncReplication)
		if !ok {
			continue
		}
		changes.names[change] = ydbreplication.Reference(record.Subject.Schema.Source, record.Subject.Name.Source)
		switch {
		case change.Before == nil && change.After != nil:
			changes.created = append(changes.created, change)
		case change.After == nil && change.Before != nil:
			changes.removed = append(changes.removed, change)
		}
	}
	return changes
}

// refuseReplications refuses a plan whose table changes would break what an
// async replication owns or what a transfer depends on.
//
// A replication owns its replica tables. While it runs they are read-only and
// the read records them rather than describing them, so no table change
// reaches them; what is left to refuse is a table declared at a replica's
// path, and a replica the plan would leave read-only for good. A transfer
// depends on its topic and its table, neither of which YDB protects: measured
// on 26.2.1.14, DROP TABLE and DROP TOPIC under a running transfer are both
// accepted, and the transfer stops with `Discovery for all topics failed`.
func (p *Planner) refuseReplications(diff *difftypes.SchemaDiff) error {
	changes := changedReplications(diff)
	for _, change := range changes.removed {
		if err := dropReplicationRefusal(diff, changes.names[change], change); err != nil {
			return err
		}
	}
	if err := refuseReplicaTables(diff, changes); err != nil {
		return err
	}
	return refuseTransferDependencies(diff)
}

// dropReplicationRefusal refuses the drop of replication name when its replica
// tables would outlive it read-only.
//
// Measured on 25.1.4.7 and 26.2.1.14: a replication dropped without CASCADE
// leaves its replica tables, and unless it was failed over first they refuse
// every write and every ALTER for good (`Can't execute write tx at replicated
// table`, `path is an async replica table`). So a running, paused or failed
// replication is dropped with CASCADE, which drops its replica tables with it,
// and a schema that removes the replication but declares one of its tables is
// asking for that table to stay writable, which only a failover gives.
func dropReplicationRefusal(diff *difftypes.SchemaDiff, name string, change *ydbdiff.AsyncReplication) error {
	if !change.Cascade() {
		return nil
	}
	targets := ydbreplication.Targets(change.Before.Spec)
	for _, table := range diff.DeclaredTables {
		if ydbreplication.UnderTarget(ydbreplication.TablePath(table.Schema, table.Name), targets) {
			return refuseFact("async replication "+name, fmt.Sprintf("the schema drops it and declares table %s, "+
				"its replica, and YDB keeps a replica of a replication dropped without failover read-only for "+
				"good (`path is an async replica table`); fail it over first with ALTER ASYNC REPLICATION %s SET "+
				"(STATE = 'DONE', FAILOVER_MODE = 'FORCE'), which makes its tables ordinary, and plan again",
				table.QualifiedName(), ydbreplication.Path(name)))
		}
	}
	return nil
}

// refuseReplicaTables refuses a table change that would reach a table a
// replication owns: a declared table at the path of a live replica, a
// replication the plan creates where a live replica of another stands, and a
// failed-over replica the plan would drop while the schema keeps its
// replication.
func refuseReplicaTables(diff *difftypes.SchemaDiff, changes replicationChanges) error {
	current := replicationsOf(diff.Features.CurrentObjects)
	for _, creation := range diff.TablesAdded {
		tablePath := namePath(creation.Name)
		if record, found := replicaRecord(diff.CurrentNotDescribed, tablePath); found {
			return refuseFact("table "+creation.Name, replicaRefusal(current, record.Name, tablePath))
		}
	}
	for _, change := range changes.created {
		for _, target := range ydbreplication.Targets(change.After.Spec) {
			if record, found := replicaUnder(diff.CurrentNotDescribed, target); found {
				return refuseFact("async replication "+changes.names[change], fmt.Sprintf("its target %s "+
					"holds %s, a replica table of another replication, and YDB creates a replica only at a free "+
					"path (measured: `Create dst error`)", target, record))
			}
		}
	}
	declared := make(map[string]bool)
	for _, kept := range replicationsOf(diff.Features.DesiredObjects) {
		declared[kept.name] = true
	}
	for _, removed := range diff.TablesRemoved.Names() {
		tablePath := namePath(removed)
		for _, held := range current {
			if held.state != ydbreplication.StateDone || !declared[held.name] ||
				!ydbreplication.UnderTarget(tablePath, ydbreplication.Targets(held.spec)) {
				continue
			}
			return refuseFact("table "+removed, fmt.Sprintf("it is a table async replication %s created and "+
				"failed over, which the schema keeps; declare the table, which is an ordinary table now, or "+
				"remove the replication from the schema", held.name))
		}
	}
	return nil
}

// replicaRefusal says why a table cannot be declared at the path of a live
// replica table.
func replicaRefusal(current []replication, recorded, tablePath string) string {
	for _, held := range current {
		if ydbreplication.UnderTarget(tablePath, ydbreplication.Targets(held.spec)) {
			return fmt.Sprintf("it is a replica table async replication %s writes, read-only while the "+
				"replication runs (`path is an async replica table`); remove the table from the schema, or fail the "+
				"replication over first with ALTER ASYNC REPLICATION %s SET (STATE = 'DONE', FAILOVER_MODE = "+
				"'FORCE'), which makes it an ordinary table", held.name, ydbreplication.Path(held.name))
		}
	}
	return fmt.Sprintf("%s is a replica table no replication of this database writes any more, which YDB keeps "+
		"read-only for good (`path is an async replica table`); drop it by hand with DROP TABLE, which YDB "+
		"accepts, or remove it from the schema", recorded)
}

// refuseTransferDependencies refuses a declared transfer whose table or topic
// would not be there once the plan has run.
//
// YDB checks neither for a transfer it keeps, and only the table for one it
// creates: measured on 26.2.1.14, CREATE TRANSFER into a missing table answers
// `Path does not exist`, while one from a missing topic is accepted and the
// transfer then stops (`Discovery error: no_topic: SCHEME_ERROR`). So a
// transfer's table has to be declared, and a topic of its own database has to
// be declared, as a topic or as a changefeed, or be one the read recorded
// rather than described. A topic the database holds and the schema does not
// declare is one the plan drops.
func refuseTransferDependencies(diff *difftypes.SchemaDiff) error {
	declared := make(map[string]schemamodel.Table, len(diff.DeclaredTables))
	for _, table := range diff.DeclaredTables {
		declared[ydbreplication.TablePath(table.Schema, table.Name)] = table
	}
	topics := make(map[string]bool)
	for _, object := range ownedObjects(diff.Features.DesiredObjects, ydbtopic.Kind) {
		topics[ydbtopic.Display(object.Ref.Schema.Source, object.Ref.Name.Source)] = true
	}
	for _, declaredTransfer := range transfersOf(diff.Features.DesiredObjects) {
		subject := "transfer " + declaredTransfer.name
		target := strings.Trim(declaredTransfer.spec.Target, "/")
		if _, ok := declared[target]; !ok {
			return refuseFact(subject, fmt.Sprintf("it writes table %s, which the schema does not declare, and YDB "+
				"creates no transfer into a table that is not there (`Path does not exist`); declare the table",
				target))
		}
		if !ydbreplication.LocalSource(declaredTransfer.spec) {
			continue
		}
		source := transferSource(diff.CurrentDatabasePath, declaredTransfer.spec.Source)
		if topics[source] || declaresChangefeed(diff.Features.DesiredObjects, declared, source) ||
			recordsTopic(diff.Features.CurrentCoverage, source) || recordsChangefeed(diff.Features.CurrentCoverage, declared, source) {
			continue
		}
		return refuseFact(subject, fmt.Sprintf("it reads topic %s, which the schema declares neither as a topic "+
			"nor as a changefeed, so the plan leaves no such topic, and YDB accepts such a transfer and then stops "+
			"it (`Discovery error`); declare the topic or the changefeed", source))
	}
	return nil
}

// declaresChangefeed reports whether source, a topic path, is the topic of a
// changefeed a declared table carries.
func declaresChangefeed(objects schemaext.Objects, declared map[string]schemamodel.Table, source string) bool {
	tablePath, changefeed, found := cutLast(source)
	if !found {
		return false
	}
	table, ok := declared[tablePath]
	if !ok {
		return false
	}
	ref := ydbschema.ChangefeedRef(table.Schema, table.Name, changefeed)
	return slices.ContainsFunc(objects.Refs(), func(candidate objectidentity.ID) bool { return candidate.Key() == ref.Key() })
}

// recordsChangefeed recognizes an explicitly observed unreadable stream whose
// parent survives. An unknown namespace alone proves no topic exists.
func recordsChangefeed(knowledgeOf schemaext.Coverage, declared map[string]schemamodel.Table, source string) bool {
	tablePath, changefeed, found := cutLast(source)
	if !found {
		return false
	}
	table, ok := declared[tablePath]
	if !ok {
		return false
	}
	knowledge, found := knowledgeOf.SubjectKnowledge(ydbschema.ChangefeedKind, ydbschema.ChangefeedRef(table.Schema, table.Name, changefeed))
	return found && knowledge.State == schemaext.Unrepresentable
}

// recordsTopic reports whether the read recorded a topic at source, a path,
// rather than described it: a topic it listed and could not read. A topic of
// a changefeed the read recorded is [recordsChangefeed]'s.
func recordsTopic(knowledgeOf schemaext.Coverage, source string) bool {
	ref, err := ydbtopic.ParsePath(source)
	if err != nil {
		return false
	}
	knowledge, recorded := knowledgeOf.SubjectKnowledge(ydbtopic.Kind, ref)
	return recorded && (knowledge.State == schemaext.Unrepresentable || knowledge.State == schemaext.Uninspected)
}

// transferSource is the path, relative to root, of the topic a transfer of
// root's database reads. It reads the source the way the plan's statement
// effects do ([ydbtopic.ResolvePath]), so an absolute source under root names
// the same topic as the relative one. A source that names no topic of the
// database is returned as written.
func transferSource(root, source string) string {
	ref, err := ydbtopic.ResolvePath(root, source)
	if err != nil {
		return source
	}
	return ydbtopic.Display(ref.Schema.Source, ref.Name.Source)
}

// cutLast splits a path at its last slash.
func cutLast(value string) (before, after string, found bool) {
	index := strings.LastIndex(value, "/")
	if index < 0 {
		return "", value, false
	}
	return value[:index], value[index+1:], true
}

// transferOfTable names a transfer of the database that writes table, or
// reads one of its changefeeds, and "" for none. A rebuild swaps the table
// under it, and the transfer would follow the old one, or lose its consumer
// with the old changefeed.
func transferOfTable(diff *difftypes.SchemaDiff, table schemamodel.Table) string {
	tablePath := ydbreplication.TablePath(table.Schema, table.Name)
	for _, held := range transfersOf(diff.Features.CurrentObjects) {
		source := transferSource(diff.CurrentDatabasePath, held.spec.Source)
		if strings.Trim(held.spec.Target, "/") == tablePath ||
			(ydbreplication.LocalSource(held.spec) && strings.HasPrefix(source, tablePath+"/")) {
			return held.name
		}
	}
	return ""
}

// replicaReads is a read of every replica path a replication the plan creates
// writes. YDB checks a view's query when it creates the view (`Cannot find
// table`), and a replication creates its replica tables itself, so a view the
// plan creates runs after the replications that create the tables it may
// read. The view's query is not parsed for the tables it names, so each view
// waits for every such replication.
func replicaReads(diff *difftypes.SchemaDiff) []plangraph.Effect {
	var effects []plangraph.Effect
	seen := make(map[objectidentity.Key]bool)
	for _, change := range changedReplications(diff).created {
		for _, item := range change.After.Spec.Items {
			schema, name, err := ydbpath.Split(item.Target)
			if err != nil {
				continue
			}
			ref := ydbscheme.Path(schema, name)
			if seen[ref.Key()] {
				continue
			}
			seen[ref.Key()] = true
			effects = append(effects, plangraph.Effect{Subject: ref, Action: plangraph.Read})
		}
	}
	return effects
}

// namePath is the path of a table the diff names by its canonical reference.
func namePath(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return name
	}
	return ydbreplication.TablePath(ref.Schema, ref.Name)
}

// replicaRecord finds the read's record of a live replica table at tablePath.
func replicaRecord(notDescribed coverage.Set, tablePath string) (coverage.Object, bool) {
	for _, object := range notDescribed.Objects {
		if object.Kind == coverage.ReplicaTable && namePath(object.Name) == tablePath {
			return object, true
		}
	}
	return coverage.Object{}, false
}

// replicaUnder finds a live replica table at target or under it, and names
// it.
func replicaUnder(notDescribed coverage.Set, target string) (string, bool) {
	for _, object := range notDescribed.Objects {
		if object.Kind == coverage.ReplicaTable && ydbreplication.UnderTarget(namePath(object.Name), []string{target}) {
			return namePath(object.Name), true
		}
	}
	return "", false
}
