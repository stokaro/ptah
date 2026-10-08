package ydb

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbreplication"
	"ptah.run/migration/schemadiff/difftypes"
)

// refuseReplications refuses an async replication or transfer change YDB
// cannot make, or one that would break what such an object owns or depends
// on, before any node is returned.
//
// A replication owns its replica tables. While it runs they are read-only and
// the read records them rather than describing them, so no table change
// reaches them; what is left to refuse is a table declared at a replica's
// path, and a replica the plan would leave read-only for good. A transfer
// depends on its topic and its table, neither of which YDB protects: measured
// on 26.2.1.14, DROP TABLE and DROP TOPIC under a running transfer are both
// accepted, and the transfer stops with `Discovery for all topics failed`.
func (p *Planner) refuseReplications(diff *difftypes.SchemaDiff) error {
	if err := p.refuseReplicationChanges(diff); err != nil {
		return err
	}
	if err := refuseReplicaTables(diff); err != nil {
		return err
	}
	if err := p.refuseTransferChanges(diff); err != nil {
		return err
	}
	return refuseTransferDependencies(diff)
}

// refuseReplicationChanges refuses a replication the target cannot create and
// a change of one YDB cannot make in place.
func (p *Planner) refuseReplicationChanges(diff *difftypes.SchemaDiff) error {
	for _, replication := range diff.AsyncReplicationsAdded {
		if err := planReplicationRefusal(
			ydbreplication.CheckReplication(replication.QualifiedName(), replication.Spec, p.caps)); err != nil {
			return err
		}
	}
	for _, change := range diff.AsyncReplicationsModified {
		if err := planReplicationRefusal(ydbreplication.CheckReplication(change.Name, change.Desired, p.caps)); err != nil {
			return err
		}
		if err := planReplicationRefusal(
			ydbreplication.ReplicationChangeRefusal(change.Name, change.Desired, change.Current, change.State)); err != nil {
			return err
		}
	}
	for _, replication := range diff.AsyncReplicationsRemoved {
		if err := dropReplicationRefusal(diff, replication); err != nil {
			return err
		}
	}
	return nil
}

// dropReplicationRefusal refuses the drop of replication when its replica
// tables would outlive it read-only.
//
// Measured on 25.1.4.7 and 26.2.1.14: a replication dropped without CASCADE
// leaves its replica tables, and unless it was failed over first they refuse
// every write and every ALTER for good (`Can't execute write tx at replicated
// table`, `path is an async replica table`). So a running, paused or failed
// replication is dropped with CASCADE, which drops its replica tables with it,
// and a schema that removes the replication but declares one of its tables is
// asking for that table to stay writable, which only a failover gives.
func dropReplicationRefusal(diff *difftypes.SchemaDiff, replication schemamodel.AsyncReplication) error {
	name := replication.QualifiedName()
	current, found := diff.Replications.CurrentReplication(name)
	if !found || current.State == catalog.ReplicationDone {
		return nil
	}
	targets := currentTargets(current)
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
// replication owns: a declared table at the path of a live replica, a table
// the plan creates under a replication the plan creates, and a failed-over
// replica the plan would drop while the schema keeps its replication.
func refuseReplicaTables(diff *difftypes.SchemaDiff) error {
	for _, creation := range diff.TablesAdded {
		tablePath := namePath(creation.Name)
		if record, found := replicaRecord(diff.CurrentNotDescribed, tablePath); found {
			return refuseFact("table "+creation.Name, replicaRefusal(diff, record.Name, tablePath))
		}
	}
	for _, replication := range diff.AsyncReplicationsAdded {
		for _, target := range ydbreplication.Targets(replication.Spec) {
			if record, found := replicaUnder(diff.CurrentNotDescribed, target); found {
				return refuseFact("async replication "+replication.QualifiedName(), fmt.Sprintf("its target %s "+
					"holds %s, a replica table of another replication, and YDB creates a replica only at a free "+
					"path (measured: `Create dst error`)", target, record))
			}
		}
	}
	for _, removed := range diff.TablesRemoved {
		tablePath := namePath(removed)
		for _, replication := range diff.Replications.CurrentReplications {
			if replication.State != catalog.ReplicationDone || !diff.Replications.Declares(replication.QualifiedName()) ||
				!ydbreplication.UnderTarget(tablePath, currentTargets(replication)) {
				continue
			}
			return refuseFact("table "+removed, fmt.Sprintf("it is a table async replication %s created and "+
				"failed over, which the schema keeps; declare the table, which is an ordinary table now, or "+
				"remove the replication from the schema", replication.QualifiedName()))
		}
	}
	return nil
}

// replicaRefusal says why a table cannot be declared at the path of a live
// replica table.
func replicaRefusal(diff *difftypes.SchemaDiff, recorded, tablePath string) string {
	for _, replication := range diff.Replications.CurrentReplications {
		if ydbreplication.UnderTarget(tablePath, currentTargets(replication)) {
			return fmt.Sprintf("it is a replica table async replication %s writes, read-only while the "+
				"replication runs (`path is an async replica table`); remove the table from the schema, or fail the "+
				"replication over first with ALTER ASYNC REPLICATION %s SET (STATE = 'DONE', FAILOVER_MODE = "+
				"'FORCE'), which makes it an ordinary table", replication.QualifiedName(),
				ydbreplication.Path(replication.QualifiedName()))
		}
	}
	return fmt.Sprintf("%s is a replica table no replication of this database writes any more, which YDB keeps "+
		"read-only for good (`path is an async replica table`); drop it by hand with DROP TABLE, which YDB "+
		"accepts, or remove it from the schema", recorded)
}

// refuseTransferChanges refuses a transfer the target cannot create and a
// change of one YDB cannot make in place.
func (p *Planner) refuseTransferChanges(diff *difftypes.SchemaDiff) error {
	for _, transfer := range diff.TransfersAdded {
		if err := planReplicationRefusal(
			ydbreplication.CheckTransfer(transfer.QualifiedName(), transfer.Spec, p.caps)); err != nil {
			return err
		}
	}
	for _, change := range diff.TransfersModified {
		if err := planReplicationRefusal(ydbreplication.CheckTransfer(change.Name, change.Desired, p.caps)); err != nil {
			return err
		}
		if err := planReplicationRefusal(
			ydbreplication.TransferChangeRefusal(change.Name, change.Desired, change.Current, change.State)); err != nil {
			return err
		}
	}
	return nil
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
	topics := make(map[string]bool, len(diff.Replications.DeclaredTopics))
	for _, topic := range diff.Replications.DeclaredTopics {
		topics[topic] = true
	}
	for _, transfer := range diff.Replications.DeclaredTransfers {
		subject := "transfer " + transfer.QualifiedName()
		target := strings.Trim(transfer.Spec.Target, "/")
		if _, ok := declared[target]; !ok {
			return refuseFact(subject, fmt.Sprintf("it writes table %s, which the schema does not declare, and YDB "+
				"creates no transfer into a table that is not there (`Path does not exist`); declare the table",
				target))
		}
		if !ydbreplication.LocalSource(transfer.Spec) {
			continue
		}
		source := ydbreplication.SourceKey(transfer.Spec.Source, "")
		if topics[source] || declaresChangefeed(diff.Replications.DesiredObjects, declared, source) || recordsTopic(diff.CurrentNotDescribed, source) || recordsChangefeed(diff.Replications.CurrentCoverage, declared, source) {
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

// recordsTopic reports whether the read recorded a topic at source, a path:
// a topic of its own, or the topic of a changefeed the read recorded rather
// than described, such as one a replication added to its source table.
func recordsTopic(notDescribed coverage.Set, source string) bool {
	schema, name, found := cutLast(source)
	if !found {
		schema, name = "", source
	}
	if object, limited := notDescribed.Limit(coverage.Topic, tableref.Canonical(schema, name)); limited &&
		!object.WholeKind() {
		return true
	}
	return false
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
	for _, transfer := range diff.Replications.CurrentTransfers {
		source := ydbreplication.SourceKey(transfer.Spec.Source, "")
		if strings.Trim(transfer.Spec.Target, "/") == tablePath ||
			(ydbreplication.LocalSource(transfer.Spec) && strings.HasPrefix(source, tablePath+"/")) {
			return transfer.QualifiedName()
		}
	}
	return ""
}

// dropReplications drops every transfer and every replication the plan
// removes, first: a transfer before the table, changefeed or topic it uses
// goes, and a replication before a table is created at one of its paths.
//
// A replication that was failed over is dropped without CASCADE, which keeps
// its tables, ordinary tables by then; any other is dropped with CASCADE,
// which drops the replica tables it created. [dropReplicationRefusal] has
// refused the plan where the schema declares one of them.
func dropReplications(diff *difftypes.SchemaDiff) []ast.Node {
	nodes := make([]ast.Node, 0, len(diff.TransfersRemoved)+len(diff.AsyncReplicationsRemoved))
	for _, transfer := range diff.TransfersRemoved {
		nodes = append(nodes, ast.NewDropTransfer(transfer.QualifiedName()))
	}
	for _, replication := range diff.AsyncReplicationsRemoved {
		name := replication.QualifiedName()
		current, found := diff.Replications.CurrentReplication(name)
		nodes = append(nodes, ast.NewDropAsyncReplication(name, !found || current.State != catalog.ReplicationDone))
	}
	return nodes
}

// changeReplications creates and changes every replication and transfer the
// plan adds or modifies, after the tables and their changefeeds: a replication
// creates its replica tables at paths the dropped tables freed, and a
// transfer needs its table, its changefeed and its consumer to exist.
func changeReplications(diff *difftypes.SchemaDiff) []ast.Node {
	nodes := make([]ast.Node, 0, len(diff.AsyncReplicationsAdded)+len(diff.AsyncReplicationsModified)+
		len(diff.TransfersAdded)+len(diff.TransfersModified))
	for _, replication := range diff.AsyncReplicationsAdded {
		nodes = append(nodes, ast.NewCreateAsyncReplication(replication.QualifiedName(), replication.Spec))
	}
	for _, change := range diff.AsyncReplicationsModified {
		nodes = append(nodes, ast.NewAlterAsyncReplication(change.Name, change.Desired, change.Current))
	}
	for _, transfer := range diff.TransfersAdded {
		nodes = append(nodes, ast.NewCreateTransfer(transfer.QualifiedName(), transfer.Spec))
	}
	for _, change := range diff.TransfersModified {
		nodes = append(nodes, ast.NewAlterTransfer(change.Name, change.Desired, change.Current))
	}
	return nodes
}

// currentTargets are the paths a replication the database holds wrote its
// replica tables at.
func currentTargets(replication catalog.AsyncReplication) []string {
	return ydbreplication.Targets(replication.Spec)
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

// planReplicationRefusal turns a replication or transfer refusal into the
// planner's error.
func planReplicationRefusal(refusal *ydbreplication.Refusal) error {
	switch {
	case refusal == nil:
		return nil
	case refusal.Key != "":
		return refuseKey(refusal.Key, refusal.Subject)
	default:
		return refuseFact(refusal.Subject, refusal.Reason)
	}
}
