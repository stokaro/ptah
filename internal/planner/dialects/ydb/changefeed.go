package ydb

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/internal/ydbchangefeed"
	"ptah.run/internal/ydbtype"
	"ptah.run/migration/schemadiff/difftypes"
)

// How a YDB plan changes a table's changefeeds.
//
// A changefeed is added and dropped through ALTER TABLE, one per statement,
// and its topic's retention and consumers change in place through ALTER
// TOPIC. No option of the changefeed itself changes in place, so a changed
// one is dropped and added again, which restarts its stream: the records
// nobody read are gone with the old topic, and every consumer of the new one
// starts from its beginning. The plan says so above the statements.
//
// The statements come after the table's column and index changes, and drops
// before additions, since YDB holds a table to a limit of changefeeds
// (measured: five on local-ydb 25.1.4.7 and 26.2.1.14, `cdc streams count has
// reached maximum value in the table`) that a swap must not cross. A table the
// plan drops takes its changefeeds with it, measured on both lines, and a
// table the plan rebuilds carries them through the rebuild.

// refuseChangefeedChanges refuses, before anything is emitted, a change of a
// table's changefeeds this target cannot make: one on a target without
// [capability.Changefeeds], and a changefeed to add whose option the target
// lacks, whose name a declared index of the table holds, or whose topic YDB
// cannot split by the table's key.
func (p *Planner) refuseChangefeedChanges(diff *difftypes.SchemaDiff) error {
	for _, tableDiff := range diff.TablesModified {
		change := tableDiff.ChangefeedsChange
		if change == nil {
			continue
		}
		subject := fmt.Sprintf("table %q", tableDiff.TableName)
		if !p.caps.Has(capability.Changefeeds) {
			return refuseKey(capability.Changefeeds, "changing the changefeeds of "+subject)
		}
		indexes := make([]string, 0, len(tableDiff.Desired.Indexes))
		for _, index := range tableDiff.Desired.Indexes {
			indexes = append(indexes, index.Name)
		}
		if reason := ydbchangefeed.NameRefusal(change.Desired, indexes); reason != "" {
			return refuseFact(subject, reason)
		}
		keyType := p.firstKeyType(tableDiff.Desired)
		for _, changefeed := range change.Desired {
			if err := p.checkChangefeed(tableDiff.TableName, changefeed, keyType); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkChangefeed refuses a changefeed the target cannot hold on a table
// whose first key column has the YDB type keyType, empty where unknown.
func (p *Planner) checkChangefeed(table string, changefeed ast.ChangefeedSpec, keyType string) error {
	if refusal := ydbchangefeed.Check(table, changefeed, p.caps); refusal != nil {
		if refusal.Key != "" {
			return refuseKey(refusal.Key, refusal.Subject)
		}
		return refuseFact(refusal.Subject, refusal.Reason)
	}
	if reason := ydbchangefeed.KeyRefusal(changefeed, keyType); reason != "" {
		return refuseFact(fmt.Sprintf("changefeed %q of table %q", changefeed.Name, table), reason)
	}
	return nil
}

// firstKeyType is the YDB type the declaration's first key column maps to, or
// empty where the declaration does not say.
func (p *Planner) firstKeyType(declaration difftypes.TableDeclaration) string {
	key := declaration.Table.PrimaryKey
	for _, field := range declaration.Fields {
		if len(key) == 0 && field.Primary {
			key = []string{field.Name}
		}
	}
	if len(key) == 0 {
		return ""
	}
	for _, field := range declaration.Fields {
		if field.Name != key[0] {
			continue
		}
		mapping, err := ydbtype.Map(field.Type, p.caps)
		if err != nil {
			return ""
		}
		return mapping.Type
	}
	return ""
}

// changeChangefeeds writes each modified table's changefeed changes, except a
// rebuilt table's, which its rebuild carries.
func changeChangefeeds(diff *difftypes.SchemaDiff, rebuilds map[string]*tableRebuild, semantics identifier.Semantics) []ast.Node {
	var nodes []ast.Node
	for _, tableDiff := range diff.TablesModified {
		if _, rebuilt := rebuilds[semantics.TableIdentityKey(tableDiff.TableName)]; rebuilt || tableDiff.ChangefeedsChange == nil {
			continue
		}
		nodes = append(nodes, changefeedNodes(tableDiff.TableName, *tableDiff.ChangefeedsChange)...)
	}
	return nodes
}

// changefeedNodes writes one table's changefeed changes: the drops, then the
// additions, a changed changefeed among both, then the changes of a topic in
// place.
func changefeedNodes(table string, change difftypes.ChangefeedsChange) []ast.Node {
	alter := func(operation ast.AlterOperation) ast.Node {
		return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{operation}}
	}
	var drops, adds, topics []ast.Node
	for _, current := range change.Current {
		desired, kept := changefeedNamed(change.Desired, current.Name)
		switch {
		case !kept:
			drops = append(drops, ast.NewComment(droppedNote(table, current)), alter(&ast.ExtensionAlterOperation{Payload: &ydbast.DropChangefeed{Name: current.Name}}))
		case ydbchangefeed.Recreated(desired, current):
			drops = append(drops, ast.NewComment(recreatedNote(table, current)), alter(&ast.ExtensionAlterOperation{Payload: &ydbast.DropChangefeed{Name: current.Name}}))
			adds = append(adds, alter(&ast.ExtensionAlterOperation{Payload: &ydbast.AddChangefeed{Changefeed: desired.Clone()}}))
		case ydbchangefeed.TopicChanged(desired, current):
			if _, restarted := ydbchangefeed.TopicStatements(table, desired, current); len(restarted) > 0 {
				topics = append(topics, ast.NewComment(restartedConsumersNote(table, current.Name, restarted)))
			}
			topics = append(topics, alter(&ast.ExtensionAlterOperation{Payload: &ydbast.AlterChangefeedTopic{Changefeed: desired.Clone(), Previous: current.Clone()}}))
		}
	}
	for _, desired := range change.Desired {
		if _, held := changefeedNamed(change.Current, desired.Name); !held {
			adds = append(adds, alter(&ast.ExtensionAlterOperation{Payload: &ydbast.AddChangefeed{Changefeed: desired.Clone()}}))
		}
	}
	return slices.Concat(drops, adds, topics)
}

// changefeedNamed finds a changefeed by name.
func changefeedNamed(changefeeds []ast.ChangefeedSpec, name string) (ast.ChangefeedSpec, bool) {
	index := slices.IndexFunc(changefeeds, func(changefeed ast.ChangefeedSpec) bool { return changefeed.Name == name })
	if index < 0 {
		return ast.ChangefeedSpec{}, false
	}
	return changefeeds[index], true
}

// droppedNote says what dropping a changefeed loses.
func droppedNote(table string, changefeed ast.ChangefeedSpec) string {
	note := fmt.Sprintf("Changefeed %s of table %s is dropped with its topic: the records nobody read are lost",
		changefeed.Name, table)
	consumers := consumerNames(changefeed)
	switch len(consumers) {
	case 0:
		return note + "."
	case 1:
		return note + ", and so is consumer " + consumers[0] + "."
	default:
		return note + ", and so are consumers " + strings.Join(consumers, ", ") + "."
	}
}

// recreatedNote says why a changefeed is dropped and added again, and what
// the restart costs its readers.
func recreatedNote(table string, changefeed ast.ChangefeedSpec) string {
	note := fmt.Sprintf("Changefeed %s of table %s is dropped and added again, because YDB changes no option of a "+
		"changefeed in place. Its stream restarts: the records nobody read are lost", changefeed.Name, table)
	consumers := consumerNames(changefeed)
	switch len(consumers) {
	case 0:
		return note + "."
	case 1:
		return note + ", and consumer " + consumers[0] + " loses its position and starts again from the beginning " +
			"of the new stream."
	default:
		return note + ", and consumers " + strings.Join(consumers, ", ") + " lose their position and start again " +
			"from the beginning of the new stream."
	}
}

// restartedConsumersNote says why consumers are dropped and added again.
func restartedConsumersNote(table, changefeed string, consumers []string) string {
	const why = "because YDB keeps a consumer's codecs once it has any."
	if len(consumers) == 1 {
		return fmt.Sprintf("Consumer %s of changefeed %s of table %s is dropped and added again, %s It loses its "+
			"position and starts again from the beginning of the stream.", consumers[0], changefeed, table, why)
	}
	return fmt.Sprintf("Consumers %s of changefeed %s of table %s are dropped and added again, %s They lose their "+
		"position and start again from the beginning of the stream.", strings.Join(consumers, ", "), changefeed, table, why)
}

// consumerNames lists a changefeed's consumers by name.
func consumerNames(changefeed ast.ChangefeedSpec) []string {
	names := make([]string, len(changefeed.Consumers))
	for i, consumer := range changefeed.Consumers {
		names[i] = consumer.Name
	}
	return names
}

// rebuildChangefeeds are the changefeeds a rebuild drops from the old table
// before it moves it, and adds to the new one once it holds the name: the
// database's and the declaration's, which are the same where the comparison
// recorded no change.
func rebuildChangefeeds(rebuild *tableRebuild) (current, desired []ast.ChangefeedSpec) {
	if rebuild.tableDiff != nil && rebuild.tableDiff.ChangefeedsChange != nil {
		return rebuild.tableDiff.ChangefeedsChange.Current, rebuild.tableDiff.ChangefeedsChange.Desired
	}
	return rebuild.declaration.Table.Changefeeds, rebuild.declaration.Table.Changefeeds
}

// rebuildChangefeedNote says why a rebuild drops and adds changefeeds.
func rebuildChangefeedNote(table string, changefeeds []ast.ChangefeedSpec) string {
	const why = "because YDB moves no table that carries a changefeed."
	if len(changefeeds) == 1 {
		return fmt.Sprintf("Changefeed %s of table %s is dropped before the swap and added again after it, %s Its "+
			"stream restarts: the records nobody read are lost, and its consumers start again from the beginning of "+
			"the new stream.", changefeeds[0].Name, table, why)
	}
	names := make([]string, len(changefeeds))
	for i, changefeed := range changefeeds {
		names[i] = changefeed.Name
	}
	return fmt.Sprintf("Changefeeds %s of table %s are dropped before the swap and added again after it, %s Each "+
		"stream restarts: the records nobody read are lost, and the consumers start again from the beginning of "+
		"the new stream.", strings.Join(names, ", "), table, why)
}
