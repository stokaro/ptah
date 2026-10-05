package ydb

import (
	"ptah.run/core/ast"
	"ptah.run/internal/ydbtopic"
	"ptah.run/migration/schemadiff/difftypes"
)

// refuseTopics refuses a topic change YDB cannot make, before any node is
// returned: a declaration the line cannot hold, through its capability key,
// and an in-place change YDB refuses -- fewer partitions, or
// auto-partitioning disabled again -- which only dropping the topic and every
// message in it could make.
func (p *Planner) refuseTopics(diff *difftypes.SchemaDiff) error {
	for _, topic := range diff.TopicsAdded {
		if err := planTopicRefusal(ydbtopic.Check(topic.QualifiedName(), topic.Spec, p.caps)); err != nil {
			return err
		}
	}
	for _, change := range diff.TopicsModified {
		if err := planTopicRefusal(ydbtopic.Check(change.Name, change.Desired, p.caps)); err != nil {
			return err
		}
		if err := planTopicRefusal(ydbtopic.ChangeRefusal(change.Name, change.Desired, change.Current)); err != nil {
			return err
		}
	}
	return nil
}

// dropTopics drops every topic the plan removes. They come first, so a table
// the plan creates under a dropped topic's path finds the path free.
func dropTopics(diff *difftypes.SchemaDiff) []ast.Node {
	nodes := make([]ast.Node, 0, len(diff.TopicsRemoved))
	for _, topic := range diff.TopicsRemoved {
		nodes = append(nodes, ast.NewDropTopic(topic.QualifiedName()))
	}
	return nodes
}

// changeTopics creates every topic the plan adds and changes every topic it
// modifies. They come after the tables are dropped, so a topic the plan
// creates under a dropped table's path finds the path free. A topic depends
// on no other object, so nothing has to wait for it.
func changeTopics(diff *difftypes.SchemaDiff) []ast.Node {
	nodes := make([]ast.Node, 0, len(diff.TopicsAdded)+len(diff.TopicsModified))
	for _, topic := range diff.TopicsAdded {
		nodes = append(nodes, ast.NewCreateTopic(topic.QualifiedName(), topic.Spec))
	}
	for _, change := range diff.TopicsModified {
		nodes = append(nodes, ast.NewAlterTopic(change.Name, change.Desired, change.Current))
	}
	return nodes
}

// planTopicRefusal turns a topic refusal into the planner's error.
func planTopicRefusal(refusal *ydbtopic.Refusal) error {
	switch {
	case refusal == nil:
		return nil
	case refusal.Key != "":
		return refuseKey(refusal.Key, refusal.Subject)
	default:
		return refuseFact(refusal.Subject, refusal.Reason)
	}
}
