package ydb

import (
	"ptah.run/core/ast"
	"ptah.run/internal/ydbtopic"
)

// renderCreateTopic writes one CREATE TOPIC with the topic's consumers and the
// settings its declaration names. A declaration YDB would keep differently
// from how it was written, or one the line cannot hold, is refused through
// [ydbtopic.Check].
func (r *Renderer) renderCreateTopic(node *ast.CreateTopicNode) error {
	if err := topicRefusal(ydbtopic.Check(node.Name, node.Spec, r.caps)); err != nil {
		return err
	}
	r.w.WriteLine(ydbtopic.CreateStatement(node.Name, node.Spec))
	return nil
}

// renderAlterTopic writes the ALTER TOPIC statements that move a topic from
// what the database holds to its declaration. A change YDB cannot make in
// place -- fewer partitions, auto-partitioning disabled again -- is refused
// through [ydbtopic.ChangeRefusal], because the only way to make it drops
// every message the topic holds.
func (r *Renderer) renderAlterTopic(node *ast.AlterTopicNode) error {
	if err := topicRefusal(ydbtopic.Check(node.Name, node.Spec, r.caps)); err != nil {
		return err
	}
	if err := topicRefusal(ydbtopic.ChangeRefusal(node.Name, node.Spec, node.Previous)); err != nil {
		return err
	}
	for _, statement := range ydbtopic.AlterStatements(node.Name, node.Spec, node.Previous) {
		r.w.WriteLine(statement)
	}
	return nil
}

// renderDropTopic writes one DROP TOPIC, which drops every message the topic
// holds and every consumer's position in it, on a target with the topics key.
func (r *Renderer) renderDropTopic(node *ast.DropTopicNode) error {
	if err := topicRefusal(ydbtopic.Check(node.Name, ast.TopicSpec{}, r.caps)); err != nil {
		return err
	}
	r.w.WriteLine(ydbtopic.DropStatement(node.Name))
	return nil
}

// topicRefusal turns a topic refusal into the renderer's error: by the
// capability key it names, or by the reason YDB refuses it on every line.
func topicRefusal(refusal *ydbtopic.Refusal) error {
	switch {
	case refusal == nil:
		return nil
	case refusal.Key != "":
		return refuseKey(refusal.Key, refusal.Subject)
	default:
		return refuseFact(refusal.Subject, refusal.Reason)
	}
}
