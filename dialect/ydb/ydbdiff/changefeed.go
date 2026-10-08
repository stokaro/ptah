// Package ydbdiff owns self-contained changes for YDB feature models.
package ydbdiff

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// ChangefeedKind identifies a directional change to one named YDB stream.
const ChangefeedKind schemaext.Kind = "ptah.run/ydb/changefeed-change"

// Changefeed captures both sides after coverage has resolved absence. A nil
// operand means known absence, never uninspected or unrepresentable state.
// Sibling state needed for a parent rebuild travels in the parent's capture.
type Changefeed struct {
	Before *ydbschema.ObservedChangefeed `json:"before"`
	After  *ydbschema.DesiredChangefeed  `json:"after"`
}

// Kind returns the stable change payload identity.
func (*Changefeed) Kind() schemaext.Kind { return ChangefeedKind }

// ReplicationManaged reports whether either operand belongs to replication.
// Planning and reversal share this predicate because neither may reinterpret
// the same change as an independent stream operation. Nil has no binding.
func (v *Changefeed) ReplicationManaged() bool {
	return v != nil && ((v.Before != nil && v.Before.Replication != nil) || (v.After != nil && v.After.RetainedReplication != nil))
}

// CloneChange copies every definition and nested consumer list.
func (v *Changefeed) CloneChange() schemaext.ChangeValue {
	return v.clone()
}

func (v *Changefeed) clone() *Changefeed {
	cloned := &Changefeed{}
	if v.Before != nil {
		cloned.Before = &ydbschema.ObservedChangefeed{Spec: v.Before.Spec.Clone(), Replication: v.Before.Replication.Clone()}
	}
	if v.After != nil {
		cloned.After = &ydbschema.DesiredChangefeed{Spec: v.After.Spec.Clone(), RetainedReplication: v.After.RetainedReplication.Clone()}
	}
	return cloned
}
