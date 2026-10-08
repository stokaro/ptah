// Package ydbast defines typed YDB operations carried by the common AST's
// extension envelopes. Payloads contain no SQL rendering or database access.
package ydbast

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

const (
	// AddChangefeedKind identifies creation of a table-owned change stream.
	AddChangefeedKind schemaext.Kind = "ptah.run/ydb/add-changefeed"
	// DropChangefeedKind identifies removal of a stream and its topic state.
	DropChangefeedKind schemaext.Kind = "ptah.run/ydb/drop-changefeed"
	// AlterChangefeedTopicKind identifies an in-place topic-state change.
	AlterChangefeedTopicKind schemaext.Kind = "ptah.run/ydb/alter-changefeed-topic"
)

// AddChangefeed declares a new table-owned stream and its topic consumers.
type AddChangefeed struct {
	Changefeed ydbschema.ChangefeedSpec `json:"changefeed"`
}

// Kind returns the stable semantic operation identity.
func (*AddChangefeed) Kind() schemaext.Kind { return AddChangefeedKind }

// CloneExtension returns a snapshot including independent consumer slices.
func (p *AddChangefeed) CloneExtension() ast.ExtensionPayload {
	return &AddChangefeed{Changefeed: p.Changefeed.Clone()}
}

// Effect describes the operation's additive intent.
func (*AddChangefeed) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Additive, Reason: "does not remove data or tighten constraints"}
}

// DropChangefeed removes a stream, its retained records, and consumer positions.
type DropChangefeed struct {
	Name string `json:"name"`
}

// Kind returns the stable semantic operation identity.
func (*DropChangefeed) Kind() schemaext.Kind { return DropChangefeedKind }

// CloneExtension returns an independent snapshot.
func (p *DropChangefeed) CloneExtension() ast.ExtensionPayload { cloned := *p; return &cloned }

// Effect records loss that reversing the schema operation cannot undo.
func (*DropChangefeed) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Destructive,
		Reason: "DROP CHANGEFEED removes the change stream with every record nobody read, and its consumers"}
}

// AlterChangefeedTopic changes retention and consumers without recreating the
// stream. Both operands travel so removed consumers and reset defaults survive.
type AlterChangefeedTopic struct {
	Changefeed ydbschema.ChangefeedSpec `json:"changefeed"`
	Previous   ydbschema.ChangefeedSpec `json:"previous"`
}

// Kind returns the stable semantic operation identity.
func (*AlterChangefeedTopic) Kind() schemaext.Kind { return AlterChangefeedTopicKind }

// CloneExtension returns independent snapshots of both operands.
func (p *AlterChangefeedTopic) CloneExtension() ast.ExtensionPayload {
	return &AlterChangefeedTopic{Changefeed: p.Changefeed.Clone(), Previous: p.Previous.Clone()}
}

// Effect conservatively records possible consumer-position and retention loss.
func (*AlterChangefeedTopic) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "ALTER TOPIC can drop a consumer's position or shorten how long the stream keeps records"}
}
