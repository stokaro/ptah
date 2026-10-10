package ydbreplication

import (
	"encoding/json"
	"reflect"
	"strings"

	"ptah.run/core/schemaext"
)

// text is a string a field holds when it is written at all.
const text = `{"type":"string","minLength":1}`

// The JSON schemas of a connection, an item and the two specs, in the form
// the codecs write them.
const (
	// ConnectionDefinition is the JSON schema of a connection.
	ConnectionDefinition = `{"type":"object","additionalProperties":false,"properties":{` +
		`"connection_string":` + text + `,"token_secret_name":` + text + `,"token_secret_path":` + text + `,` +
		`"user":` + text + `,"password_secret_name":` + text + `,"password_secret_path":` + text + `}}`
	// ItemDefinition is the JSON schema of one replicated table.
	ItemDefinition = `{"type":"object","required":["source","target"],"additionalProperties":false,"properties":{` +
		`"source":` + text + `,"target":` + text + `}}`
	// ReplicationDefinition is the JSON schema of a replication's spec.
	ReplicationDefinition = `{"type":"object","required":["connection"],"additionalProperties":false,"properties":{` +
		`"connection":` + ConnectionDefinition + `,"items":{"type":"array","minItems":1,"items":` + ItemDefinition + `},` +
		`"consistency_level":` + text + `,"commit_interval":` + text + `}}`
	// TransferDefinition is the JSON schema of a transfer's spec.
	TransferDefinition = `{"type":"object","required":["source","target","lambda"],"additionalProperties":false,"properties":{` +
		`"connection":` + ConnectionDefinition + `,"source":` + text + `,"target":` + text + `,"lambda":` + text + `,` +
		`"consumer":` + text + `,"batch_size_bytes":{"type":"integer","minimum":1},"flush_interval":` + text + `}}`
	stateDefinition = `{"enum":["running","paused","done","error"]}`
)

// The wire shapes come from the JSON tags of the model types, which are what
// the encoder writes, so the keys are spelled in one place.
var (
	desiredReplicationShape  = wireShape[DesiredReplication]("async replication")
	observedReplicationShape = wireShape[ObservedReplication]("async replication")
	desiredTransferShape     = wireShape[DesiredTransfer]("transfer")
	observedTransferShape    = wireShape[ObservedTransfer]("transfer")
	replicationSpecShape     = wireShape[ReplicationSpec]("async replication spec")
	transferSpecShape        = wireShape[TransferSpec]("transfer spec")
	connectionShape          = wireShape[Connection]("connection")
	itemShape                = wireShape[Item]("item")
)

// Codecs returns the version-one desired and observed models of both kinds:
// [ReplicationCodecs] followed by [TransferCodecs]. The wire keeps every
// setting as written and resolves no default. A decoder accepts only the
// spelling the encoder writes, so an omitted setting written out, such as an
// empty string or an empty item list, is refused, as are a null and an
// unknown key at any depth. Every refusal is a [schemaext.InvalidModelError]
// wrapping [schemaext.ErrInvalidValue].
func Codecs() []schemaext.Codec {
	return append(ReplicationCodecs(), TransferCodecs()...)
}

// ReplicationCodecs returns the desired and observed async replication
// codecs, in that order.
func ReplicationCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredReplication]{Prototype: &DesiredReplication{}, Representation: schemaext.Desired, Version: 1,
			Definition: definition(ReplicationDefinition, `"struct_name":`+text),
			Shape:      replicationShape(desiredReplicationShape), Validate: (*DesiredReplication).Validate,
		}.Codec(),
		schemaext.ModelCodec[*ObservedReplication]{Prototype: &ObservedReplication{}, Representation: schemaext.Observed, Version: 1,
			Definition: definition(ReplicationDefinition, `"state":`+stateDefinition),
			Shape:      replicationShape(observedReplicationShape), Validate: (*ObservedReplication).Validate,
		}.Codec(),
	}
}

// TransferCodecs returns the desired and observed transfer codecs, in that
// order.
func TransferCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredTransfer]{Prototype: &DesiredTransfer{}, Representation: schemaext.Desired, Version: 1,
			Definition: definition(TransferDefinition, `"struct_name":`+text),
			Shape:      transferShape(desiredTransferShape), Validate: (*DesiredTransfer).Validate,
		}.Codec(),
		schemaext.ModelCodec[*ObservedTransfer]{Prototype: &ObservedTransfer{}, Representation: schemaext.Observed, Version: 1,
			Definition: definition(TransferDefinition, `"state":`+stateDefinition),
			Shape:      transferShape(observedTransferShape), Validate: (*ObservedTransfer).Validate,
		}.Codec(),
	}
}

// definition is the JSON schema of a model holding spec and one more optional
// property.
func definition(spec, property string) json.RawMessage {
	return json.RawMessage(`{"type":"object","required":["spec"],"additionalProperties":false,"properties":{"spec":` +
		spec + `,` + property + `}}`)
}

// replicationShape checks a replication model's keys, its spec's, its
// connection's and each item's.
func replicationShape(model schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		fields, err := schemaext.DecodeObject(data, model)
		if err != nil {
			return err
		}
		spec, err := schemaext.DecodeObject(fields["spec"], replicationSpecShape)
		if err != nil {
			return err
		}
		if _, err := schemaext.DecodeObject(spec["connection"], connectionShape); err != nil {
			return err
		}
		if _, present := spec["items"]; !present {
			return nil
		}
		items, err := schemaext.DecodeJSON[[]json.RawMessage](spec["items"])
		if err != nil {
			return err
		}
		for _, item := range items {
			if _, err := schemaext.DecodeObject(item, itemShape); err != nil {
				return err
			}
		}
		return nil
	}
}

// transferShape checks a transfer model's keys, its spec's and its
// connection's.
func transferShape(model schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		fields, err := schemaext.DecodeObject(data, model)
		if err != nil {
			return err
		}
		spec, err := schemaext.DecodeObject(fields["spec"], transferSpecShape)
		if err != nil {
			return err
		}
		if connection, present := spec["connection"]; present {
			if _, err := schemaext.DecodeObject(connection, connectionShape); err != nil {
				return err
			}
		}
		return nil
	}
}

// wireShape derives the strict shape of T's JSON object from its tags. Every
// tagged field is allowed. One without omitempty or omitzero is required, and
// one with either is NonEmpty: its encoder never writes its empty value.
func wireShape[T any](name string) schemaext.ObjectShape {
	shape := schemaext.ObjectShape{Name: name}
	for field := range reflect.TypeFor[T]().Fields() {
		key, options, _ := strings.Cut(field.Tag.Get("json"), ",")
		shape.Allowed = append(shape.Allowed, key)
		if options == "omitempty" || options == "omitzero" {
			shape.NonEmpty = append(shape.NonEmpty, key)
			continue
		}
		shape.Required = append(shape.Required, key)
	}
	return shape
}

// ReplicationCoverage records a source's claim about the async replication
// namespace. Enrollment is limited to this package's own definitions,
// whatever else a runtime registers.
func ReplicationCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return ownedCoverage(ReplicationKind, representation, knowledge, subjects)
}

// TransferCoverage records a source's claim about the transfer namespace.
func TransferCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return ownedCoverage(TransferKind, representation, knowledge, subjects)
}

// ownedCoverage builds this package's model registry once; see
// [schemaext.OwnedCoverageSource].
var ownedCoverage = schemaext.OwnedCoverageSource("ptah.run/ydb", Codecs)

// The reasons a read records an async replication or a transfer it lists and
// does not describe, which a source written from the read carries as a limit
// rather than a declaration: a target without the kind's key, and a cluster
// that does not serve the replication API, which local-ydb leaves out unless
// YDB_GRPC_SERVICES names it.
const (
	// UnsupportedReplicationReason is why a replication is recorded on a
	// target without the async_replication key.
	UnsupportedReplicationReason = "target capability async_replication is unavailable, so Ptah leaves the replication unmanaged"
	// UnsupportedTransferReason is why a transfer is recorded on a target
	// without the transfers key.
	UnsupportedTransferReason = "target capability transfers is unavailable, so Ptah leaves the transfer unmanaged"
	// ServiceUnavailableReason is why either is recorded on a cluster that
	// does not serve the replication API.
	ServiceUnavailableReason = "the cluster does not serve the replication API, so Ptah leaves the object unmanaged"
)
