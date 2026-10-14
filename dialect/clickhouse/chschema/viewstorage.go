package chschema

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

// ViewStorageKind identifies where a materialized view keeps its rows: the
// engine and clauses of the inner table the view stores them in, or the table
// a `TO` clause sends them to. It is a setting of the common materialized
// view.
//
// Ptah creates every materialized view with `ENGINE = MergeTree ORDER BY
// tuple()` and models no other storage, so the kind carries no value. Its
// coverage says what a read saw: a view stored the way Ptah creates one is
// covered, and a view stored any other way is an unrepresentable subject of
// the kind, whose reason names the clause the server stores. Nothing can then
// read such a view as stored the way Ptah stores it.
const ViewStorageKind schemaext.Kind = "ptah.run/clickhouse/view-storage"

// DesiredViewStorage names [ViewStorageKind] in the desired representation.
// No source declares a value; its codec refuses every one.
type DesiredViewStorage struct{}

// ObservedViewStorage names [ViewStorageKind] in the observed representation.
// No reader reports a value; its codec refuses every one.
type ObservedViewStorage struct{}

// Kind returns the owned view storage model identity.
func (*DesiredViewStorage) Kind() schemaext.Kind { return ViewStorageKind }

// Kind returns the owned view storage model identity.
func (*ObservedViewStorage) Kind() schemaext.Kind { return ViewStorageKind }

// Clone returns an independent value. A nil receiver remains typed nil.
func (v *DesiredViewStorage) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredViewStorage)(nil)
	}
	return &DesiredViewStorage{}
}

// Clone returns an independent value. A nil receiver remains typed nil.
func (v *ObservedViewStorage) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedViewStorage)(nil)
	}
	return &ObservedViewStorage{}
}

// Equal reports whether other is a value of the same model.
func (v *DesiredViewStorage) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredViewStorage)
	return ok && (v == nil) == (w == nil)
}

// Equal reports whether other is a value of the same model.
func (v *ObservedViewStorage) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedViewStorage)
	return ok && (v == nil) == (w == nil)
}

// viewStorageDefinition describes a model whose every value is refused.
const viewStorageDefinition = `{"$id":"ptah.run/clickhouse/view-storage/v1","values":"none: a materialized view's storage is recorded as coverage only"}`

// ViewStorageCodecs returns the versioned codecs that name [ViewStorageKind]
// in both representations. Every encode, decode and clone fails with
// schemaext.ErrInvalidValue, because the kind holds no value; registering the
// codecs lets coverage name the kind.
func ViewStorageCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		viewStorageCodec(&DesiredViewStorage{}, schemaext.Desired),
		viewStorageCodec(&ObservedViewStorage{}, schemaext.Observed),
	}
}

func viewStorageCodec(prototype schemaext.Value, representation schemaext.Representation) schemaext.Codec {
	refuse := func(schemaext.Payload) (json.RawMessage, error) { return nil, errViewStorageValue }
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1, Definition: json.RawMessage(viewStorageDefinition),
		Clone:  func(schemaext.Payload) (schemaext.Payload, error) { return nil, errViewStorageValue },
		Encode: refuse, Canonical: refuse,
		Decode: func(json.RawMessage) (schemaext.Payload, error) { return nil, errViewStorageValue },
	}
}

var errViewStorageValue = fmt.Errorf("%w: a ClickHouse materialized view's storage is not modeled and carries no value", schemaext.ErrInvalidValue)
