package provider

import (
	"bytes"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CloneSharingModels returns a deep copy of message in which every place
// that held one value model message holds one copy of it. A request names the
// same described model in the coverage of every table it carries, and
// proto.Clone copies the model's definition once for each of those places, so
// a copy of a request of 500 tables held hundreds of megabytes of the same
// few definitions. The copy is equal to proto.Clone's, and shares nothing
// with message; its models are shared exactly where message's were, so a
// change to one of them is a change to every place that holds it.
func CloneSharingModels[M proto.Message](message M) M {
	copied, isM := cloneSharing(message.ProtoReflect(), make(map[*protocol.ValueModel]*protocol.ValueModel)).Interface().(M)
	if !isM {
		return proto.CloneOf(message)
	}
	return copied
}

// cloneSharing copies message, mapping each value model message it meets to
// one copy of it. A part whose type cannot hold a value model is copied by
// proto.Clone.
func cloneSharing(message protoreflect.Message, models map[*protocol.ValueModel]*protocol.ValueModel) protoreflect.Message {
	if model, isModel := message.Interface().(*protocol.ValueModel); isModel && model != nil {
		copied, made := models[model]
		if !made {
			copied = proto.CloneOf(model)
			models[model] = copied
		}
		return copied.ProtoReflect()
	}
	if !message.IsValid() || !holdsModels(message.Descriptor().FullName()) {
		return proto.Clone(message.Interface()).ProtoReflect()
	}
	copied := message.New()
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList():
			source, target := value.List(), copied.Mutable(field).List()
			for i := range source.Len() {
				target.Append(cloneValue(field, source.Get(i), models))
			}
		case field.IsMap():
			target := copied.Mutable(field).Map()
			value.Map().Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
				target.Set(key, cloneMapValue(field, entry, models))
				return true
			})
		default:
			copied.Set(field, cloneValue(field, value, models))
		}
		return true
	})
	if unknown := message.GetUnknown(); len(unknown) != 0 {
		copied.SetUnknown(bytes.Clone(unknown))
	}
	return copied
}

// cloneValue copies one value of field: a message by cloneSharing, bytes by
// value, and any other scalar as it is.
func cloneValue(field protoreflect.FieldDescriptor, value protoreflect.Value, models map[*protocol.ValueModel]*protocol.ValueModel) protoreflect.Value {
	switch {
	case field.Message() != nil:
		return protoreflect.ValueOfMessage(cloneSharing(value.Message(), models))
	case field.Kind() == protoreflect.BytesKind:
		return protoreflect.ValueOfBytes(bytes.Clone(value.Bytes()))
	default:
		return value
	}
}

// cloneMapValue is cloneValue for the value of a map entry.
func cloneMapValue(field protoreflect.FieldDescriptor, value protoreflect.Value, models map[*protocol.ValueModel]*protocol.ValueModel) protoreflect.Value {
	switch held := field.MapValue(); {
	case held.Message() != nil:
		return protoreflect.ValueOfMessage(cloneSharing(value.Message(), models))
	case held.Kind() == protoreflect.BytesKind:
		return protoreflect.ValueOfBytes(bytes.Clone(value.Bytes()))
	default:
		return value
	}
}
