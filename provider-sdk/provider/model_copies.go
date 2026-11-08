package provider

import (
	"bytes"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// CloneSharingModels returns a deep copy of message in which every place
// that held one value model message holds one copy of it, and every place
// that held one kind of a coverage, as the coverages of
// SharedCoverageFeature do, holds one copy of that kind. A request names the
// same described model in the coverage of every table it carries, and
// proto.Clone copies the model's definition once for each of those places, so
// a copy of a request of 500 tables held hundreds of megabytes of the same
// few definitions. The copy is equal to proto.Clone's, and shares nothing
// with message; its models and kinds are shared exactly where message's
// were, so a change to one of them is a change to every place that holds it.
func CloneSharingModels[M proto.Message](message M) M {
	copied, isM := cloneSharing(message.ProtoReflect(), &sharedCopies{
		models: make(map[*protocol.ValueModel]*protocol.ValueModel),
		kinds:  make(map[*protocol.ComparisonKindCoverage]*protocol.ComparisonKindCoverage),
	}).Interface().(M)
	if !isM {
		return proto.CloneOf(message)
	}
	return copied
}

// sharedCopies maps each value model message and each coverage kind message
// a copy meets to the one copy made of it.
type sharedCopies struct {
	models map[*protocol.ValueModel]*protocol.ValueModel
	kinds  map[*protocol.ComparisonKindCoverage]*protocol.ComparisonKindCoverage
}

// cloneSharing copies message, mapping each value model message and each
// coverage kind message it meets to one copy of it. A part whose type cannot
// hold a value model is copied by proto.Clone.
func cloneSharing(message protoreflect.Message, models *sharedCopies) protoreflect.Message {
	switch held := message.Interface().(type) {
	case *protocol.ValueModel:
		if held != nil {
			copied, made := models.models[held]
			if !made {
				copied = proto.CloneOf(held)
				models.models[held] = copied
			}
			return copied.ProtoReflect()
		}
	case *protocol.ComparisonKindCoverage:
		if held != nil {
			copied, made := models.kinds[held]
			if !made {
				copied = cloneKind(held, models)
				models.kinds[held] = copied
			}
			return copied.ProtoReflect()
		}
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

// cloneKind copies a coverage kind field by field, so its model is shared
// as every other copy of that model is.
func cloneKind(kind *protocol.ComparisonKindCoverage, models *sharedCopies) *protocol.ComparisonKindCoverage {
	copied := &protocol.ComparisonKindCoverage{}
	if model := kind.GetModel(); model != nil {
		copied.Model, _ = cloneSharing(model.ProtoReflect(), models).Interface().(*protocol.ValueModel)
	}
	for _, source := range kind.GetSources() {
		copied.Sources = append(copied.Sources, proto.CloneOf(source))
	}
	copied.ProtoReflect().SetUnknown(bytes.Clone(kind.ProtoReflect().GetUnknown()))
	return copied
}

// cloneValue copies one value of field: a message by cloneSharing, bytes by
// value, and any other scalar as it is.
func cloneValue(field protoreflect.FieldDescriptor, value protoreflect.Value, models *sharedCopies) protoreflect.Value {
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
func cloneMapValue(field protoreflect.FieldDescriptor, value protoreflect.Value, models *sharedCopies) protoreflect.Value {
	switch held := field.MapValue(); {
	case held.Message() != nil:
		return protoreflect.ValueOfMessage(cloneSharing(value.Message(), models))
	case held.Kind() == protoreflect.BytesKind:
		return protoreflect.ValueOfBytes(bytes.Clone(value.Bytes()))
	default:
		return value
	}
}
