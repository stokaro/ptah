package provider

import (
	"fmt"
	"slices"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ModelReferencesFeature lets the host and the provider send a value model of
// the negotiated description by its reference: a ValueModel that carries only
// its kind, representation, schema version and definition digest. A coverage
// record repeats every model it covers, and a request carries a coverage per
// table and per state, so a message that repeated each definition grew by
// every model's definition for every table. Contract 25.26. It is a two-way
// feature of the provider alone: a provider lists it, the host lists it in
// its host features, and the SDK's client and server send and resolve
// references only when the negotiated description lists it. Services never
// see a reference: the receiving side replaces each one with the described
// model before a service or a validator reads the message.
//
// A model the description does not declare, such as one a captured document
// names under another definition, is always sent whole, so a refusal of an
// unknown or changed model reads it as it was written.
const ModelReferencesFeature = "models.references"

// ModelReference returns the reference that stands for model: a new
// ValueModel with only its kind, representation, schema version and
// definition digest.
func ModelReference(model *protocol.ValueModel) *protocol.ValueModel {
	return &protocol.ValueModel{Kind: model.GetKind(), Representation: model.GetRepresentation(), SchemaVersion: model.GetSchemaVersion(),
		DefinitionSha256: model.GetDefinitionSha256()}
}

// ReferenceModels returns message with each value model that equals a model
// of description replaced by its reference. It returns message itself when
// message holds no such model. Otherwise it returns a copy that shares every
// part of message the replacement does not reach; message is never changed,
// and the copy is for sending, not for changing. A model description does not
// declare, or declares differently in two engines, is kept whole. The models
// of a Description are its own declarations and are never replaced.
func ReferenceModels(description *protocol.Description, message proto.Message) proto.Message {
	declared := describedModels(description)
	// A request repeats one model in every table's coverage, mostly as the
	// same message, so each message is compared once.
	decided := make(map[*protocol.ValueModel]bool)
	replaced, changed := referenceModels(message.ProtoReflect(), func(model *protocol.ValueModel) bool {
		refers, seen := decided[model]
		if !seen {
			found := declared[modelIdentityOf(model)]
			refers = len(model.GetDefinitionJson()) != 0 && found != nil && (model == found || proto.Equal(model, found))
			decided[model] = refers
		}
		return refers
	})
	if !changed {
		return message
	}
	return replaced.Interface()
}

// referenceModels returns the message that stands for message once each
// model refers accepts is replaced by its reference, and whether that is a
// new message. A new message shares the fields no replacement reaches.
func referenceModels(message protoreflect.Message, refers func(*protocol.ValueModel) bool) (protoreflect.Message, bool) {
	if message.Descriptor().FullName() == valueModelName {
		model, isModel := message.Interface().(*protocol.ValueModel)
		if !isModel || !refers(model) {
			return message, false
		}
		return ModelReference(model).ProtoReflect(), true
	}
	if !holdsModels(message.Descriptor().FullName()) {
		return message, false
	}
	var copied protoreflect.Message
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		held := fieldMessage(field)
		if held == nil || !holdsModels(held.FullName()) {
			return true
		}
		replaced, changed := referenceField(message, field, value, refers)
		if changed {
			if copied == nil {
				copied = shallowCopy(message)
			}
			copied.Set(field, replaced)
		}
		return true
	})
	if copied == nil {
		return message, false
	}
	return copied, true
}

// referenceField is referenceModels over the value of one field of message:
// a new list or map that holds the replaced elements and shares the others,
// or a replaced message.
func referenceField(message protoreflect.Message, field protoreflect.FieldDescriptor, value protoreflect.Value, refers func(*protocol.ValueModel) bool) (protoreflect.Value, bool) {
	switch {
	case field.IsList():
		list := value.List()
		var copied protoreflect.List
		for i := range list.Len() {
			replaced, changed := referenceModels(list.Get(i).Message(), refers)
			if !changed {
				continue
			}
			if copied == nil {
				copied = newListLike(message, field, list)
			}
			copied.Set(i, protoreflect.ValueOfMessage(replaced))
		}
		return protoreflect.ValueOfList(copied), copied != nil
	case field.IsMap():
		entries := value.Map()
		var copied protoreflect.Map
		entries.Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
			replaced, changed := referenceModels(entry.Message(), refers)
			if changed {
				if copied == nil {
					copied = newMapLike(message, field, entries)
				}
				copied.Set(key, protoreflect.ValueOfMessage(replaced))
			}
			return true
		})
		return protoreflect.ValueOfMap(copied), copied != nil
	default:
		replaced, changed := referenceModels(value.Message(), refers)
		return protoreflect.ValueOfMessage(replaced), changed
	}
}

// shallowCopy is a new message of message's type that holds message's
// fields, sharing their values.
func shallowCopy(message protoreflect.Message) protoreflect.Message {
	copied := message.New()
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList():
			copied.Set(field, protoreflect.ValueOfList(newListLike(message, field, value.List())))
		case field.IsMap():
			copied.Set(field, protoreflect.ValueOfMap(newMapLike(message, field, value.Map())))
		default:
			copied.Set(field, value)
		}
		return true
	})
	copied.SetUnknown(message.GetUnknown())
	return copied
}

// newListLike is a new list of the field of message holding list's
// elements.
func newListLike(message protoreflect.Message, field protoreflect.FieldDescriptor, list protoreflect.List) protoreflect.List {
	copied := message.NewField(field).List()
	for i := range list.Len() {
		copied.Append(list.Get(i))
	}
	return copied
}

// newMapLike is a new map of the field of message holding entries' entries.
func newMapLike(message protoreflect.Message, field protoreflect.FieldDescriptor, entries protoreflect.Map) protoreflect.Map {
	copied := message.NewField(field).Map()
	entries.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
		copied.Set(key, value)
		return true
	})
	return copied
}

// ResolveModelReferences replaces, in message, each model reference with the
// model of description it names. A reference is a value model with no
// definition. Every reference to one model is replaced by the same copy of
// it, made for this message, so message holds each definition once however
// often it names the model: a change to one replaced model is a change to
// all of them, and never to description. It refuses a reference that carries
// more than its kind, representation, schema version and digest, one that
// names no model of description, and one that names a model two engines
// declare differently, and then message may hold some references resolved
// and others not.
func ResolveModelReferences(description *protocol.Description, message proto.Message) error {
	declared := describedModels(description)
	copies := make(map[*protocol.ValueModel]*protocol.ValueModel)
	replaced, changed, err := replaceModels(message.ProtoReflect(), func(model *protocol.ValueModel) (*protocol.ValueModel, error) {
		if len(model.GetDefinitionJson()) != 0 {
			return nil, nil
		}
		found := declared[modelIdentityOf(model)]
		if !proto.Equal(model, ModelReference(model)) || found == nil {
			return nil, fmt.Errorf("%w: a model reference names no model of the description: %s of version %d", ErrProtocol, model.GetKind(), model.GetSchemaVersion())
		}
		copied, made := copies[found]
		if !made {
			copied = proto.CloneOf(found)
			copies[found] = copied
		}
		return copied, nil
	})
	if changed {
		proto.Reset(message)
		proto.Merge(message, replaced.Interface())
	}
	return err
}

// replaceModels sets, in message, each value model replace returns a
// replacement for in place of that model, and stops at the first error. It
// returns the message that stands for message when message is itself a
// model, and whether it is a new one.
func replaceModels(message protoreflect.Message, replace func(*protocol.ValueModel) (*protocol.ValueModel, error)) (protoreflect.Message, bool, error) {
	if message.Descriptor().FullName() == valueModelName {
		model, isModel := message.Interface().(*protocol.ValueModel)
		if !isModel {
			return message, false, nil
		}
		replacement, err := replace(model)
		if err != nil || replacement == nil {
			return message, false, err
		}
		return replacement.ProtoReflect(), true, nil
	}
	if !holdsModels(message.Descriptor().FullName()) {
		return message, false, nil
	}
	var err error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		held := fieldMessage(field)
		if held == nil || !holdsModels(held.FullName()) {
			return true
		}
		switch {
		case field.IsList():
			list := value.List()
			for i := 0; i < list.Len() && err == nil; i++ {
				var replaced protoreflect.Message
				var changed bool
				if replaced, changed, err = replaceModels(list.Get(i).Message(), replace); changed {
					list.Set(i, protoreflect.ValueOfMessage(replaced))
				}
			}
		case field.IsMap():
			entries := value.Map()
			entries.Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
				var replaced protoreflect.Message
				var changed bool
				if replaced, changed, err = replaceModels(entry.Message(), replace); changed {
					entries.Set(key, protoreflect.ValueOfMessage(replaced))
				}
				return err == nil
			})
		default:
			var replaced protoreflect.Message
			var changed bool
			if replaced, changed, err = replaceModels(value.Message(), replace); changed {
				message.Set(field, protoreflect.ValueOfMessage(replaced))
			}
		}
		return err == nil
	})
	return message, false, err
}

// modelIdentity is what a reference names a model by.
type modelIdentity struct {
	kind           string
	representation protocol.ValueInput_Representation
	version        uint32
	digest         string
}

func modelIdentityOf(model *protocol.ValueModel) modelIdentity {
	return modelIdentity{kind: model.GetKind(), representation: model.GetRepresentation(), version: model.GetSchemaVersion(), digest: model.GetDefinitionSha256()}
}

// describedModels maps each identity to the model description declares under
// it, and to nil where two engines declare different models under one
// identity, which no reference can name.
func describedModels(description *protocol.Description) map[modelIdentity]*protocol.ValueModel {
	declared := make(map[modelIdentity]*protocol.ValueModel)
	for _, engine := range description.GetEngines() {
		for _, model := range engine.GetValueModels() {
			identity := modelIdentityOf(model)
			if held, seen := declared[identity]; seen && !proto.Equal(held, model) {
				declared[identity] = nil
				continue
			}
			if _, seen := declared[identity]; !seen {
				declared[identity] = model
			}
		}
	}
	return declared
}

var (
	valueModelName  = (&protocol.ValueModel{}).ProtoReflect().Descriptor().FullName()
	modelHolders    map[protoreflect.FullName]bool
	modelHoldersSet sync.Once
)

// holdsModels reports whether a message of the given type can contain a value
// model outside a Description, so a walk skips every other branch. The set is
// computed once, over every message of the protocol file, as a fixed point:
// a type that holds a value model or a type in the set is in the set. A
// Description and its Engine are left out, since their models are the
// declarations.
func holdsModels(name protoreflect.FullName) bool {
	modelHoldersSet.Do(func() {
		var types []protoreflect.MessageDescriptor
		var collect func(protoreflect.MessageDescriptors)
		collect = func(messages protoreflect.MessageDescriptors) {
			for i := range messages.Len() {
				types = append(types, messages.Get(i))
				collect(messages.Get(i).Messages())
			}
		}
		collect(protocol.File_protocol_experimental_provider_proto.Messages())
		declarations := []protoreflect.FullName{(&protocol.Description{}).ProtoReflect().Descriptor().FullName(), (&protocol.Engine{}).ProtoReflect().Descriptor().FullName()}
		modelHolders = map[protoreflect.FullName]bool{valueModelName: true}
		for changed := true; changed; {
			changed = false
			for _, message := range types {
				if modelHolders[message.FullName()] || slices.Contains(declarations, message.FullName()) {
					continue
				}
				fields := message.Fields()
				for i := range fields.Len() {
					if field := fieldMessage(fields.Get(i)); field != nil && modelHolders[field.FullName()] {
						modelHolders[message.FullName()], changed = true, true
						break
					}
				}
			}
		}
	})
	return modelHolders[name]
}

// fieldMessage is the message type a field holds, a map's value type for a
// map, and nil for a scalar.
func fieldMessage(field protoreflect.FieldDescriptor) protoreflect.MessageDescriptor {
	if field.IsMap() {
		return field.MapValue().Message()
	}
	return field.Message()
}
