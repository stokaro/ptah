package provider

import (
	"fmt"
	"slices"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func validateValueContext(description *protocol.Description, engine string, features map[string]bool, value *protocol.Envelope) error {
	if value.GetContext() == nil {
		return nil
	}
	if !EngineOffers(description, engine, ValueContextFeature) || !features[ValueContextFeature] {
		return fmt.Errorf("%w: value context was not negotiated", ErrProtocol)
	}
	return ValidateDatabaseContext(value.GetContext())
}

func validateContextReadiness(description *protocol.Description, request *protocol.NormalizeValuesRequest, value *protocol.NormalizedValue) error {
	if !value.GetRequiresDatabaseContext() {
		return nil
	}
	if !EngineOffers(description, request.GetEngine(), ValueContextFeature) ||
		!slices.Contains(request.GetTargetFeatures(), ValueContextFeature) {
		return fmt.Errorf("%w: unresolved value context was not negotiated", ErrProtocol)
	}
	if value.GetValue().GetContext() != nil || len(value.GetRequiredObjects()) != 0 || value.GetChangeSemantics() != nil || value.GetOperationSemantics() != nil {
		return fmt.Errorf("%w: unresolved context cannot carry bound or partial dependency facts", ErrProtocol)
	}
	return nil
}

// requireResolvedContext checks every nested normalized operand at a semantic
// consumer boundary. The transport carries captured values in several parent
// messages; a new carrier must not bypass the same dependency-readiness rule.
func requireResolvedContext(message proto.Message) error {
	if message == nil {
		return nil
	}
	if value, ok := message.(*protocol.NormalizedValue); ok && value.GetRequiresDatabaseContext() {
		return fmt.Errorf("%w: dependency consumer requires resolved database context", ErrProtocol)
	}
	var failure error
	message.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if held := fieldMessage(field); held == nil || !holdsNormalizedValues(held) {
			return true
		}
		switch {
		case field.IsMap():
			if field.MapValue().Message() != nil {
				value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
					failure = requireResolvedContext(item.Message().Interface())
					return failure == nil
				})
			}
		case field.IsList():
			for i := 0; i < value.List().Len() && failure == nil; i++ {
				failure = requireResolvedContext(value.List().Get(i).Message().Interface())
			}
		default:
			failure = requireResolvedContext(value.Message().Interface())
		}
		return failure == nil
	})
	return failure
}

var (
	normalizedValueName = (&protocol.NormalizedValue{}).ProtoReflect().Descriptor().FullName()
	valueHolders        sync.Map
)

// holdsNormalizedValues reports whether a message of the type described can
// contain a normalized value, so requireResolvedContext skips every branch
// that cannot. A request's coverage repeats its models for every table, and
// walking each model, none of which holds a value, was most of the walk. The
// answer for every type reachable from the one asked about is computed once,
// as a fixed point: a normalized value is a holder, and so is a type with a
// field of a holder's type.
func holdsNormalizedValues(message protoreflect.MessageDescriptor) bool {
	if known, found := valueHolders.Load(message.FullName()); found {
		held, _ := known.(bool)
		return held
	}
	reachable := make(map[protoreflect.FullName]protoreflect.MessageDescriptor)
	var visit func(protoreflect.MessageDescriptor)
	visit = func(held protoreflect.MessageDescriptor) {
		if _, seen := reachable[held.FullName()]; seen {
			return
		}
		reachable[held.FullName()] = held
		fields := held.Fields()
		for i := range fields.Len() {
			if next := fieldMessage(fields.Get(i)); next != nil {
				visit(next)
			}
		}
	}
	visit(message)
	holders := map[protoreflect.FullName]bool{normalizedValueName: true}
	for changed := true; changed; {
		changed = false
		for name, held := range reachable {
			if holders[name] {
				continue
			}
			fields := held.Fields()
			for i := range fields.Len() {
				if next := fieldMessage(fields.Get(i)); next != nil && holders[next.FullName()] {
					holders[name], changed = true, true
					break
				}
			}
		}
	}
	for name := range reachable {
		valueHolders.Store(name, holders[name])
	}
	return holders[message.FullName()]
}

func validateUnitContexts(description *protocol.Description, engine string, units []*protocol.Unit) error {
	for _, unit := range units {
		value := unit.GetNative()
		if value.GetContext() == nil {
			continue
		}
		target := engine
		if target == "" {
			target = value.GetOwner().GetEngine()
		}
		if value.GetOwner().GetEngine() != target || !EngineOffers(description, target, ValueContextFeature) {
			return fmt.Errorf("%w: native unit context was not negotiated for its engine", ErrProtocol)
		}
		if err := ValidateDatabaseContext(value.GetContext()); err != nil {
			return err
		}
	}
	return nil
}
